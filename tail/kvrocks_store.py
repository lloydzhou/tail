"""Kvrocks 缓存存储 —— Python 参考版的 L2/唯一后端。

用 Kvrocks(硬盘持久化,Redis 协议)作为前缀缓存后端。
对应设计文档第 5.1 节,用 Kvrocks 替代 Redis,数据落硬盘。

与 ``PrefixCacheStore``(纯内存)实现相同的接口(get/set/delete/__len__/clear),
可互换注入 gateway。生产推荐用 KvrocksStore;CI/无 Kvrocks 环境用 PrefixCacheStore。
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import redis

logger = logging.getLogger(__name__)

from .store import CacheEntry


class KvrocksStore:
    """直连 Kvrocks 的前缀缓存存储。

    与 PrefixCacheStore 接口兼容:get / set / delete / clear / __len__。
    数据以 JSON 序列化存入 Kvrocks,key = ``{ns}:{hash}``,带 TTL。
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6666,
        *,
        ns: str = "prefix_cache",
        max_prefix_bytes: int = 8 * 1024 * 1024,
        socket_timeout: float = 0.5,
    ):
        self._ns = ns
        self._max = max_prefix_bytes
        self._client = redis.Redis(
            host=host, port=port, decode_responses=False,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_timeout,
        )

    def _key(self, h: str) -> str:
        return f"{self._ns}:{h}"

    def get(self, hash_value: str, now: float | None = None) -> Optional[CacheEntry]:
        """读取;过期或不存在返回 None。"""
        if now is None:
            now = time.time()
        try:
            raw = self._client.get(self._key(hash_value))
        except redis.RedisError as e:
            logger.warning("kvrocks get failed: %s", e)
            return None
        if raw is None:
            return None
        try:
            d = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if d.get("expire_at", 0) <= now:
            return None
        return CacheEntry(
            hash=hash_value,
            token_ids=d.get("token_ids", []),
            messages=d.get("messages", []),
            model=d.get("model", ""),
            prefix_length=d.get("prefix_length", 0),
            created_at=d.get("created_at", now),
            expire_at=d.get("expire_at", now),
        )

    def set(self, entry: CacheEntry, ttl: Optional[int] = None) -> None:
        """写入;ttl 为 None 时按 entry.expire_at 计算。"""
        d = {
            "messages": entry.messages,
            "model": entry.model,
            "prefix_length": entry.prefix_length,
            "expire_at": entry.expire_at,
            "created_at": entry.created_at,
            "token_ids": entry.token_ids,
        }
        blob = json.dumps(d, ensure_ascii=False).encode("utf-8")
        if len(blob) > self._max:
            return
        if ttl is None:
            ttl = max(1, int(entry.expire_at - time.time()))
        try:
            self._client.set(self._key(entry.hash), blob, ex=ttl)
        except redis.RedisError as e:
            logger.warning("kvrocks set failed: %s", e)

    def delete(self, hash_value: str) -> bool:
        try:
            return bool(self._client.delete(self._key(hash_value)))
        except redis.RedisError:
            return False

    def clear(self) -> None:
        """清空本命名空间下的所有 key。"""
        try:
            for k in self._client.scan_iter(match=f"{self._ns}:*"):
                self._client.delete(k)
        except redis.RedisError as e:
            logger.warning("kvrocks clear failed: %s", e)

    def __len__(self) -> int:
        try:
            return sum(1 for _ in self._client.scan_iter(match=f"{self._ns}:*"))
        except redis.RedisError:
            return 0

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.RedisError:
            return False
