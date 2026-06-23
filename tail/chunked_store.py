"""ChunkedStore —— v2.1 分层 Segment-Merkle 缓存存储。

对应设计文档 §2~§6。

存储布局(都在 Kvrocks):
    sys:    {sys_hash}       → system 全文           (sys_hash != "0")
    tools:  {tools_hash}     → tools 全文            (tools_hash != "0")
    seg:    {seg_hash}       → segment JSON          (内容寻址去重)
    pfx:    {pfx_hash}       → PrefixNode JSON       (Merkle 链节点)
    meta:   {cache_key}      → {sys_hash, tools_hash, pfx_hash, len, expire_at}

对外入口:cache_key = "sys_hash::tools_hash::pfx_hash"。

核心 API:
    put(request)        → cache_key   (写入完整请求的三段 + 链)
    get(cache_key)      → request     (还原完整请求;失败返回 None)
    put_increment(...)  → cache_key   (增量写入:只加新 segment,见 §3.5)

不变量:还原后的 messages 与原数组字节级一致(由 segment 展平保证)。
"""

from __future__ import annotations

import json
import logging
import time
from typing import List, Optional

import redis

from .merkle import (
    EMPTY_HASH, PrefixNode, build_chain_nodes, chain_hash,
    messages_prefix_hash, next_node, segment_hash,
)
from .segment import split_segments

logger = logging.getLogger(__name__)

NULL_HASH = "0"  # 缺失段(system/tools)的固定占位


class ChunkedStore:
    """v2.1 分层 Segment-Merkle 缓存(基于 Kvrocks / Redis 协议)。"""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6666,
        *,
        ns: str = "tail",                # key 前缀命名空间
        ttl_meta: int = 2 * 3600,         # meta 入口 TTL
        ttl_stable: int = 24 * 3600,      # sys/tools/seg(稳定内容)TTL
        ttl_pfx: int = 6 * 3600,          # pfx 节点 TTL
        socket_timeout: float = 1.0,
    ):
        self._ns = ns
        self._ttl_meta = ttl_meta
        self._ttl_stable = ttl_stable
        self._ttl_pfx = ttl_pfx
        self._client = redis.Redis(
            host=host, port=port, decode_responses=False,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_timeout,
        )

    # ------------------------------------------------------------------
    # key 构造
    # ------------------------------------------------------------------
    def _k(self, kind: str, h: str) -> str:
        return f"{self._ns}:{kind}:{h}"

    # ------------------------------------------------------------------
    # 对外:put(完整请求)
    # ------------------------------------------------------------------
    def put(self, request: dict, *, expire_at: Optional[float] = None) -> str:
        """写入一个完整请求,返回 cache_key。

        request = { "system": str|None, "tools": list|None, "messages": list }
        """
        if expire_at is None:
            expire_at = time.time() + self._ttl_meta

        system = request.get("system")
        tools = request.get("tools")
        messages = request.get("messages", [])

        sys_hash = self._put_optional("sys", system)
        tools_hash = self._put_optional("tools", tools)
        pfx_hash = self._put_messages(messages)

        cache_key = f"{sys_hash}::{tools_hash}::{pfx_hash}"
        meta = {
            "sys_hash": sys_hash,
            "tools_hash": tools_hash,
            "pfx_hash": pfx_hash,
            "len": len(messages),
            "expire_at": expire_at,
        }
        self._client.set(self._k("meta", cache_key),
                         json.dumps(meta).encode("utf-8"), ex=self._ttl_meta)
        return cache_key

    def _put_optional(self, kind: str, content) -> str:
        """对 system/tools 段:有内容则算 hash 并存(JSON);None → 返回 '0'。"""
        if content is None:
            return NULL_HASH
        value = json.dumps(content, ensure_ascii=False).encode("utf-8")
        import hashlib
        h = hashlib.sha256(value).hexdigest()[:16]
        key = self._k(kind, h)
        # 内容寻址去重:setnx 只在不存在时写
        self._client.setnx(key, value)
        self._client.expire(key, self._ttl_stable)
        return h

    def _put_messages(self, messages: List[dict]) -> str:
        """写入 messages 的 segment + Merkle 链,返回前缀 pfx_hash。"""
        if not messages:
            return EMPTY_HASH
        segments = split_segments(messages)
        seg_hashes = []
        for seg in segments:
            sh = segment_hash(seg)
            seg_hashes.append(sh)
            # 内容寻址去重:setnx
            seg_key = self._k("seg", sh)
            self._client.setnx(seg_key, json.dumps(seg, ensure_ascii=False).encode("utf-8"))
            self._client.expire(seg_key, self._ttl_stable)
        # 写 Merkle 链节点
        nodes = build_chain_nodes(seg_hashes)
        for pfx_hash, node in nodes:
            pfx_key = self._k("pfx", pfx_hash)
            self._client.setnx(pfx_key, json.dumps(node.to_dict()).encode("utf-8"))
            self._client.expire(pfx_key, self._ttl_pfx)
        return nodes[-1][0] if nodes else EMPTY_HASH

    # ------------------------------------------------------------------
    # 对外:get(还原完整请求)
    # ------------------------------------------------------------------
    def get(self, cache_key: str) -> Optional[dict]:
        """根据 cache_key 还原完整请求。失败返回 None(任一段缺失/损坏)。"""
        try:
            raw = self._client.get(self._k("meta", cache_key))
        except redis.RedisError as e:
            logger.warning("chunked get meta failed: %s", e)
            return None
        if raw is None:
            return None
        try:
            meta = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if meta.get("expire_at", 0) <= time.time():
            return None

        # 还原三段
        system = self._get_str("sys", meta["sys_hash"])
        tools = self._get_str("tools", meta["tools_hash"])
        messages = self._reconstruct_prefix(meta["pfx_hash"])
        if messages is None:
            return None  # 链断裂

        request = {"messages": messages}
        if system is not None:
            request["system"] = system
        if tools is not None:
            request["tools"] = tools
        return request

    def _get_str(self, kind: str, h: str):
        """还原 system/tools 段。NULL_HASH → None(缺失);否则返回解析后的值。"""
        if h == NULL_HASH:
            return None
        try:
            raw = self._client.get(self._k(kind, h))
        except redis.RedisError:
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return None

    def _reconstruct_prefix(self, pfx_hash: str) -> Optional[List[dict]]:
        """沿 Merkle 链回溯还原 messages。链断裂返回 None。"""
        if pfx_hash == EMPTY_HASH:
            return []
        seg_hashes: List[str] = []
        cur = pfx_hash
        seen = set()  # 防环
        while cur != EMPTY_HASH:
            if cur in seen:
                return None  # 环,异常
            seen.add(cur)
            try:
                raw = self._client.get(self._k("pfx", cur))
            except redis.RedisError:
                return None
            if raw is None:
                return None  # 链中间节点缺失(过期被驱逐)
            try:
                node = PrefixNode.from_dict(json.loads(raw))
            except (ValueError, TypeError, KeyError):
                return None
            seg_hashes.append(node.seg_ref)
            cur = node.prev
        seg_hashes.reverse()
        # 批量 mget 还原所有 segment
        if not seg_hashes:
            return []
        keys = [self._k("seg", sh) for sh in seg_hashes]
        try:
            blobs = self._client.mget(keys)
        except redis.RedisError:
            return None
        messages: List[dict] = []
        for blob in blobs:
            if blob is None:
                return None  # 某 segment 缺失
            try:
                seg = json.loads(blob)
            except (ValueError, TypeError):
                return None
            messages.extend(seg)
        return messages

    # ------------------------------------------------------------------
    # 增量写入(对话增长一个回合,只加新 segment + 一个节点)
    # ------------------------------------------------------------------
    def extend(self, prev_cache_key: str, new_messages: List[dict],
               *, expire_at: Optional[float] = None) -> Optional[str]:
        """对话从 prev_cache_key 增长,加入 new_messages(若干新 segment)。

        返回新的 cache_key;失败返回 None(prev 不存在等)。
        这是 §3.5 的 O(1) 增量写入:只加新 segment + 新节点,不重写已有链。
        """
        if expire_at is None:
            expire_at = time.time() + self._ttl_meta
        # 取 prev 的 meta(拿 sys/tools/pfx_hash)
        try:
            raw = self._client.get(self._k("meta", prev_cache_key))
        except redis.RedisError:
            return None
        if raw is None:
            return None
        try:
            prev_meta = json.loads(raw)
        except (ValueError, TypeError):
            return None
        sys_hash = prev_meta["sys_hash"]
        tools_hash = prev_meta["tools_hash"]
        prev_pfx = prev_meta["pfx_hash"]
        prev_seg_count = self._get_seg_count(prev_pfx)

        # 处理 new_messages:切 segment,只写新的
        if not new_messages:
            return prev_cache_key
        new_segments = split_segments(new_messages)
        cur_pfx = prev_pfx
        cur_count = prev_seg_count
        for seg in new_segments:
            sh = segment_hash(seg)
            seg_key = self._k("seg", sh)
            self._client.setnx(seg_key, json.dumps(seg, ensure_ascii=False).encode("utf-8"))
            self._client.expire(seg_key, self._ttl_stable)
            new_pfx_hash, node = next_node(cur_pfx, sh, cur_count)
            pfx_key = self._k("pfx", new_pfx_hash)
            self._client.setnx(pfx_key, json.dumps(node.to_dict()).encode("utf-8"))
            self._client.expire(pfx_key, self._ttl_pfx)
            cur_pfx = new_pfx_hash
            cur_count += 1

        cache_key = f"{sys_hash}::{tools_hash}::{cur_pfx}"
        meta = {
            "sys_hash": sys_hash, "tools_hash": tools_hash,
            "pfx_hash": cur_pfx, "len": prev_meta["len"] + len(new_messages),
            "expire_at": expire_at,
        }
        self._client.set(self._k("meta", cache_key),
                         json.dumps(meta).encode("utf-8"), ex=self._ttl_meta)
        return cache_key

    def _get_seg_count(self, pfx_hash: str) -> int:
        if pfx_hash == EMPTY_HASH:
            return 0
        try:
            raw = self._client.get(self._k("pfx", pfx_hash))
            if raw is None:
                return 0
            return PrefixNode.from_dict(json.loads(raw)).seg_count
        except (redis.RedisError, ValueError, TypeError, KeyError):
            return 0

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------
    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.RedisError:
            return False

    def clear(self) -> None:
        """清空本命名空间。"""
        try:
            for k in self._client.scan_iter(match=f"{self._ns}:*"):
                self._client.delete(k)
        except redis.RedisError as e:
            logger.warning("chunked clear failed: %s", e)

    def __len__(self) -> int:
        try:
            return sum(1 for _ in self._client.scan_iter(match=f"{self._ns}:meta:*"))
        except redis.RedisError:
            return 0
