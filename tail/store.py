"""前缀缓存存储 —— 进程内 LRU + TTL 实现。

对应设计文档第 5.1 节「缓存存储」中的「本地共享内存(LRU 淘汰)」,
以及第 9 章「本地缓存采用 LRU+TTL」。

生产 OpenResty 版本对应 lua_shared_dict + LRU;二级 Redis 缓存在本
参考实现中暂不包含,但存储接口已抽象(``PrefixCacheStore``),可平滑
接入 Redis 作为二级缓存。
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class CacheEntry:
    """单条前缀缓存。

    Attributes:
      hash:         前缀哈希(同时作为存储 key)。
      token_ids:    前缀的 token 序列,用于哈希校验。
      messages:     前缀的完整消息数组,用于还原请求体。
      model:        模型名(缓存按模型粒度管理,见第 9 章风险表)。
      prefix_length:前缀消息条数。
      created_at:   创建时间(Unix 秒)。
      expire_at:    过期时间(Unix 秒)。
      last_access:  最近访问时间(LRU 依据)。
    """

    hash: str
    token_ids: List[int]
    messages: List[dict]
    model: str
    prefix_length: int
    created_at: float
    expire_at: float
    last_access: float = field(default_factory=time.time)


class PrefixCacheStore:
    """线程安全的 LRU + TTL 缓存。

    - 命中时刷新 ``last_access`` 并把条目移到队尾(LRU 依据)。
    - 过期(超 TTL)或容量超限时淘汰。
    """

    def __init__(self, max_entries: int = 10000):
        self._max = max_entries
        self._data: "OrderedDict[str, CacheEntry]" = OrderedDict()
        self._lock = threading.RLock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def get(self, hash_value: str, now: float | None = None) -> Optional[CacheEntry]:
        """获取缓存条目;过期或不存在返回 None,命中时刷新 LRU 顺序。"""
        if now is None:
            now = time.time()
        with self._lock:
            entry = self._data.get(hash_value)
            if entry is None:
                return None
            if entry.expire_at <= now:
                # 惰性淘汰过期项。
                self._data.pop(hash_value, None)
                return None
            entry.last_access = now
            self._data.move_to_end(hash_value)
            return entry

    def set(self, entry: CacheEntry) -> None:
        with self._lock:
            self._data[entry.hash] = entry
            self._data.move_to_end(entry.hash)
            self._evict_locked()

    def delete(self, hash_value: str) -> bool:
        with self._lock:
            return self._data.pop(hash_value, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def _evict_locked(self) -> None:
        """在已持锁情况下,先淘汰过期项,再按 LRU 淘汰到容量上限以内。"""
        now = time.time()
        if self._data:
            expired = [k for k, v in self._data.items() if v.expire_at <= now]
            for k in expired:
                self._data.pop(k, None)
        while len(self._data) > self._max:
            self._data.popitem(last=False)
