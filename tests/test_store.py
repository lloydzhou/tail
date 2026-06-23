"""单元测试:LRU + TTL 缓存存储。

对应设计文档第 5.1 节、第 9 章「LRU+TTL」。
"""

import time

from tail.store import CacheEntry, PrefixCacheStore


def _entry(hash_value="h1", expire_at=None, model="m", prefix_length=2):
    return CacheEntry(
        hash=hash_value,
        token_ids=[1, 2],
        messages=[{"role": "user", "content": "a"}],
        model=model,
        prefix_length=prefix_length,
        created_at=time.time(),
        expire_at=expire_at if expire_at is not None else time.time() + 100,
    )


def test_set_and_get():
    store = PrefixCacheStore()
    store.set(_entry("h1"))
    assert store.get("h1") is not None
    assert store.get("nope") is None


def test_expired_entry_is_evicted_lazily():
    store = PrefixCacheStore()
    store.set(_entry("h1", expire_at=time.time() - 1))
    assert store.get("h1") is None
    assert len(store) == 0


def test_lru_eviction_when_capacity_exceeded():
    store = PrefixCacheStore(max_entries=2)
    store.set(_entry("a"))
    store.set(_entry("b"))
    # 访问 a,使 b 成为最旧
    store.get("a")
    store.set(_entry("c"))
    assert store.get("a") is not None  # a 被访问过,保留
    assert store.get("c") is not None
    assert store.get("b") is None  # b 被 LRU 淘汰


def test_get_refreshes_lru_order():
    store = PrefixCacheStore(max_entries=2)
    store.set(_entry("a"))
    store.set(_entry("b"))
    # a 最旧;不访问 a,直接插入 c → a 被淘汰
    store.set(_entry("c"))
    assert store.get("a") is None
    assert store.get("b") is not None


def test_delete_and_clear():
    store = PrefixCacheStore()
    store.set(_entry("a"))
    assert store.delete("a") is True
    assert store.delete("a") is False
    store.set(_entry("b"))
    store.clear()
    assert len(store) == 0


def test_overwrite_same_hash_updates_entry():
    store = PrefixCacheStore()
    store.set(_entry("h", prefix_length=1))
    store.set(_entry("h", prefix_length=5))
    e = store.get("h")
    assert e.prefix_length == 5
    assert len(store) == 1
