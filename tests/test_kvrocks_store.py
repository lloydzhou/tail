"""KvrocksStore 测试 —— 连真实 Kvrocks(端口 6666)。

验证 Python 参考版的 Kvrocks 后端:roundtrip、TTL 过期、模型隔离、
大前缀上限、连接容错、命名空间隔离、clear/__len__ 等。

若 Kvrocks 不可达,整套测试 skip(不阻塞 CI)。
"""

from __future__ import annotations

import socket
import time

import pytest

from tail import CacheEntry, KvrocksStore

KVROCKS_HOST = "127.0.0.1"
KVROCKS_PORT = 6666


def _kvrocks_up() -> bool:
    try:
        with socket.create_connection((KVROCKS_HOST, KVROCKS_PORT), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _kvrocks_up(), reason="Kvrocks 不可达(6666)")


@pytest.fixture
def store():
    """每个测试独立的命名空间,避免互相污染。"""
    import random
    ns = f"test_{random.randint(10000, 99999)}"
    s = KvrocksStore(host=KVROCKS_HOST, port=KVROCKS_PORT, ns=ns)
    s.clear()
    yield s
    s.clear()


def _entry(hash_value="h1", *, model="m", prefix_length=2, expire_delta=1000, messages=None):
    return CacheEntry(
        hash=hash_value,
        token_ids=[1, 2],
        messages=messages or [{"role": "user", "content": "a"}],
        model=model,
        prefix_length=prefix_length,
        created_at=time.time(),
        expire_at=time.time() + expire_delta,
    )


# ---- 基础 roundtrip ----


def test_set_and_get(store):
    store.set(_entry("h1", messages=[{"role": "user", "content": "hi"}]))
    got = store.get("h1")
    assert got is not None
    assert got.messages == [{"role": "user", "content": "hi"}]
    assert got.model == "m"


def test_get_miss(store):
    assert store.get("nonexistent") is None


def test_delete(store):
    store.set(_entry("h1"))
    assert store.get("h1") is not None
    assert store.delete("h1") is True
    assert store.get("h1") is None
    assert store.delete("h1") is False  # 已删


def test_overwrite_same_hash(store):
    store.set(_entry("h", prefix_length=1, messages=[{"role": "user", "content": "old"}]))
    store.set(_entry("h", prefix_length=5, messages=[{"role": "user", "content": "new"}]))
    got = store.get("h")
    assert got.prefix_length == 5
    assert got.messages[0]["content"] == "new"


def test_len_and_clear(store):
    for i in range(5):
        store.set(_entry(f"h{i}"))
    assert len(store) == 5
    store.clear()
    assert len(store) == 0


# ---- TTL 过期 ----


def test_expired_entry_returns_none(store):
    store.set(_entry("h", expire_delta=-10))  # 已过期
    assert store.get("h") is None


def test_valid_expire_returned(store):
    store.set(_entry("h", expire_delta=1000))
    got = store.get("h")
    assert got is not None
    assert got.expire_at > time.time()


# ---- 大前缀上限 ----


def test_oversized_entry_silently_dropped(store):
    big_msgs = [{"role": "system", "content": "X" * (5 * 1024 * 1024)}]
    store_with_small_limit = KvrocksStore(
        host=KVROCKS_HOST, port=KVROCKS_PORT,
        ns=store._ns, max_prefix_bytes=1024,
    )
    store_with_small_limit.set(_entry("big", messages=big_msgs))
    assert store_with_small_limit.get("big") is None


# ---- 命名空间隔离 ----


def test_namespace_isolation():
    import random
    ns1 = f"iso1_{random.randint(10000, 99999)}"
    ns2 = f"iso2_{random.randint(10000, 99999)}"
    s1 = KvrocksStore(host=KVROCKS_HOST, port=KVROCKS_PORT, ns=ns1)
    s2 = KvrocksStore(host=KVROCKS_HOST, port=KVROCKS_PORT, ns=ns2)
    s1.clear(); s2.clear()
    try:
        s1.set(_entry("shared_key", messages=[{"role": "user", "content": "in ns1"}]))
        # ns2 看不到 ns1 的同 hash key
        assert s2.get("shared_key") is None
        assert s1.get("shared_key") is not None
    finally:
        s1.clear(); s2.clear()


# ---- 模型/字段一致性 ----


def test_entry_fields_preserved(store):
    store.set(_entry("h", model="gpt-4o", prefix_length=3,
                     messages=[{"role": "system", "content": "S"},
                               {"role": "user", "content": "q1"},
                               {"role": "user", "content": "q2"}]))
    got = store.get("h")
    assert got.model == "gpt-4o"
    assert got.prefix_length == 3
    assert len(got.messages) == 3


def test_ping(store):
    assert store.ping() is True


# ---- 容错:坏的 Kvrocks 地址 ----


def test_unreachable_kvrocks_returns_none():
    bad = KvrocksStore(host="127.0.0.1", port=19999, socket_timeout=0.3)
    assert bad.get("x") is None  # 不抛异常
    assert bad.ping() is False
    bad.set(_entry("x"))  # 不抛异常
    assert len(bad) == 0


# ---- 并发写不同 key ----


def test_concurrent_writes(store):
    import threading
    errors = []

    def writer(i):
        try:
            store.set(_entry(f"h{i}", messages=[{"role": "user", "content": str(i)}]))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(store) == 20
    # 随机抽查
    got = store.get("h10")
    assert got is not None and got.messages[0]["content"] == "10"
