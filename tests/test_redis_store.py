"""Redis 链式存储的真实集成测试(需要一台可连的 Redis)。

默认连 ``redis://127.0.0.1:6390/0``,可用 ``TAIL_REDIS_URL`` 覆盖。
连不上就整体 skip——CI 上不该因为它挂掉,但本地必须能跑通:
Lua、定长 parent 编码、TTL 续期这些东西**只有**真跑过 Redis 才算验证。

一条命令起一个临时实例::

    docker run -d --rm -p 6390:6379 redis:7-alpine
"""

from __future__ import annotations

import os
import random
import time

import pytest

redis = pytest.importorskip("redis")

from tail.transport import storage  # noqa: E402
from tail.transport.gateway import roundtrip  # noqa: E402

_URL = os.environ.get("TAIL_REDIS_URL", "redis://127.0.0.1:6390/0")


@pytest.fixture(scope="module")
def client():
    try:
        c = redis.Redis.from_url(_URL, socket_connect_timeout=1)
        c.ping()
    except Exception as exc:  # pragma: no cover - 环境缺失
        pytest.skip(f"连不上 Redis({_URL}):{exc}")
    yield c
    c.close()


@pytest.fixture
def store(client):
    st = storage.RedisChainStore(client, ttl=60, namespace="tailtest")
    st.clear()
    yield st
    st.clear()


def _chain(text: str, block_size: int = 64):
    """切满块。不满一块的尾巴不进链(与线上一致),所以用例断言时要按满块长度比。"""
    items = storage.text_blocks(text.encode(), block_size)
    return items, text.encode()[:len(items) * block_size]


class TestRedisChainStore:
    def test_put_and_read_roundtrip(self, store):
        text = "".join(chr(0x4e00 + i % 500) for i in range(400))   # 多字节正文
        items, want = _chain(text, 64)
        assert len(items) >= 3
        tip = store.put_chain(b"", items)
        assert tip == items[-1][0]

        contents = store.read_prefix(tip, len(items))
        assert contents is not None
        # 顺序必须是「从前到后」:脚本按走链方向返回,实现里倒过来
        assert [bytes(c) for c in contents] == [block for _bid, block in items]
        assert b"".join(contents) == want

    def test_contents_are_zero_copy_views(self, store):
        """正文用 memoryview 从 value 里切出来,不是 ``v[32:]`` 的拷贝。"""
        items, _ = _chain("z" * 200, 64)
        tip = store.put_chain(b"", items)
        contents = store.read_prefix(tip, len(items))
        assert contents and all(isinstance(c, memoryview) for c in contents)

    def test_root_block_content_is_exact(self, store):
        """根块的正文必须原样存、原样取。

        回归:根块的父哈希早先写成空串,``v[32:]`` 会吃掉根块正文的前 32 字节。
        父哈希必须定长(16 字节零哈希的 hex)。
        """
        first = b"x" * 64
        rest = b"y" * 64
        items = storage.text_blocks(first + rest, 64)
        tip = store.put_chain(b"", items)
        contents = store.read_prefix(tip, 2)
        assert contents is not None
        assert bytes(contents[0]) == first
        # value 本身仍是「32 B 父哈希 + 正文」
        raw = store._client.get(store._key(items[0][0]))
        assert raw == b"0" * 32 + first

    def test_missing_middle_block_is_incomplete(self, store, client):
        items, _ = _chain("b" * 400, 64)
        tip = store.put_chain(b"", items)
        client.delete(store._key(items[1][0]))
        assert store.read_prefix(tip, len(items)) is None

    def test_missing_tip_block_is_incomplete(self, store, client):
        items, _ = _chain("t" * 200, 64)
        tip = store.put_chain(b"", items)
        client.delete(store._key(items[-1][0]))
        assert store.read_prefix(tip, len(items)) is None

    def test_partial_length(self, store):
        items, _ = _chain("c" * 400, 64)
        tip = store.put_chain(b"", items)
        got = store.read_prefix(tip, 2)
        assert got is not None and [bytes(c) for c in got] == [
            items[-2][1], items[-1][1]]
        assert store.read_prefix(tip, 0) == []
        assert store.read_prefix(tip, len(items) + 1) is None   # 越过根 = 不完整

    def test_ttl_is_refreshed_on_read(self, client):
        st = storage.RedisChainStore(client, ttl=2, namespace="tailtest-ttl")
        st.clear()
        items, _ = _chain("d" * 200, 64)
        tip = st.put_chain(b"", items)
        time.sleep(1.5)
        assert st.read_prefix(tip, len(items)) is not None    # 读一次续期
        time.sleep(1.2)
        # 若没续期,第 0 块这时已经过期(2.7s > 2s)
        assert st.read_prefix(tip, len(items)) is not None
        # 整条前缀的 key 都必须被续上,不能只续链尾
        assert client.ttl(st._key(items[0][0])) > 0
        assert client.ttl(st._key(items[-1][0])) > 0

    def test_expired_chain_reads_as_miss(self, client):
        st = storage.RedisChainStore(client, ttl=1, namespace="tailtest-exp")
        st.clear()
        items, _ = _chain("e" * 200, 64)
        tip = st.put_chain(b"", items)
        time.sleep(1.2)
        assert st.read_prefix(tip, len(items)) is None

    def test_clear_only_touches_own_namespace(self, client, store):
        other = storage.RedisChainStore(client, ttl=60, namespace="tailtest-other")
        other.clear()
        items, _ = _chain("f" * 200, 64)
        tip_other = other.put_chain(b"", items)
        tip = store.put_chain(b"", items)
        store.clear()
        assert store.read_prefix(tip, len(items)) is None
        assert other.read_prefix(tip_other, len(items)) is not None
        other.clear()


class TestFullPathOnRedis:
    def test_gateway_roundtrip_over_redis(self, store):
        rng = random.Random(1)
        msgs = [{"role": "system", "content": "你是助手" + "细节" * 80}]
        for i in range(4):
            msgs.append({"role": "user", "content": f"问{i}:" + "描述" * 60})
            msgs.append({"role": "assistant", "content": f"答{i}:" + "结论" * 90})
        payload = {"model": "deepseek-chat", "messages": msgs}

        from tail.transport.gateway import TailClient
        gw_client = TailClient(block_size=256)
        assert roundtrip(payload, store, gw_client) == payload
        # 第二轮全命中:客户端只发不足一块的尾巴
        before = sum(1 for _ in store._client.scan_iter(
            match=store._ns + storage.KEY_BLOCK + "*"))
        back = roundtrip(payload, store, gw_client)
        assert back == payload
        after = sum(1 for _ in store._client.scan_iter(
            match=store._ns + storage.KEY_BLOCK + "*"))
        assert after == before, "同样的内容不该再写块"

    def test_have_roundtrip_through_url_factory(self, client):
        st = storage.make_store(_URL, ttl=60)
        assert isinstance(st, storage.RedisChainStore)
        st.clear()
        items, _ = _chain("h" * 300, 64)
        tip = st.put_chain(b"", items)
        got = st.read_prefix(tip, len(items))
        assert got is not None and [bytes(c) for c in got] == [
            block for _bid, block in items]
        st.clear()
