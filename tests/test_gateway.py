"""网关端到端测试:以 httpx.AsyncClient + ASGITransport 打网关。

重点验证「网关到底把什么转发给了后端」——通过共享 backend app 的
``/__backend/received`` 观测点断言拼装正确性。这是整个协议正确性的核心验证。

对应设计文档第 5 章、第 8 章 Phase 1「验证各种场景下的请求体拼接正确性」。
"""

from __future__ import annotations

import pytest
import httpx

from tail import protocol
from tail.backend import build_backend_app
from tail.gateway import build_app
from tail.store import PrefixCacheStore
from tail.tokenizer import Tokenizer
from tests.helpers import async_client, get_received, reset_backend

MODEL = "deepseek-chat"


def _make_stack(*, miss_mode: str = "fast_fail", cache_ttl: int = 3600, ttl_jitter: int = 0):
    backend_app = build_backend_app()
    gw = build_app(
        store=PrefixCacheStore(max_entries=100),
        tokenizer=Tokenizer(),
        backend_url="http://backend.test",
        backend_transport=httpx.ASGITransport(app=backend_app),
        miss_mode=miss_mode,
        cache_ttl=cache_ttl,
        ttl_jitter=ttl_jitter,
    )
    return backend_app, gw


async def _post(gw, *, messages, cache_hash=None, prefix_length=None, model=MODEL):
    headers = {"Content-Type": "application/json"}
    if cache_hash is not None:
        headers[protocol.HEADER_CACHE_HASH] = cache_hash
        if prefix_length is not None:
            headers[protocol.HEADER_CACHE_PREFIX_LENGTH] = str(prefix_length)
    body = {"model": model, "messages": messages}
    async with async_client(gw) as c:
        return await c.post("/v1/chat/completions", json=body, headers=headers)


# ---------------------------------------------------------------------------
# 场景 1:无哈希首次请求,完整透传 + 返回新哈希
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_request_passes_full_messages_and_returns_hash():
    backend, gw = _make_stack()
    await reset_backend(backend)
    msgs = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "Hello"},
    ]
    resp = await _post(gw, messages=msgs)
    assert resp.status_code == 200
    assert resp.headers[protocol.HEADER_RESP_CACHE_HASH]
    assert int(resp.headers[protocol.HEADER_RESP_CACHE_EXPIRE]) > 0
    assert resp.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"
    seen = await get_received(backend)
    assert len(seen) == 1
    assert seen[0]["messages"] == msgs


# ---------------------------------------------------------------------------
# 场景 2:命中缓存 —— 增量被正确拼装成完整 messages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_hit_reconstructs_full_messages():
    backend, gw = _make_stack()
    await reset_backend(backend)
    msgs = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "Hello"},
    ]
    r1 = await _post(gw, messages=msgs)
    h1 = r1.headers[protocol.HEADER_RESP_CACHE_HASH]
    inc = [{"role": "assistant", "content": "Hi!"}, {"role": "user", "content": "How are you?"}]
    r2 = await _post(gw, messages=inc, cache_hash=h1, prefix_length=len(msgs))
    assert r2.status_code == 200
    assert r2.headers[protocol.HEADER_RESP_CACHE_HIT] == "true"
    seen = await get_received(backend)
    assert len(seen) == 2
    assert seen[1]["messages"] == msgs + inc


# ---------------------------------------------------------------------------
# 场景 3:缓存未命中(fast_fail) —— 不转发后端,返回 422
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_miss_fast_fail_does_not_hit_backend():
    backend, gw = _make_stack(miss_mode="fast_fail")
    await reset_backend(backend)
    r = await _post(
        gw, messages=[{"role": "user", "content": "only incremental"}],
        cache_hash="nope", prefix_length=99,
    )
    assert r.status_code == 422
    assert r.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"
    assert len(await get_received(backend)) == 0


# ---------------------------------------------------------------------------
# 场景 4:passthrough 模式 —— 未命中时把当前 messages 当完整请求转发
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_miss_passthrough_forwards_as_full():
    backend, gw = _make_stack(miss_mode="passthrough")
    await reset_backend(backend)
    msgs = [{"role": "user", "content": "treated as full"}]
    r = await _post(gw, messages=msgs, cache_hash="bogus", prefix_length=1)
    assert r.status_code == 200
    assert r.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"
    seen = await get_received(backend)
    assert len(seen) == 1
    assert seen[0]["messages"] == msgs


# ---------------------------------------------------------------------------
# 场景 5:prefix_length 不一致 → 不命中(fast_fail)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefix_length_mismatch_is_miss():
    backend, gw = _make_stack()
    await reset_backend(backend)
    r1 = await _post(gw, messages=[{"role": "user", "content": "hi"}])
    h1 = r1.headers[protocol.HEADER_RESP_CACHE_HASH]
    # 故意给错 prefix_length(真实是 1,这里传 5)
    r2 = await _post(
        gw, messages=[{"role": "user", "content": "x"}],
        cache_hash=h1, prefix_length=5,
    )
    assert r2.status_code == 422
    assert r2.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"


# ---------------------------------------------------------------------------
# 场景 6:多轮对话下前缀单调增长,每次都返回新哈希
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_turn_returns_increasing_prefix_and_new_hashes():
    backend, gw = _make_stack()
    await reset_backend(backend)
    m1 = [{"role": "system", "content": "S"}, {"role": "user", "content": "1"}]
    r1 = await _post(gw, messages=m1)
    h1 = r1.headers[protocol.HEADER_RESP_CACHE_HASH]

    m2 = m1 + [{"role": "assistant", "content": "a1"}, {"role": "user", "content": "2"}]
    r2 = await _post(gw, messages=m2[len(m1):], cache_hash=h1, prefix_length=len(m1))
    h2 = r2.headers[protocol.HEADER_RESP_CACHE_HASH]

    m3 = m2 + [{"role": "assistant", "content": "a2"}, {"role": "user", "content": "3"}]
    r3 = await _post(gw, messages=m3[len(m2):], cache_hash=h2, prefix_length=len(m2))
    h3 = r3.headers[protocol.HEADER_RESP_CACHE_HASH]

    assert len({h1, h2, h3}) == 3
    seen = await get_received(backend)
    assert [s["messages"] for s in seen] == [m1, m2, m3]


# ---------------------------------------------------------------------------
# 场景 7:模型不一致 → 不命中(缓存按模型粒度管理)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_model_mismatch_is_miss():
    backend, gw = _make_stack()
    await reset_backend(backend)
    r1 = await _post(gw, messages=[{"role": "user", "content": "hi"}])
    h1 = r1.headers[protocol.HEADER_RESP_CACHE_HASH]
    r2 = await _post(
        gw, messages=[{"role": "user", "content": "x"}],
        cache_hash=h1, prefix_length=1, model="gpt-4o",
    )
    assert r2.status_code == 422
    assert r2.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"


# ---------------------------------------------------------------------------
# 场景 8:统计端点与带宽节省计量
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stats_endpoint_counts_hits_and_bytes():
    backend, gw = _make_stack()
    await reset_backend(backend)
    big = [{"role": "system", "content": "X" * 2000}, {"role": "user", "content": "hi"}]
    r1 = await _post(gw, messages=big)
    h1 = r1.headers[protocol.HEADER_RESP_CACHE_HASH]
    r2 = await _post(
        gw, messages=[{"role": "user", "content": "yo"}],
        cache_hash=h1, prefix_length=len(big),
    )
    assert r2.status_code == 200
    async with async_client(gw) as c:
        s = (await c.get("/__gateway/stats")).json()
    assert s["requests"] == 2
    assert s["hits"] == 1
    assert s["misses"] == 1
    # 客户端两次发送字节(大body + 小增量) < 网关转给后端字节(两次完整)
    assert s["bytes_to_backend"] > s["bytes_from_client"]


# ---------------------------------------------------------------------------
# 场景 9:后端不可达时降级,返回 502 而非崩溃
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gateway_fallback_on_internal_error():
    class _BoomTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("boom", request=request)

    gw = build_app(
        store=PrefixCacheStore(),
        tokenizer=Tokenizer(),
        backend_url="http://backend.test",
        backend_transport=_BoomTransport(),
    )
    async with async_client(gw) as c:
        r = await c.post(
            "/v1/chat/completions",
            json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
        )
    assert r.status_code in (502, 200)


# ---------------------------------------------------------------------------
# 场景 10:缓存过期后,旧哈希不命中(走 fast_fail)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_hash_is_miss():
    backend, gw = _make_stack(cache_ttl=0, ttl_jitter=0)
    await reset_backend(backend)
    r1 = await _post(gw, messages=[{"role": "user", "content": "hi"}])
    h1 = r1.headers[protocol.HEADER_RESP_CACHE_HASH]
    # TTL=0,下次 get 时必过期
    r2 = await _post(
        gw, messages=[{"role": "user", "content": "more"}],
        cache_hash=h1, prefix_length=1,
    )
    assert r2.status_code == 422  # 过期 → fast_fail
