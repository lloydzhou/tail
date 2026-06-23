"""SDK 端到端链路测试:AsyncSDK → 网关 → 后端,模拟真实多轮对话。

验证「对调用方透明」:用户只管传完整 messages,SDK 内部自动协商。
对应设计文档第 6 章。全部走 ASGITransport(进程内),无真实端口。
"""

from __future__ import annotations

import asyncio

import pytest

from tail import protocol
from tail.sdk import _CacheState
from tests.helpers import get_received, make_async_sdk, reset_backend

MODEL = "deepseek-chat"


def _make_stack(*, miss_mode: str = "fast_fail", cache_ttl: int = 3600, auto_retry: bool = True):
    import httpx

    from tail.backend import build_backend_app
    from tail.gateway import build_app
    from tail.store import PrefixCacheStore
    from tail.tokenizer import Tokenizer

    backend_app = build_backend_app()
    gw = build_app(
        store=PrefixCacheStore(max_entries=100),
        tokenizer=Tokenizer(),
        backend_url="http://backend.test",
        backend_transport=httpx.ASGITransport(app=backend_app),
        miss_mode=miss_mode,
        cache_ttl=cache_ttl,
        ttl_jitter=0,
    )
    sdk = make_async_sdk(gw, auto_retry_on_miss=auto_retry)
    return backend_app, gw, sdk


# ---------------------------------------------------------------------------
# 场景 1:多轮对话,SDK 自动协商,后端每次都收到完整 messages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_multi_turn_transparent_caching():
    backend, gw, sdk = _make_stack()
    await reset_backend(backend)

    base = [{"role": "system", "content": "You are a helpful assistant." * 50}]
    msgs1 = base + [{"role": "user", "content": "Hello"}]
    r1 = await sdk.chat_completions(model=MODEL, messages=msgs1)
    assert r1.status_code == 200
    assert r1.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"

    msgs2 = msgs1 + [
        {"role": "assistant", "content": "Hi!"},
        {"role": "user", "content": "How are you?"},
    ]
    r2 = await sdk.chat_completions(model=MODEL, messages=msgs2)
    assert r2.status_code == 200
    assert r2.headers[protocol.HEADER_RESP_CACHE_HIT] == "true"

    seen = await get_received(backend)
    assert [s["messages"] for s in seen] == [msgs1, msgs2]
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 2:首次请求必然 miss,SDK 仍能正确工作
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_first_call_is_miss_but_succeeds():
    backend, gw, sdk = _make_stack(cache_ttl=10)
    await reset_backend(backend)
    msgs = [{"role": "user", "content": "hi"}]
    r1 = await sdk.chat_completions(model=MODEL, messages=msgs)
    assert r1.status_code == 200
    assert r1.headers[protocol.HEADER_RESP_CACHE_HIT] == "false"
    assert len(await get_received(backend)) == 1
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 3:自动重试 —— fast_fail 模式下哈希失效会自动用全量重试一次
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_auto_retry_on_miss():
    backend, gw, sdk = _make_stack(miss_mode="fast_fail")
    await reset_backend(backend)
    key = (MODEL, sdk.base_url)
    sdk._cm._caches[key] = _CacheState(
        cache_hash="stale_hash_xyz",
        expire_time=10**12,
        prefix_messages=[{"role": "system", "content": "old prefix"}],
    )
    msgs = [
        {"role": "system", "content": "old prefix"},
        {"role": "user", "content": "new question"},
    ]
    r = await sdk.chat_completions(model=MODEL, messages=msgs)
    assert r.status_code == 200  # 自动重试后成功
    seen = await get_received(backend)
    # fast_fail 第一次不转发后端;重试全量转发一次
    assert len(seen) == 1
    assert seen[0]["messages"] == msgs
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 4:禁用自动重试时,miss 直接返回 422
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_no_retry_returns_miss_error():
    backend, gw, sdk = _make_stack(miss_mode="fast_fail", auto_retry=False)
    await reset_backend(backend)
    key = (MODEL, sdk.base_url)
    sdk._cm._caches[key] = _CacheState(
        cache_hash="stale_hash_xyz",
        expire_time=10**12,
        prefix_messages=[{"role": "system", "content": "old prefix"}],
    )
    msgs = [
        {"role": "system", "content": "old prefix"},
        {"role": "user", "content": "q"},
    ]
    r = await sdk.chat_completions(model=MODEL, messages=msgs)
    assert r.status_code == 422
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 5:SDK 本地缓存随对话增长
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_prefix_grows_with_conversation():
    backend, gw, sdk = _make_stack()
    await reset_backend(backend)
    key = (MODEL, sdk.base_url)

    m1 = [{"role": "user", "content": "a"}]
    await sdk.chat_completions(model=MODEL, messages=m1)
    assert len((await sdk._get_cache_state(key)).prefix_messages) == 1

    m2 = m1 + [{"role": "assistant", "content": "b"}, {"role": "user", "content": "c"}]
    r2 = await sdk.chat_completions(model=MODEL, messages=m2)
    assert r2.headers[protocol.HEADER_RESP_CACHE_HIT] == "true"
    assert len((await sdk._get_cache_state(key)).prefix_messages) == 3
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 6:并发安全 —— 多协程同时调用不崩,缓存一致
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_concurrency_safety():
    backend, gw, sdk = _make_stack()
    await reset_backend(backend)

    async def worker(i):
        msgs = [{"role": "user", "content": f"msg-{i}"}]
        r = await sdk.chat_completions(model=MODEL, messages=msgs)
        assert r.status_code == 200

    await asyncio.gather(*(worker(i) for i in range(8)))
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 7:多模型互不干扰 —— 不同 model 的缓存独立
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_caches_isolated_per_model():
    backend, gw, sdk = _make_stack()
    await reset_backend(backend)
    key_d = (MODEL, sdk.base_url)
    key_g = ("gpt-4o", sdk.base_url)

    await sdk.chat_completions(model=MODEL, messages=[{"role": "user", "content": "a"}])
    await sdk.chat_completions(model="gpt-4o", messages=[{"role": "user", "content": "b"}])
    sd = await sdk._get_cache_state(key_d)
    sg = await sdk._get_cache_state(key_g)
    assert sd is not None and sg is not None
    assert sd.prefix_messages != sg.prefix_messages
    await sdk.close()


# ---------------------------------------------------------------------------
# 场景 8:带宽节省 —— 长前缀场景下,客户端上传量显著小于后端接收量
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sdk_saves_bandwidth_on_long_prefix():
    import httpx

    from tests.helpers import async_client

    backend, gw, sdk = _make_stack()
    await reset_backend(backend)
    # 构造一个非常长的 system 前缀
    big_system = [{"role": "system", "content": "S" * 5000}]
    msgs1 = big_system + [{"role": "user", "content": "q1"}]
    await sdk.chat_completions(model=MODEL, messages=msgs1)
    # 第二轮只增量
    msgs2 = msgs1 + [{"role": "assistant", "content": "a1"}, {"role": "user", "content": "q2"}]
    await sdk.chat_completions(model=MODEL, messages=msgs2)

    async with async_client(gw) as c:
        s = (await c.get("/__gateway/stats")).json()
    # 两次客户端上传 < 两次后端接收(后端每次都是含长 system 的完整请求)
    assert s["bytes_from_client"] < s["bytes_to_backend"]
    # 节省比例:长 system 前缀在第二轮以增量形式省下。
    # 节省 ≈ 第二轮省下的 system(5000B) / 两次后端接收总和(约 2×5KB+)
    # 这里只要求一个明显的正向节省(> 30%),不追求理论最优。
    saved_ratio = (s["bytes_to_backend"] - s["bytes_from_client"]) / s["bytes_to_backend"]
    assert saved_ratio > 0.30, f"带宽节省过低: {saved_ratio:.2%}"
    await sdk.close()
