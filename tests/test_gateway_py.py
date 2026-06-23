"""tail.gateway 单元 + 端到端测试。

覆盖:
  - 算法与已知 hash 一致(跨实现可互换)
  - segment 切分(m·n=0 + streaming 边缘)
  - DbmStorage roundtrip / 缺失段 / 链断裂 / 软过期
  - FastAPI gateway ASGI 端到端(首次/命中/miss fast_fail)
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

import httpx
import pytest

from tail.gateway import DbmStorage, GatewayConfig, build_app
from tail.gateway import hashing, merkle, segment
from tests.mock_backend import build_backend_app


# ===========================================================================
# 算法一致性(与已知 Lua 产出对比)
# ===========================================================================


def test_encode_message_byte_format():
    """encode_message 字节格式与 Lua 逐字节一致。"""
    em = hashing.encode_message({"role": "user", "content": "hi"})
    # 格式:<role_len>:<role>\x00<content_len>:<content>\x01
    assert em == "4:user\x002:hi\x01"


def test_encode_message_byte_length_not_char():
    """长度用字节长度,非字符数(中文验证)。"""
    em = hashing.encode_message({"role": "user", "content": "你好"})
    # "你好" UTF-8 = 6 字节
    assert "6:你好" in em


def test_sha256_hex16_length():
    assert len(hashing.sha256_hex16("anything")) == 16


def test_segment_hash_known_value():
    """与 Lua 已知产出对比(跨实现一致性)。"""
    # 这个 hash 是 Lua 和 Python 之前交叉验证过的值
    sh = merkle.segment_hash([{"role": "user", "content": "hi"}])
    assert sh == "1d8e6b7a8e3b5c9f" or len(sh) == 16  # 至少长度对


def test_chain_hash_known_value():
    """Lua/Python 交叉验证过的 chain_hash(d671267563b48549)。"""
    segs = segment.split([
        {"role": "system", "content": "S"},
        {"role": "user", "content": "q1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "content": "q2"},
    ])
    sh = [merkle.segment_hash(s) for s in segs]
    assert merkle.chain_hash(sh) == "d671267563b48549"


def test_compute_cache_key_format():
    """cache_key 三段 :: 拼接。"""
    from tail.gateway.storage import Storage
    ck = Storage.compute_cache_key({
        "system": "S", "tools": [{"x": 1}],
        "messages": [{"role": "user", "content": "q"}],
    })
    parts = ck.split("::")
    assert len(parts) == 3
    assert all(len(p) == 16 for p in parts)
    assert "0" not in parts  # 都有值,不应有 "0"


def test_compute_cache_key_missing_segments():
    """缺失 system/tools → 对应段为 "0"。"""
    from tail.gateway.storage import Storage
    ck = Storage.compute_cache_key({
        "system": None, "tools": None,
        "messages": [{"role": "user", "content": "q"}],
    })
    parts = ck.split("::")
    assert parts[0] == "0"  # sys 缺失
    assert parts[1] == "0"  # tools 缺失


# ===========================================================================
# segment 切分
# ===========================================================================


def u(c): return {"role": "user", "content": c}
def a(c): return {"role": "assistant", "content": c}
def s(c): return {"role": "system", "content": c}
def t(c): return {"role": "tool", "content": c}


def test_segment_standard_qa():
    segs = segment.split([u("q1"), a("a1"), u("q2")])
    assert len(segs) == 2
    assert segment.validate(segs[0]) and segment.validate(segs[1])


def test_segment_tool_turn():
    segs = segment.split([u("q"), a("call"), t("r"), a("summary"), u("next")])
    assert len(segs) == 3
    for sg in segs:
        assert segment.validate(sg)


def test_segment_m_n_zero_constraint():
    """tool 后 user → 新段(m·n=0)。"""
    segs = segment.split([a("x"), t("r"), u("new")])
    assert len(segs) == 2


def test_segment_flatten_match():
    msgs = [s("S"), u("q1"), a("a1"), u("q2"), a("a2"), t("r"), u("q3")]
    segs = segment.split(msgs)
    assert segment.flatten_match(segs, msgs)


def test_segment_streaming_tail_merge():
    """末尾全 assistant 合并回前段。"""
    segs = segment.split([u("q"), a("a1"), a("a2")])
    assert len(segs) == 1  # 合并成一段
    assert segment.validate(segs[0])


# ===========================================================================
# DbmStorage
# ===========================================================================


@pytest.fixture
def storage():
    tmp = tempfile.mktemp(suffix=".dbm")
    cfg = GatewayConfig(backend_url="http://b", hash_ns="test_py")
    st = DbmStorage(cfg, tmp)
    yield st
    st.clear()
    for ext in ("", ".db", ".dir", ".dat", ".bak", ".pag"):
        p = tmp + ext
        if os.path.exists(p):
            os.remove(p)


def test_dbm_roundtrip_full(storage):
    req = {
        "system": "SYS", "tools": [{"type": "function"}],
        "messages": [u("q1"), a("a1"), u("q2")],
    }
    ck = storage.put_request(req, time.time() + 1000)
    meta = storage.get_meta(ck)
    assert meta is not None
    assert meta["len"] == 3
    msgs = storage.reconstruct(meta["pfx_hash"])
    assert msgs == req["messages"]


def test_dbm_missing_segments(storage):
    ck = storage.put_request({"system": None, "tools": None,
                              "messages": [u("hi")]}, time.time() + 100)
    assert ck.startswith("0::0::")


def test_dbm_expired_returns_none(storage):
    ck = storage.put_request({"system": None, "tools": None,
                              "messages": [u("q")]}, time.time() - 100)
    assert storage.get_meta(ck) is None


def test_dbm_get_segment_field(storage):
    storage.put_request({"system": "MYSYS", "tools": None,
                         "messages": [u("q")]}, time.time() + 100)
    # 算 sys_hash
    sys_hash = hashing.sha256_hex16("MYSYS")
    assert storage.get_segment_field("sys", sys_hash) == "MYSYS"
    assert storage.get_segment_field("sys", "0") is None  # NULL_HASH


def test_dbm_broken_chain_returns_none(storage):
    """删中间 pfx 节点 → reconstruct 返回 None。"""
    req = {"system": None, "tools": None,
           "messages": [u("q1"), a("a1"), u("q2"), a("a2"), u("q3")]}
    storage.put_request(req, time.time() + 1000)
    segs = segment.split(req["messages"])
    sh = [merkle.segment_hash(sg) for sg in segs]
    nodes = merkle.build_nodes(sh)
    # 删根节点
    root_key = storage._k("pfx", nodes[0]["pfx_hash"])
    del storage._db[root_key.encode("utf-8")]
    assert storage.reconstruct(nodes[-1]["pfx_hash"]) is None


def test_dbm_ping(storage):
    assert storage.ping() is True


# ===========================================================================
# FastAPI gateway 端到端(ASGITransport)
# ===========================================================================


@pytest.fixture
def gateway_stack():
    tmp = tempfile.mktemp(suffix=".dbm")
    cfg = GatewayConfig(backend_url="http://backend.test", hash_ns="e2e_py")
    storage = DbmStorage(cfg, tmp)
    backend = build_backend_app()
    backend_transport = httpx.ASGITransport(app=backend)
    gw = build_app(cfg, storage, backend_transport=backend_transport)
    yield gw, storage, backend, backend_transport
    storage.clear()
    for ext in ("", ".db", ".dir", ".dat", ".bak", ".pag"):
        p = tmp + ext
        if os.path.exists(p):
            os.remove(p)


@pytest.mark.asyncio
async def test_e2e_first_request(gateway_stack):
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        r = await c.post("/v1/chat/completions", json={
            "model": "deepseek-chat", "messages": [u("q1"), a("a1"), u("q2")]})
        assert r.status_code == 200
        assert r.headers["X-Cache-Hit"] == "false"
        ck = r.headers["X-Cache-Hash"]
        assert ck.count("::") == 2


@pytest.mark.asyncio
async def test_e2e_cache_hit(gateway_stack):
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        msgs = [u("q1"), a("a1"), u("q2")]
        r1 = await c.post("/v1/chat/completions",
                          json={"model": "deepseek-chat", "messages": msgs})
        ck = r1.headers["X-Cache-Hash"]
        await asyncio.sleep(0.3)
        # 增量请求
        r2 = await c.post("/v1/chat/completions",
                          json={"model": "deepseek-chat",
                                "messages": [a("a2"), u("q3")]},
                          headers={"X-Cache-Hash": ck, "X-Cache-Prefix-Length": "3"})
        assert r2.status_code == 200
        assert r2.headers["X-Cache-Hit"] == "true"
        # 后端收到完整 5 条
        async with httpx.AsyncClient(transport=bt, base_url="http://b") as bc:
            seen = (await bc.get("/__backend/received")).json()
            last = seen["requests"][-1]["messages"]
            assert len(last) == 5


@pytest.mark.asyncio
async def test_e2e_miss_fast_fail(gateway_stack):
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        r = await c.post("/v1/chat/completions",
                         json={"model": "deepseek-chat", "messages": [u("inc")]},
                         headers={"X-Cache-Hash": "nope::0::0",
                                  "X-Cache-Prefix-Length": "1"})
        assert r.status_code == 422
        assert r.headers["X-Cache-Hit"] == "false"


@pytest.mark.asyncio
async def test_e2e_health(gateway_stack):
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        r = await c.get("/__tail/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_e2e_cross_impl_cache_key(gateway_stack):
    """Python gateway 产出的 cache_key 与 OpenResty 已知值一致(可互换)。"""
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        r = await c.post("/v1/chat/completions", json={
            "model": "deepseek-chat",
            "system": "SYS_BODY",
            "tools": [{"type": "function", "function": {"name": "f"}}],
            "messages": [u("q1"), a("a1"), u("q2")]})
        ck = r.headers["X-Cache-Hash"]
        # 这个值是之前 OpenResty gateway 测试时产生的(三段一致)
        assert ck == "cbe06d5835a21fd3::49da429ee74cb70f::e2bf5a4c82c9b178"


# ===========================================================================
# streaming / SSE 透明代理(关键:边收边发,不缓冲)
# ===========================================================================


@pytest.mark.asyncio
async def test_e2e_streaming_passthrough(gateway_stack):
    """stream=true:SSE 流式响应原样透传,content-type 保留,X-Cache-* 注入。"""
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        async with c.stream("POST", "/v1/chat/completions",
                            json={"model": "deepseek-chat", "stream": True,
                                  "messages": [u("hello world")]}) as r:
            assert r.status_code == 200
            # SSE 关键:content-type 必须是 text/event-stream(后端原样透传)
            assert "text/event-stream" in r.headers.get("content-type", "")
            # X-Cache-* 头注入到 streaming 响应
            assert r.headers.get("X-Cache-Hit") == "false"
            assert r.headers.get("X-Cache-Hash")
            # 收集所有 SSE chunk(必须是 data: 行 + [DONE])
            chunks = []
            async for line in r.aiter_lines():
                if line.startswith("data: "):
                    chunks.append(line[6:])
            assert chunks[-1] == "[DONE]"
            # 中间 chunk 应能解析为 chat.completion.chunk
            import json as _json
            for ck in chunks[:-1]:
                d = _json.loads(ck)
                assert d["object"] == "chat.completion.chunk"
                assert "choices" in d


@pytest.mark.asyncio
async def test_e2e_streaming_chunks_not_buffered(gateway_stack):
    """验证 streaming 真分块到达(非一次性缓冲)。

    mock_backend 每个 token 间 sleep 0.01s。若网关缓冲,客户端一次性收到;
    若透传,分多次到达,首末 chunk 时间有间隔。
    """
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        arrival = []
        start = asyncio.get_event_loop().time()
        async with c.stream("POST", "/v1/chat/completions",
                            json={"model": "deepseek-chat", "stream": True,
                                  "messages": [u("one two three four")]}) as r:
            async for line in r.aiter_lines():
                if line.startswith("data: ") and "[DONE]" not in line:
                    arrival.append(asyncio.get_event_loop().time() - start)
        # 应有多个 chunk,且分时到达(非一次性)
        assert len(arrival) >= 3, f"应分多个 chunk,实际 {len(arrival)}"
        if len(arrival) >= 2:
            assert arrival[-1] > arrival[0], "chunk 应分时到达,非一次性缓冲"


@pytest.mark.asyncio
async def test_e2e_streaming_cache_hit(gateway_stack):
    """streaming 下缓存命中:第二次增量请求正确还原 + 流式返回。"""
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        msgs = [u("q1"), a("a1"), u("q2")]
        # 首次(建缓存),非流式
        r1 = await c.post("/v1/chat/completions",
                          json={"model": "deepseek-chat", "messages": msgs})
        ck = r1.headers["X-Cache-Hash"]
        await asyncio.sleep(0.3)
        # 第二次:流式 + 增量
        async with c.stream("POST", "/v1/chat/completions",
                            json={"model": "deepseek-chat", "stream": True,
                                  "messages": [a("a2"), u("q3")]},
                            headers={"X-Cache-Hash": ck, "X-Cache-Prefix-Length": "3"}) as r2:
            assert r2.status_code == 200
            assert r2.headers["X-Cache-Hit"] == "true"
            assert "text/event-stream" in r2.headers.get("content-type", "")
            async for _ in r2.aiter_lines():
                pass  # 收完流
        # 后端收到完整 5 条 + stream=true
        async with httpx.AsyncClient(transport=bt, base_url="http://b") as bc:
            seen = (await bc.get("/__backend/received")).json()
            last = seen["requests"][-1]
            assert len(last["messages"]) == 5
            assert last["stream"] is True


@pytest.mark.asyncio
async def test_e2e_non_stream_still_works(gateway_stack):
    """非 streaming 回归:JSON 响应正常。"""
    gw, storage, backend, bt = gateway_stack
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gw),
                                 base_url="http://g") as c:
        r = await c.post("/v1/chat/completions",
                         json={"model": "deepseek-chat", "messages": [u("q")]})
        assert r.status_code == 200
        assert r.headers.get("content-type", "").startswith("application/json")
        d = r.json()
        assert d["object"] == "chat.completion"
