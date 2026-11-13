"""端到端测试:客户端 → 网关(ASGI)→ mock 上游后端。

验证整条链路真的通,且上游收到的 messages 与客户端发出的一致。
重点覆盖:
  - 流式重建的 JSON 上游能正确解析
  - 多轮对话的线上体积收敛
  - 存储失效时的降级与恢复
  - 普通 JSON 透传仍可用
"""

from __future__ import annotations

import asyncio
import random
import time

import httpx2 as httpx
import pytest
from fastapi.testclient import TestClient

from tail import serve
from tail.transport import gateway as gw
from tail.transport import storage

from .mock_backend import build_backend_app

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 "
          "缓存 传输 协议 分块 网关 客户端").split()


def _sent(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(_WORDS) + ("，" if rng.random() < 0.3 else "")
                   for _ in range(n))


@pytest.fixture
def backend():
    return build_backend_app()


@pytest.fixture
def gateway_app(backend):
    st = storage.MemoryChainStore()
    app = serve.build_app("http://mock", st)
    app.state.client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=backend), timeout=30)
    return app


def _post(tc, body, session=b"", have_len=0, have_tip=b""):
    """按 v3 线缆发一个请求:会话键 + 前缀凭据走头,body 只有尾部字节。"""
    headers = {"content-type": gw.CONTENT_TYPE_BLOCKS}
    if session:
        headers[gw.HEADER_SESSION] = session.hex()
    if have_len:
        headers[gw.HEADER_HAVE] = f"{have_len};{have_tip.hex()}"
    return tc.post("/v1/chat/completions", content=body, headers=headers)


def _send(tc, client, payload):
    """客户端 prepare → 发 → 用响应头更新凭据。返回 (响应, 线上字节数)。"""
    prepared = client.prepare(payload)
    body = b"".join(prepared.body)
    r = _post(tc, body, prepared.session, prepared.have_len, prepared.have_tip)
    if r.status_code == 200:
        client.update_from_headers(prepared.session, r.headers)
    return r, len(body)


class TestAsyncToSyncBody:
    """请求体异步流 → 同步迭代 的桥接。"""

    def test_stream_error_is_propagated(self):
        """上游/网络在传输中途出错时,错误必须传给消费端。

        回归:早先 ``_pump`` 的异常被 asyncio.Task 吞掉且无人 await,消费端只看到
        哨兵、当成正常结束——结果是**把被截断的请求静默转发给上游**。
        """
        class _BadRequest:
            async def stream(self):
                yield b"a" * 16
                raise RuntimeError("client disconnected mid-body")

        body = serve._AsyncToSyncBody(_BadRequest())

        async def run():
            body.start()
            # __iter__ 会阻塞,必须放到线程里,让事件循环继续跑 pump
            return await asyncio.to_thread(lambda: list(body))

        with pytest.raises(RuntimeError, match="client disconnected"):
            asyncio.run(run())

    def test_normal_stream_completes(self):
        class _OkRequest:
            async def stream(self):
                for piece in (b"aa", b"bb", b"", b"cc"):
                    yield piece

        body = serve._AsyncToSyncBody(_OkRequest())

        async def run():
            body.start()
            return await asyncio.to_thread(lambda: list(body))

        assert asyncio.run(run()) == [b"aa", b"bb", b"cc"]


class TestEndToEnd:
    def test_upstream_receives_before_client_finishes(self, gateway_app, backend):
        """**全程流式**:上游开始收包时,客户端远没发完。

        这是流式的真正目的——不是"上游能流式解析 JSON",而是让大包被拆成连续
        片段边收边转发,避免一次性冲一个巨大数据包。

        判别方法:客户端分多片发送(每片间隔 20ms),若网关先
        ``await request.body()`` 收齐再转发,上游的首片到达时刻会≈发完时刻;
        若边收边转发,则在只发出几片时上游就开始收到数据。
        """
        rng = random.Random(8)
        msgs = [{"role": "system", "content": _sent(rng, 200)}]
        for i in range(60):
            msgs.append({"role": "user", "content": f"q{i}:" + _sent(rng, 40)})
            msgs.append({"role": "assistant", "content": f"a{i}:" + _sent(rng, 40)})
        payload = {"model": "deepseek-chat", "messages": msgs}
        prepared = gw.TailClient(block_size=256).prepare(payload)
        body = b"".join(prepared.body)
        assert prepared.have_len == 0

        pieces = [body[i:i + 1024] for i in range(0, len(body), 1024)]
        assert len(pieces) >= 8, "请求体要够大才能观察分段效果"

        sent_at: list[float] = []
        gap = 0.01

        async def gen():
            for i, piece in enumerate(pieces):
                sent_at.append(time.monotonic())
                yield piece
                if i < len(pieces) - 1:
                    await asyncio.sleep(gap)

        async def run():
            async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=gateway_app),
                    timeout=30) as client:
                return await client.post(
                    "http://tail.test/v1/chat/completions",
                    content=gen(),
                    headers={"content-type": gw.CONTENT_TYPE_BLOCKS,
                             gw.HEADER_SESSION: prepared.session.hex()})

        r = asyncio.run(run())
        assert r.status_code == 200
        assert backend.state.received[-1]["messages"] == msgs

        arrived = backend.state.first_chunk_at
        done = backend.state.body_done_at
        assert arrived is not None and done is not None

        # 上游收到首片时,客户端才发出很少几片
        sent_before = sum(1 for t in sent_at if t <= arrived)
        assert sent_before < len(pieces) * 0.5, (
            f"上游首片到达时客户端已发出 {sent_before}/{len(pieces)} 片——"
            f"说明网关先把整个 body 收完才转发,不是流式")

        # 首片到收完的时间应接近「剩余片数 × 间隔」,而不是≈0
        assert (done - arrived) > gap * len(pieces) * 0.3

    def test_all_messages_survive_streaming(self, gateway_app, backend):
        """流式重建的 JSON 里一条 message 都不能少。"""
        rng = random.Random(4)
        msgs = [{"role": "system", "content": _sent(rng, 100)}]
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            for turn in range(1, 16):
                msgs.append({"role": "user", "content": f"问{turn}:" + _sent(rng, 40)})
                r, _ = _send(tc, client, {"model": "x", "messages": msgs})
                assert r.status_code == 200
                assert backend.state.received[-1]["messages"] == msgs
                msgs.append({"role": "assistant",
                             "content": f"答{turn}:" + _sent(rng, 40)})

    def test_json_passthrough_still_works(self, gateway_app, backend):
        payload = {"model": "x", "messages": [{"role": "user", "content": "你好"}]}
        with TestClient(gateway_app) as tc:
            r = tc.post("/v1/chat/completions", json=payload)
        assert r.status_code == 200
        assert backend.state.received[-1]["messages"] == payload["messages"]

    def test_multi_turn_volume_does_not_grow_linearly(self, gateway_app):
        rng = random.Random(7)
        msgs = [{"role": "system", "content": _sent(rng, 300)}]
        client = gw.TailClient()
        sizes, ctx = [], []
        with TestClient(gateway_app) as tc:
            for turn in range(1, 21):
                msgs.append({"role": "user", "content": f"追问{turn}:" + _sent(rng, 50)})
                r, sent = _send(tc, client, {"model": "x", "messages": msgs})
                assert r.status_code == 200
                sizes.append(sent)
                ctx.append(sum(len(m["content"]) for m in msgs))
                msgs.append({"role": "assistant",
                             "content": f"回答{turn}:" + _sent(rng, 50)})
        recent = sum(sizes[10:]) / len(sizes[10:])
        assert recent < ctx[-1] * 0.4, (
            f"后半段平均线上 {recent:.0f}B vs 上下文 {ctx[-1]}B,去重没生效")

    def test_healthz(self, gateway_app):
        with TestClient(gateway_app) as tc:
            r = tc.get("/healthz")
        assert r.status_code == 200 and r.json()["ok"] is True

    def test_missing_blocks_signals_retransmit(self, gateway_app):
        rng = random.Random(5)
        payload = {"model": "x", "messages": [
            {"role": "system", "content": _sent(rng, 100)},
            {"role": "user", "content": "问题"},
        ]}
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            assert _send(tc, client, payload)[0].status_code == 200
            gateway_app.state.storage.clear()
            r, _ = _send(tc, client, payload)
        assert r.status_code == 409
        assert r.headers[gw.HEADER_RETRANSMIT] == "1"

    def test_mid_block_miss_returns_409_without_inference(self, gateway_app, backend):
        """块洞落在正文中间:必须干净地返回 409 + 重发信号,而不是抛异常。

        以前的行为是「跳过缺块继续解」,洞落在 UTF-8 多字节字符中间时后续片段
        首字节非法,解码抛 UnicodeDecodeError;洞落在 message 边界时则拼出一条
        语义已经变了的合法 prompt,白跑一次上游推理。
        """
        rng = random.Random(7)
        # 中文正文每字 3 字节,512B 块边界必然切开多字节字符 → 洞会制造非法 UTF-8
        payload = {"model": "x", "messages": [
            {"role": "user", "content": _sent(rng, 400)},
            {"role": "assistant", "content": _sent(rng, 400)},
            {"role": "user", "content": "继续"},
        ]}
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            assert _send(tc, client, payload)[0].status_code == 200
            before = len(backend.state.received)
            ids = list(gateway_app.state.storage._data)
            assert len(ids) >= 3, "需要足够多的块才能挖中间洞"
            gateway_app.state.storage._data.pop(ids[1])     # 挖掉中间一块
            r, _ = _send(tc, client, payload)
            assert r.status_code == 409
            assert r.headers.get(gw.HEADER_RETRANSMIT) == "1"
            assert r.headers.get(gw.HEADER_STATUS) == gw.CACHE_DEGRADED
            assert len(backend.state.received) == before, "缺块时不该有上游推理"
            # 客户端按重发信号重来一次:恢复成正确请求
            client.invalidate()
            r2, _ = _send(tc, client, payload)
            assert r2.status_code == 200
            assert backend.state.received[-1]["messages"] == payload["messages"]

    def test_client_retransmit_recovers(self, gateway_app, backend):
        rng = random.Random(6)
        payload = {"model": "x", "messages": [
            {"role": "system", "content": _sent(rng, 100)},
            {"role": "user", "content": "问题"},
        ]}
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            assert _send(tc, client, payload)[0].status_code == 200
            gateway_app.state.storage.clear()
            r1, _ = _send(tc, client, payload)
            assert r1.headers.get(gw.HEADER_RETRANSMIT) == "1"
            client.invalidate()          # 客户端按响应头重发全文
            r2, _ = _send(tc, client, payload)
            assert r2.status_code == 200
            assert backend.state.received[-1]["messages"] == payload["messages"]

    def test_hit_ratio_header(self, gateway_app):
        rng = random.Random(8)
        payload = {"model": "x", "messages": [
            {"role": "system", "content": _sent(rng, 100)},
            {"role": "user", "content": "问题"},
        ]}
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            _, cold = _send(tc, client, payload)          # 第一轮建立缓存
            r, warm = _send(tc, client, payload)
        # 只有整块能进缓存:重复请求只需重发不足一块的尾巴
        assert warm < cold / 2
        assert warm < 300, f"重复请求还发了 {warm}B"
        assert float(r.headers[gw.HEADER_HIT]) > 0.6
