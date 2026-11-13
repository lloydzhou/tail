"""端到端测试:客户端 → 网关(ASGI)→ mock 上游后端。

验证 issue 里「网关侧把文本流反渲染成 JSON 后交给上游」这条链路真的通,
以及上游收到的 messages 与客户端发出的一致。
"""

from __future__ import annotations

import json
import random

import httpx
import pytest
from fastapi.testclient import TestClient

from tail import serve
from tail.transport import gateway as gw
from tail.transport import render, storage

from .mock_backend import build_backend_app

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 "
          "缓存 传输 协议 分块 网关 客户端").split()


def _sent(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(_WORDS) + ("，" if rng.random() < 0.3 else "")
                   for _ in range(n))


@pytest.fixture
def backend():
    app = build_backend_app()
    return app


@pytest.fixture
def gateway_app(backend):
    st = storage.MemoryStorage()
    app = serve.build_app("http://mock", st)
    app.state.client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=backend), timeout=30)
    return app


class TestEndToEnd:
    def test_blocks_request_reaches_upstream(self, gateway_app, backend):
        rng = random.Random(3)
        payload = {"model": "deepseek-chat", "messages": [
            {"role": "system", "content": _sent(rng, 100)},
            {"role": "user", "content": "请分析数据"},
        ]}
        client = gw.TailClient()
        body = client.encode_request(payload)

        with TestClient(gateway_app) as tc:
            r = tc.post("/v1/chat/completions", content=body,
                        headers={"content-type": gw.CONTENT_TYPE_BLOCKS})

        assert r.status_code == 200
        assert r.headers["x-tail-version"] == gw.TAIL_VERSION
        # 后端应收到还原后的 messages
        received = backend.state.received
        assert len(received) == 1
        assert received[0]["messages"] == payload["messages"]
        assert received[0]["model"] == payload["model"]

    def test_json_passthrough_still_works(self, gateway_app, backend):
        payload = {"model": "x", "messages": [{"role": "user", "content": "你好"}]}
        with TestClient(gateway_app) as tc:
            r = tc.post("/v1/chat/completions", json=payload)
        assert r.status_code == 200
        assert backend.state.received[0]["messages"] == payload["messages"]

    def test_multi_turn_volume_does_not_grow_linearly(self, gateway_app):
        """线上体积不应随上下文规模线性增长。

        注意:体积会呈**锯齿状**(尾块被不断追加的内容吞掉,直到某个轮次
        再次命中切点)。所以这里不断言"逐轮单调下降",而是断言:
          - 后半段的体积明显小于「全量重传」的量级
          - 上下文翻倍时,线上体积的增长远慢于线性
        """
        rng = random.Random(7)
        msgs = [{"role": "system", "content": _sent(rng, 300)}]
        client = gw.TailClient()
        sizes, ctx = [], []
        with TestClient(gateway_app) as tc:
            for turn in range(1, 21):
                msgs.append({"role": "user", "content": f"追问{turn}:" + _sent(rng, 50)})
                body = client.encode_request({"model": "x", "messages": msgs})
                r = tc.post("/v1/chat/completions", content=body,
                            headers={"content-type": gw.CONTENT_TYPE_BLOCKS})
                assert r.status_code == 200
                sizes.append(len(body))
                ctx.append(len(render.render_request({"model": "x", "messages": msgs})))
                msgs.append({"role": "assistant", "content": f"回答{turn}:" + _sent(rng, 50)})

        # 上下文从第 1 轮到第 20 轮翻了好几倍,线上体积的均值应远低于上下文规模
        recent = sum(sizes[10:]) / len(sizes[10:])
        assert recent < ctx[-1] * 0.5, (
            f"后半段平均线上 {recent:.0f}B vs 上下文 {ctx[-1]}B,去重没生效")
        # 首轮(冷启动)最大,之后都应比它小
        assert max(sizes[1:]) <= sizes[0] * 1.2, f"出现比冷启动更大的请求: {sizes}"

    def test_healthz(self, gateway_app):
        with TestClient(gateway_app) as tc:
            r = tc.get("/healthz")
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_missing_blocks_returns_retransmit_signal(self, gateway_app):
        rng = random.Random(5)
        payload = {"model": "x", "messages": [
            {"role": "system", "content": _sent(rng, 100)},
            {"role": "user", "content": "问题"},
        ]}
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            # 第一轮建立缓存
            client.encode_request(payload)
            # 清空存储后发第二轮(全是引用帧)
            gateway_app.state.storage.clear()
            body = client.encode_request(payload)
            r = tc.post("/v1/chat/completions", content=body,
                        headers={"content-type": gw.CONTENT_TYPE_BLOCKS})

        assert r.status_code == 409
        assert r.headers[gw.HEADER_RETRANSMIT] == "1"

    def test_client_retransmit_recovers(self, gateway_app):
        rng = random.Random(6)
        payload = {"model": "x", "messages": [
            {"role": "system", "content": _sent(rng, 100)},
            {"role": "user", "content": "问题"},
        ]}
        client = gw.TailClient()
        with TestClient(gateway_app) as tc:
            client.encode_request(payload)
            gateway_app.state.storage.clear()
            r1 = tc.post("/v1/chat/completions",
                         content=client.encode_request(payload),
                         headers={"content-type": gw.CONTENT_TYPE_BLOCKS})
            assert r1.status_code == 409
            # 客户端按 409 响应头重发全文
            client.invalidate()
            r2 = tc.post("/v1/chat/completions",
                         content=client.encode_request(payload),
                         headers={"content-type": gw.CONTENT_TYPE_BLOCKS})
            assert r2.status_code == 200
