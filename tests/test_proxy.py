"""本地客户端代理的端到端测试:magpie → 代理 → 网关 → 后端。

链路与真机部署同构(只是把网络换成 ASGI):

    magpie(JSON) → tail.proxy(客户端协议) → tail.serve(网关协议) → 推理服务

要证明的四件事:
  1. magpie 完全不用改:发出去的是标准 JSON,收到的也是标准响应;
  2. 第二轮起线上只有尾巴字节,首轮全量;
  3. ``X-Tail-*`` 是两侧之间的内部头,不漏给 magpie;
  4. 前缀在服务端丢了(清缓存/TTL 过期)时,代理自己重发一次,对 magpie 仍是一次成功调用。
"""

from __future__ import annotations

import json
import random
from types import SimpleNamespace

import httpx2 as httpx
import pytest
from fastapi.testclient import TestClient

from tail import proxy, serve
from tail.transport import storage

from .mock_backend import build_backend_app

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 "
          "缓存 传输 协议 分块 网关 客户端").split()


def _text(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(_WORDS) + ("，" if rng.random() < 0.3 else "")
                   for _ in range(n))


_TOOLS = [{
    "type": "function",
    "function": {
        "name": "read_file",
        "description": "读取一个文件",
        "parameters": {"type": "object",
                       "properties": {"path": {"type": "string"}}},
    },
}]


def _payload(turns: int, stream: bool = False, rng=None) -> dict:
    """一个 agent 形状的请求:长 system + tools + 逐轮增长的历史。"""
    rng = rng or random.Random(11)
    messages = [{"role": "system", "content": "你是助手。" + _text(rng, 60)}]
    for i in range(turns):
        messages.append({"role": "user", "content": f"q{i}:" + _text(rng, 30)})
        messages.append({"role": "assistant",
                         "content": f"a{i}:" + _text(rng, 30)})
    return {"model": "deepseek-chat", "messages": messages, "stream": stream,
            "tools": _TOOLS, "temperature": 0.7}


@pytest.fixture
def chain():
    """magpie(TestClient) → 代理 → 网关 → mock 后端。"""
    backend = build_backend_app()
    gw_app = serve.build_app("http://mock", storage.MemoryChainStore())
    gw_app.state.client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=backend), timeout=30)

    proxy_app = proxy.build_app("http://gateway")
    proxy_app.state.client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gw_app), timeout=30)

    tc = TestClient(proxy_app)
    return SimpleNamespace(tc=tc, backend=backend, gw=gw_app,
                           app=proxy_app, stats=proxy_app.state.stats)


def _chat(tc, payload):
    return tc.post("/v1/chat/completions", json=payload)


class TestRoundTrip:
    def test_backend_receives_identical_payload(self, chain):
        """magpie 发什么,推理服务就该收到什么(含 tools/参数)。"""
        payload = _payload(6)
        r = _chat(chain.tc, payload)
        assert r.status_code == 200
        sent = chain.backend.state.received[-1]
        assert sent["messages"] == payload["messages"]
        assert sent["model"] == payload["model"]
        assert r.json()["object"] == "chat.completion"

    def test_second_round_sends_only_the_tail(self, chain):
        """第二轮起,线上只剩新增的尾部字节。"""
        rng = random.Random(7)
        payload = _payload(20, rng=rng)
        assert _chat(chain.tc, payload).status_code == 200

        before = chain.stats.as_dict()
        payload["messages"] += [
            {"role": "user", "content": "继续"},
            {"role": "assistant", "content": "好的。"},
            {"role": "user", "content": "再继续"},
        ]
        r = _chat(chain.tc, payload)
        assert r.status_code == 200
        after = chain.stats.as_dict()

        wire = after["wire_bytes_out"] - before["wire_bytes_out"]
        json_in = after["json_bytes_in"] - before["json_bytes_in"]
        assert after["hits"] == 1 and after["misses"] == 1
        assert after["prefix_bytes_reused"] > 0
        # 只有尾巴上线:留 8% 余量给分块对齐
        assert wire < json_in * 0.08, (wire, json_in)

    def test_identical_request_second_time_is_almost_free(self, chain):
        """同一请求再发一次:整个上下文都在服务端,线上只够一个凭据。"""
        payload = _payload(10, rng=random.Random(3))
        assert _chat(chain.tc, payload).status_code == 200
        before = chain.stats.as_dict()
        assert _chat(chain.tc, payload).status_code == 200
        after = chain.stats.as_dict()
        wire = after["wire_bytes_out"] - before["wire_bytes_out"]
        # 只剩「一个不满的块 + 固定头开销」(满块才进缓存)
        assert wire < 608, wire
        assert chain.backend.state.received[-1]["messages"] == payload["messages"]


class TestResponseRelay:
    def test_stream_is_relayed(self, chain):
        payload = _payload(3, stream=True, rng=random.Random(5))
        with chain.tc.stream("POST", "/v1/chat/completions", json=payload) as r:
            assert r.status_code == 200
            assert "event-stream" in r.headers["content-type"]
            body = b"".join(r.iter_bytes()).decode()
        assert "data: [DONE]" in body
        assert "chat.completion.chunk" in body

    def test_tail_headers_do_not_leak(self, chain):
        """X-Tail-* 是两侧协议内部头,magpie 不该看到。"""
        payload = _payload(4, rng=random.Random(9))
        _chat(chain.tc, payload)
        payload["messages"].append({"role": "user", "content": "再问一次"})
        r = _chat(chain.tc, payload)
        assert r.status_code == 200
        assert not [k for k in r.headers if k.lower().startswith("x-tail-")]


class TestRetransmit:
    def test_lost_prefix_is_retried_transparently(self, chain):
        """服务端清缓存(或 TTL 到期)后,代理丢凭据重发,magpie 仍只看到一次成功。"""
        payload = _payload(8, rng=random.Random(13))
        assert _chat(chain.tc, payload).status_code == 200

        chain.gw.state.storage.clear()
        payload["messages"].append({"role": "user", "content": "清完缓存再问"})
        r = _chat(chain.tc, payload)

        assert r.status_code == 200
        assert chain.stats.retransmits == 1
        assert chain.backend.state.received[-1]["messages"] == payload["messages"]

    def test_credential_table_recovers_after_retransmit(self, chain):
        """重发之后凭据重新建立,下一轮又只发尾巴。"""
        payload = _payload(6, rng=random.Random(17))
        assert _chat(chain.tc, payload).status_code == 200
        chain.gw.state.storage.clear()
        payload["messages"].append({"role": "user", "content": "x"})
        assert _chat(chain.tc, payload).status_code == 200

        before = chain.stats.as_dict()
        payload["messages"].append({"role": "user", "content": "y"})
        assert _chat(chain.tc, payload).status_code == 200
        after = chain.stats.as_dict()
        wire = after["wire_bytes_out"] - before["wire_bytes_out"]
        json_in = after["json_bytes_in"] - before["json_bytes_in"]
        assert wire < 768, (wire, json_in)
        assert json_in > 3000
        assert chain.stats.retransmits == 1


class TestModes:
    def test_native_mode_forwards_json_untouched(self):
        """A/B 基线:同一个代理换成纯转发,字节数就是 JSON 本身。"""
        backend = build_backend_app()
        app = proxy.build_app("http://backend", native=True)
        app.state.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=backend), timeout=30)
        tc = TestClient(app)

        payload = _payload(5, rng=random.Random(21))
        raw = json.dumps(payload).encode()
        r = tc.post("/v1/chat/completions", content=raw,
                    headers={"content-type": "application/json"})
        assert r.status_code == 200
        stats = app.state.stats.as_dict()
        assert stats["native_requests"] == 1
        assert stats["wire_bytes_out"] == len(raw)
        assert backend.state.received[-1]["messages"] == payload["messages"]

    def test_non_chat_path_is_passed_through(self, chain):
        r = chain.tc.get("/v1/models")
        assert chain.stats.passthrough == 1
        # 后端没有这个路由(网关只接受 POST chat completions)→ 原样回 4xx,
        # 至少证明请求确实被透传上去了
        assert r.status_code in (404, 405)

    def test_invalid_json_is_not_guessed(self, chain):
        r = chain.tc.post("/v1/chat/completions", content=b"not json",
                          headers={"content-type": "application/json"})
        assert r.status_code == 200
        assert chain.stats.passthrough == 1
        assert chain.stats.tail_requests == 0


class TestStats:
    def test_stats_endpoint_reports_wire_saving(self, chain):
        payload = _payload(12, rng=random.Random(23))
        for _ in range(4):
            assert _chat(chain.tc, payload).status_code == 200
            payload["messages"].append({"role": "user", "content": "再来"})

        stats = chain.tc.get("/__tail/stats").json()
        assert stats["requests"] == 4
        assert stats["tail_requests"] == 4
        assert stats["misses"] == 1 and stats["hits"] == 3
        # 4 轮里只有第 1 轮是全量,累计比值应该在 3 倍上下
        assert stats["wire_bytes_out"] < stats["json_bytes_in"]
        assert stats["wire_saving_ratio"] > 2.5

    def test_healthz(self, chain):
        assert chain.tc.get("/healthz").json()["ok"] is True
