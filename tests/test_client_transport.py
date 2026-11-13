"""客户端接入层测试:``TailTransport`` 让 openai SDK 零改动接入 v3 线缆。

三层验证:

1. **单元** —— 用假内层 transport(背后是真网关 + 真存储)捕获请求,确认
   chat 请求被换成「请求头 + 尾部字节」、其他请求原样透传、前缀失效时自动重发。
2. **集成** —— 尾部字节经网关还原成原 payload。
3. **真实 SDK** —— 装了 ``openai`` 时用真 SDK 跑一轮(未装则跳过)。
"""

from __future__ import annotations

import json

import pytest

from tail.transport import codec, gateway as gw, storage
from tail.transport.client import TailAsyncTransport, TailTransport

httpx = pytest.importorskip("httpx2")


# ---------------------------------------------------------------------------
# 假内层 transport:背后是**真网关 + 真存储**,所以响应头里的凭据是真的
# ---------------------------------------------------------------------------

class _FakeInner:
    def __init__(self, store=None) -> None:
        self.store = store if store is not None else storage.MemoryChainStore()
        self.requests = []
        self.responses = []

    def handle_request(self, request):
        body = b"".join(request.stream)
        self.requests.append((request, body))
        if request.headers.get("content-type") != gw.CONTENT_TYPE_BLOCKS:
            # 透传路径:不做任何解析
            return httpx.Response(200, json={"ok": True, "passthrough": True})
        have_len, have_tip = gw.decode_have(request.headers.get(gw.HEADER_HAVE))
        gateway = gw.TailGateway(self.store)
        try:
            session = gateway.iter_payload_json(iter([body]), have_len, have_tip)
            payload = json.loads(b"".join(session))
            headers = {
                gw.HEADER_VERSION: gw.TAIL_VERSION,
                gw.HEADER_STATUS: session.status,
                gw.HEADER_HIT: f"{session.hit_ratio:.3f}",
                gw.HEADER_HAVE: session.have_header(),
            }
            status = 200
            content = {"id": "chatcmpl-test", "object": "chat.completion",
                       "created": 0, "model": payload["model"],
                       "choices": [{"index": 0, "finish_reason": "stop",
                                    "message": {"role": "assistant",
                                                "content": "ok"}}]}
        except codec.MissingPrefix:
            headers = {gw.HEADER_VERSION: gw.TAIL_VERSION,
                       gw.HEADER_RETRANSMIT: "1"}
            status, content = 409, {"error": "retransmit"}
        response = httpx.Response(status, json=content, headers=headers)
        self.responses.append(response)
        return response

    def close(self) -> None:
        pass


def _chat_request(payload: dict):
    """构造一个 openai SDK 风格、已序列化好的 chat 请求。"""
    return httpx.Request(
        "POST", "http://tail.test/v1/chat/completions",
        headers={"content-type": "application/json",
                 "authorization": "Bearer sk-test"},
        content=json.dumps(payload, ensure_ascii=False).encode())


def _payload(turns: int = 6) -> dict:
    msgs = [{"role": "system", "content": "你是严谨的助手。" * 60}]
    for i in range(turns):
        msgs.append({"role": "user", "content": f"第{i}问：" + "描述上下文" * 60})
        msgs.append({"role": "assistant", "content": f"第{i}答：" + "给出结论" * 80})
    return {"model": "deepseek-chat", "temperature": 0.7, "messages": msgs}


# ---------------------------------------------------------------------------
# 单元
# ---------------------------------------------------------------------------

class TestTailTransport:

    def test_chat_request_is_converted_to_v3_wire(self):
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        response = transport.handle_request(_chat_request(_payload()))

        assert response.status_code == 200
        request, body = inner.requests[-1]
        assert request.headers["content-type"] == gw.CONTENT_TYPE_BLOCKS
        assert len(request.headers[gw.HEADER_SESSION]) == 32
        assert gw.HEADER_HAVE not in request.headers      # 冷启动没有凭据
        assert body.startswith(b"<|model|>")             # 发的是渲染文本,不是 JSON
        assert response.headers[gw.HEADER_HAVE]

    def test_non_chat_request_passes_through(self):
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        request = httpx.Request("POST", "http://tail.test/v1/embeddings",
                                json={"input": "x"})
        transport.handle_request(request)
        _, body = inner.requests[-1]
        assert json.loads(body) == {"input": "x"}

    def test_invalid_json_is_not_guessed(self):
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        request = httpx.Request(
            "POST", "http://tail.test/v1/chat/completions",
            headers={"content-type": "application/json"}, content=b"{not json")
        transport.handle_request(request)
        _, body = inner.requests[-1]
        assert body == b"{not json"

    def test_second_request_reuses_prefix(self):
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        payload = _payload()
        transport.handle_request(_chat_request(payload))
        transport.handle_request(_chat_request(payload))

        (r1, b1), (r2, b2) = inner.requests[-2], inner.requests[-1]
        assert gw.HEADER_HAVE not in r1.headers
        assert r2.headers[gw.HEADER_HAVE]                # 第二轮带上凭据
        assert len(b2) < len(b1) // 5, (len(b1), len(b2))

    def test_retransmit_triggers_full_resend(self):
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        payload = _payload()
        transport.handle_request(_chat_request(payload))     # 建立凭据
        inner.store.clear()                                  # 网关存储被清

        response = transport.handle_request(_chat_request(payload))
        assert response.status_code == 200                   # 上层只看到成功
        first, second = inner.requests[-2], inner.requests[-1]
        assert gw.HEADER_HAVE in first[0].headers            # 第一次带旧凭据
        assert gw.HEADER_HAVE not in second[0].headers       # 重发时凭据已丢
        assert transport.client.sessions == 1                # 重建了凭据

    def test_transport_keeps_no_body_copy(self):
        """客户端不留正文副本:本地状态只有每会话的 (L, tip)。"""
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        payload = _payload(turns=8)
        transport.handle_request(_chat_request(payload))
        transport.handle_request(_chat_request(payload))

        client = transport.client
        assert client.sessions == 1
        # 8+16 = 24 字节/会话;整份上下文有几十 KB,差三个数量级
        assert client.credential_bytes <= 24 * 4


class TestAsyncTransport:

    def test_async_roundtrip(self):
        import asyncio

        class _AsyncInner(_FakeInner):
            async def handle_async_request(self, request):
                return self.handle_request(request)

            async def aclose(self):
                pass

        inner = _AsyncInner()
        transport = TailAsyncTransport(inner=inner)
        payload = _payload(turns=3)

        async def run():
            request = _chat_request(payload)
            r1 = await transport.handle_async_request(request)
            r2 = await transport.handle_async_request(_chat_request(payload))
            return r1, r2

        r1, r2 = asyncio.run(run())
        assert r1.status_code == 200 and r2.status_code == 200
        assert len(inner.requests) == 2


# ---------------------------------------------------------------------------
# 真实 openai SDK
# ---------------------------------------------------------------------------

class TestWithRealOpenAISDK:

    def test_openai_sdk_roundtrip(self):
        openai = pytest.importorskip("openai")
        sdk_client = openai.OpenAI(
            api_key="sk-test", base_url="http://tail.test/v1",
            http_client=httpx.Client(transport=TailTransport(inner=_FakeInner())))
        payload = _payload(turns=2)
        completion = sdk_client.chat.completions.create(
            model=payload["model"], messages=payload["messages"])
        assert completion.choices[0].message.content == "ok"
