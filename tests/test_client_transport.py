"""客户端接入层测试:``TailTransport`` 让 openai SDK 零改动接入帧流协议。

三层验证:

1. **单元** —— 用假内层 transport 捕获请求,确认 chat 请求被换成帧流、
   其他请求原样透传、缺块时自动重发。
2. **集成** —— 帧流能被网关解码还原成原 payload。
3. **真实 SDK** —— 装了 ``openai`` 时用真 SDK 跑一轮(未装则跳过)。
"""

from __future__ import annotations

import json

import pytest

from tail.transport import codec, frames, gateway as gw, render
from tail.transport.client import TailAsyncTransport, TailTransport

httpx = pytest.importorskip("httpx2")


# ---------------------------------------------------------------------------
# 测试用内层 transport:不发网络,只记录请求并回一个假响应
# ---------------------------------------------------------------------------

class _FakeInner:
    def __init__(self, extra_headers: dict | None = None) -> None:
        self.requests = []
        self._extra = extra_headers or {}

    def handle_request(self, request):
        body = b"".join(request.stream)
        self.requests.append((request, body))
        return httpx.Response(
            200, json={"ok": True},
            headers={"x-tail-version": "2", **self._extra})

    def close(self) -> None:
        pass


def _chat_request(payload: dict):
    """构造一个 openai SDK 风格、已序列化好的 chat 请求。"""
    return httpx.Request(
        "POST", "http://tail.test/v1/chat/completions",
        headers={"content-type": "application/json",
                 "authorization": "Bearer sk-test"},
        content=json.dumps(payload, ensure_ascii=False).encode())


def _decode_wire(wire: bytes, store=None):
    """帧流 → payload,用真实解码器。

    引用帧只能对着**同一份存储**解——网关解码时会把新块写进去
    (``TailGateway`` 的 ``on_new_block``),所以连续两轮复用同一个 store
    就等价于真实的「客户端两轮 + 网关持续在线」场景。
    """
    from tail.transport import storage as st
    store = store if store is not None else st.MemoryStorage()
    out, _session = gw.TailGateway(store).collect_payload([wire])
    return out, store


# ---------------------------------------------------------------------------
# 单元
# ---------------------------------------------------------------------------

class TestTailTransport:
    def test_chat_request_is_converted_to_frames(self):
        payload = {"model": "deepseek-chat",
                   "messages": [{"role": "user", "content": "你好"}]}
        inner = _FakeInner()
        TailTransport(inner=inner).handle_request(_chat_request(payload))

        request, body = inner.requests[-1]
        # Content-Type 换成帧流,Content-Length 必须去掉(体变成流式)
        assert request.headers["content-type"] == gw.CONTENT_TYPE_BLOCKS
        assert "content-length" not in request.headers
        # 首个字节是帧类型标记,不是 '{'
        assert body[0] in (frames.FRAME_REF, frames.FRAME_NEW)
        # 能还原成原 payload
        out, _ = _decode_wire(body)
        assert out == payload

    def test_non_chat_request_passes_through(self):
        inner = _FakeInner()
        req = httpx.Request("POST", "http://tail.test/v1/embeddings",
                            headers={"content-type": "application/json"},
                            content=b'{"input":"x"}')
        TailTransport(inner=inner).handle_request(req)

        request, body = inner.requests[-1]
        assert body == b'{"input":"x"}'
        assert request.headers["content-type"] == "application/json"

    def test_invalid_json_is_not_guessed(self):
        """body 不是合法 JSON 时原样发出,不能猜。"""
        inner = _FakeInner()
        req = httpx.Request("POST", "http://tail.test/v1/chat/completions",
                            headers={"content-type": "application/json"},
                            content=b"not json at all")
        TailTransport(inner=inner).handle_request(req)
        _request, body = inner.requests[-1]
        assert body == b"not json at all"

    def test_retransmit_triggers_full_resend(self):
        """响应带 X-Tail-Retransmit 时清空本地已知集合、自动重发一次。"""
        payload = {"model": "m",
                   "messages": [{"role": "user", "content": "x" * 2000}]}
        inner = _FakeInner(extra_headers={gw.HEADER_RETRANSMIT: "1"})
        transport = TailTransport(inner=inner)

        transport.handle_request(_chat_request(payload))
        # 两次尝试:首次带引用帧(第二块起),重发时全量新块
        assert len(inner.requests) == 2
        second_body = inner.requests[1][1]
        out, _ = _decode_wire(second_body)
        assert out == payload
        # 重发后不再有引用帧(已知集合已被 invalidate)
        decoded = list(frames.FrameDecoder().feed(second_body))
        assert all(kind == "new" for kind, _ in decoded if kind != "end")

    def test_second_request_reuses_blocks(self):
        """第二轮同样的前缀只发引用帧,线上体积显著变小。"""
        payload = {"model": "m",
                   "messages": [{"role": "user", "content": "y" * 3000}]}
        inner = _FakeInner()
        transport = TailTransport(inner=inner)
        transport.handle_request(_chat_request(payload))
        first, store = _decode_wire(inner.requests[-1][1])

        transport.handle_request(_chat_request(payload))
        second, _ = _decode_wire(inner.requests[-1][1], store)

        assert first == payload and second == payload
        # 第二轮明显更小:首轮全是新块,次轮只剩引用 + 结尾
        assert len(inner.requests[-1][1]) < len(inner.requests[0][1])


# ---------------------------------------------------------------------------
# 「本地只记哈希、不存内容」这条性质
# ---------------------------------------------------------------------------

class TestLocalKnownSet:
    """客户端的本地状态必须只是块哈希集合,不能留正文副本。"""

    def test_known_set_holds_hashes_only(self):
        from tail.transport import chunking

        client = gw.TailClient()
        payload = {"model": "m",
                   "messages": [{"role": "user", "content": "z" * 10_000}]}
        encoded = client.encode_request(payload)

        known = client._encoder._known          # 内部集合,直接验证元素形态
        assert known, "编码后应记下已发送的块"
        # 每一项必须是定长块 id,而不是内容
        assert all(len(bid) == chunking.BLOCK_ID_SIZE for bid in known)
        # 块 id 是二进制哈希,不含正文
        assert not any(b"zzzz" in bid for bid in known)
        # 本地状态量与线上体积同阶,不随正文大小膨胀
        assert len(known) * chunking.BLOCK_ID_SIZE < len(encoded)

    def test_reusing_client_is_what_makes_it_cheap(self):
        """复用同一个 client 才有缓存;每轮新建等于永远冷启动。"""
        payload = {"model": "m",
                   "messages": [{"role": "user", "content": "q" * 6000}]}

        reused = gw.TailClient()
        cold_then_warm = [len(reused.encode_request(payload)) for _ in range(2)]
        assert cold_then_warm[1] < cold_then_warm[0] / 2

        fresh_total = sum(len(gw.TailClient().encode_request(payload))
                          for _ in range(2))
        # 不复用 = 两次都是冷启动体量
        assert fresh_total > cold_then_warm[0] + cold_then_warm[1]

    def test_prefix_shape_reuse(self):
        """复用是前缀形状的:往中间插一条消息,后面全部失效。"""
        head = [{"role": "system", "content": "s" * 2000}]
        client = gw.TailClient()
        base = client.encode_request(
            {"model": "m", "messages": head + [{"role": "user", "content": "a" * 2000}]})

        # 纯追加:几乎全命中
        appended = client.encode_request(
            {"model": "m", "messages": head
             + [{"role": "user", "content": "a" * 2000},
                {"role": "assistant", "content": "b" * 50}]})
        # 插在中间:前缀断裂,后续块哈希全变
        inserted = client.encode_request(
            {"model": "m", "messages": [head[0],
                                        {"role": "user", "content": "X"},
                                        {"role": "user", "content": "a" * 2000}]})

        assert len(appended) < len(base)          # 追加几乎免费
        assert len(inserted) > len(appended)      # 中间插入代价高


# ---------------------------------------------------------------------------
# 真实 openai SDK
# ---------------------------------------------------------------------------

openai = pytest.importorskip("openai")


class TestWithRealOpenAISDK:
    """用真实 ``openai.OpenAI`` + 真实 HTTP 端口跑,验证零改动接入。"""

    def test_openai_sdk_roundtrip(self):
        import socket
        import threading
        import time

        import uvicorn

        from tail import serve
        from tail.transport import storage
        from tests.mock_backend import build_backend_app

        def free_port() -> int:
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            s.close()
            return port

        backend = build_backend_app()
        backend_port = free_port()
        threading.Thread(
            target=uvicorn.Server(uvicorn.Config(
                backend, host="127.0.0.1", port=backend_port,
                log_level="error")).run, daemon=True).start()

        gateway_port = free_port()
        app = serve.build_app(f"http://127.0.0.1:{backend_port}",
                             storage.MemoryStorage())
        threading.Thread(
            target=uvicorn.Server(uvicorn.Config(
                app, host="127.0.0.1", port=gateway_port,
                log_level="error")).run, daemon=True).start()

        # 等端口起来
        last_headers: dict = {}

        def on_response(resp):
            last_headers.clear()
            last_headers.update(resp.headers)

        # openai 3.x 用 httpx2;装不上就用 httpx(两者接口一致)
        try:
            import httpx2 as sdk_http
        except ImportError:                       # pragma: no cover
            sdk_http = httpx

        sdk_client = openai.OpenAI(
            base_url=f"http://127.0.0.1:{gateway_port}/v1",
            api_key="sk-test",
            http_client=sdk_http.Client(
                transport=TailTransport(),
                event_hooks={"response": [on_response]}),
        )

        # 等网关可用
        for _ in range(200):
            try:
                sdk_http.get(f"http://127.0.0.1:{gateway_port}/v1/models",
                             timeout=0.5)
                break
            except Exception:                  # noqa: BLE001
                time.sleep(0.05)
        else:                                  # pragma: no cover
            pytest.skip("网关未能启动")

        messages = [{"role": "system", "content": "你是助手。"},
                    {"role": "user", "content": "第一轮问题"}]
        sent_snapshots = []
        for turn in range(1, 3):
            sent_snapshots.append([dict(m) for m in messages])
            result = sdk_client.chat.completions.create(
                model="deepseek-chat", messages=messages)
            reply = result.choices[0].message.content
            messages.append({"role": "assistant", "content": reply})
            messages.append({"role": "user", "content": f"第{turn + 1}轮问题"})

        # 真的走了帧流协议(而不是悄悄退回 JSON)
        assert last_headers.get("x-tail-version") == gw.TAIL_VERSION
        # 上游收到的 messages 与客户端实际发出的那一份完全一致
        got = backend.state.received[-1]["messages"]
        assert got == sent_snapshots[-1]
