"""网关侧与客户端的协议层:头、状态、会话。

v3 线缆
-------
请求::

    POST /v1/chat/completions
    Content-Type: application/x-tail-v3
    X-Tail-Session: <32 hex>        # 会话键 = 渲染文本首块的哈希
    X-Tail-Have: <L>;<32 hex>       # 可选:客户端认为网关已持有的前缀
    body: 渲染文本第 L 字节之后的原始字节

响应::

    X-Tail-Version: 3
    X-Tail-Have: <L'>;<32 hex>      # 下一轮的凭据,服务端永远是权威
    X-Tail-Status: hit|miss|degraded
    X-Tail-Hit: 0.97
    (前缀失效时 409 + X-Tail-Retransmit: 1)

与 v2 的差别:没有帧、没有逐块引用、客户端不发任何哈希。
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

from . import chunking, render
from .codec import MissingPrefix, PrefixClient, PrefixReader, PreparedRequest

#: 请求体的 Content-Type
CONTENT_TYPE_BLOCKS = "application/x-tail-v3"

#: 线缆版本(与 v2 不兼容)
TAIL_VERSION = "3"

HEADER_VERSION = "X-Tail-Version"
HEADER_STATUS = "X-Tail-Status"
HEADER_HIT = "X-Tail-Hit"
HEADER_RETRANSMIT = "X-Tail-Retransmit"
HEADER_SESSION = "X-Tail-Session"
HEADER_HAVE = "X-Tail-Have"

CACHE_HIT = "hit"
CACHE_MISS = "miss"
CACHE_DEGRADED = "degraded"


# ---------------------------------------------------------------------------
# 头的编解码
# ---------------------------------------------------------------------------

def encode_have(length: int, tip: bytes) -> str:
    """``X-Tail-Have`` 的值:``<字节数>;<链尾 hex>``。"""
    if length <= 0 or not tip:
        return ""
    return f"{length};{tip.hex()}"


def decode_have(value: Optional[str]) -> Tuple[int, bytes]:
    """解析 ``X-Tail-Have``。非法/缺失一律当作「从 0 开始」。"""
    if not value:
        return 0, b""
    try:
        length_s, tip_s = value.split(";", 1)
        length = int(length_s)
        tip = bytes.fromhex(tip_s)
    except (ValueError, AttributeError):
        return 0, b""
    if length <= 0 or len(tip) != chunking.BLOCK_ID_SIZE:
        return 0, b""
    return length, tip


def decode_session(value: Optional[str]) -> bytes:
    if not value:
        return b""
    try:
        raw = bytes.fromhex(value)
    except ValueError:
        return b""
    return raw if len(raw) == chunking.BLOCK_ID_SIZE else b""


# ---------------------------------------------------------------------------
# 客户端
# ---------------------------------------------------------------------------

class TailClient:
    """客户端协议层:与 HTTP 库无关。

    上层拿到 :meth:`prepare` 的结果,把 ``have`` / ``session`` 写进头、
    把 ``body`` 当请求体流式发出即可。
    """

    def __init__(self, block_size: int = chunking.BLOCK_SIZE,
                 max_credentials: int = 4) -> None:
        self._encoder = PrefixClient(block_size=block_size,
                                     max_credentials=max_credentials)

    # -- 编码 -------------------------------------------------------------

    def prepare(self, payload: Dict[str, Any]) -> PreparedRequest:
        return self._encoder.prepare(payload)

    def encode_request(self, payload: Dict[str, Any]) -> bytes:
        """一次性版本(测试用)。"""
        return b"".join(self.prepare(payload).body)

    # -- 凭据 -------------------------------------------------------------

    def set_credential(self, session: bytes, length: int, tip: bytes) -> None:
        self._encoder.set_credential(session, length, tip)

    def update_from_headers(self, session: bytes, headers) -> None:
        length, tip = decode_have(headers.get(HEADER_HAVE))
        if length:
            self._encoder.set_credential(session, length, tip)

    def invalidate(self, session: Optional[bytes] = None) -> None:
        self._encoder.invalidate(session)

    @property
    def sessions(self) -> int:
        return self._encoder.sessions

    @property
    def credential_bytes(self) -> int:
        return self._encoder.credential_bytes


# ---------------------------------------------------------------------------
# 网关
# ---------------------------------------------------------------------------

class TailGateway:
    """网关协议层:把 (前缀凭据, 尾部字节) 还原成标准 OpenAI JSON 流。"""

    def __init__(self, store) -> None:
        self.store = store

    @staticmethod
    def wants_blocks(content_type: Optional[str]) -> bool:
        return CONTENT_TYPE_BLOCKS in (content_type or "")

    def make_reader(self, have_len: int = 0, have_tip: bytes = b"") -> PrefixReader:
        return PrefixReader(self.store, have_len=have_len, have_tip=have_tip)

    # -- 流式解码 ---------------------------------------------------------

    def iter_payload_json(self, body_chunks: Iterable[bytes],
                          have_len: int = 0,
                          have_tip: bytes = b"") -> "DecodeSession":
        """请求体字节流 → OpenAI 请求体 JSON 片段流(全程流式)。"""
        return DecodeSession(self, body_chunks, have_len, have_tip)

    def collect_payload(self, body_chunks: Iterable[bytes],
                        have_len: int = 0,
                        have_tip: bytes = b"") -> Tuple[Dict[str, Any], "DecodeSession"]:
        """一次性版本(测试用)。"""
        session = DecodeSession(self, body_chunks, have_len, have_tip)
        raw = b"".join(session)
        return json.loads(raw), session


class DecodeSession:
    """一次请求的解码会话:产出的既是 JSON 片段流,也带统计信息。"""

    def __init__(self, gateway: TailGateway, body_chunks: Iterable[bytes],
                 have_len: int = 0, have_tip: bytes = b"") -> None:
        self._gateway = gateway
        self._chunks = body_chunks
        self._reader = gateway.make_reader(have_len, have_tip)
        self._parser = render.RenderParser()

        self.text_bytes = 0
        self.message_count = 0
        self.params: Dict[str, Any] = {}
        self._consumed = False
        self._error: Optional[str] = None

    # -- 统计 -------------------------------------------------------------

    @property
    def prefix_len(self) -> int:
        """本轮结束时网关持有的前缀字节数(只算满块)。"""
        return self._reader.total_blocks * self._reader._bs

    @property
    def tip(self) -> bytes:
        return self._reader.tip

    @property
    def hit_ratio(self) -> float:
        return self._reader.hit_ratio

    @property
    def new_blocks(self) -> int:
        return self._reader.new_blocks

    @property
    def prefix_blocks(self) -> int:
        return self._reader.prefix_blocks

    @property
    def ok(self) -> bool:
        return self._error is None

    @property
    def status(self) -> str:
        if self._error is not None:
            return CACHE_DEGRADED
        if self._reader.prefix_blocks == 0:
            return CACHE_MISS
        return CACHE_HIT

    def have_header(self) -> str:
        return encode_have(self.prefix_len, self.tip)

    # -- 迭代 -------------------------------------------------------------

    def __iter__(self) -> Iterator[bytes]:
        return self._iterate()

    def _iterate(self) -> Iterator[bytes]:
        if self._consumed:
            return
        self._consumed = True

        opened = False

        def emit_head() -> Iterator[bytes]:
            nonlocal opened
            if opened:
                return
            opened = True
            yield b'{"messages":['

        first_msg = [True]
        try:
            for piece in self._reader.iter_text(self._chunks):
                self.text_bytes += len(piece)
                for event in self._parser.feed(piece):
                    yield from self._handle_event(event, emit_head, first_msg)
            for event in self._parser.finish():
                yield from self._handle_event(event, emit_head, first_msg)
        except (ValueError, MissingPrefix) as exc:
            self._error = str(exc)
            raise

        yield from emit_head()
        yield b"]"
        for key, value in self.params.items():
            yield ("," + json.dumps(key, ensure_ascii=False) + ":"
                   + json.dumps(value, ensure_ascii=False,
                                separators=(",", ":"))).encode("utf-8")
        yield b"}"

    def _handle_event(self, event, emit_head, first_msg) -> Iterator[bytes]:
        kind, value = event
        if kind == render.EventKind.PARAM:
            name, param_value = value
            self.params[name] = param_value
            return
        if kind != render.EventKind.MESSAGE_END:
            return
        msg = value
        self.message_count += 1
        yield from emit_head()
        if not first_msg[0]:
            yield b","
        first_msg[0] = False
        yield _json_message(msg).encode("utf-8")


def _json_message(msg: Dict[str, Any]) -> str:
    return json.dumps(msg, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


# ---------------------------------------------------------------------------
# 便捷:一次性往返(测试 / 单机嵌入)
# ---------------------------------------------------------------------------

def roundtrip(payload: Dict[str, Any], store,
              client: Optional[TailClient] = None) -> Dict[str, Any]:
    """完整走一遍客户端编码 → 网关解码 → 反渲染,返回还原的请求体。"""
    client = client or TailClient()
    prepared = client.prepare(payload)
    gw = TailGateway(store)
    back, session = gw.collect_payload(prepared.body,
                                       prepared.have_len, prepared.have_tip)
    client.set_credential(prepared.session, session.prefix_len, session.tip)
    return back
