"""网关核心:接收帧流,流式还原成 JSON 片段,转发给上游。

数据流::

    POST /v1/chat/completions
      Content-Type: application/x-tail-v2
      ↓ 帧流片段(增量喂入)
      解码器(Decoder)  ── 引用帧查存储,新块帧直传 + 写存储
      ↓ 渲染文本片段(边收边解)
      流式反渲染器(RenderParser)  ── 文本片段 → 完整 message
      ↓ 逐条 message 序列化成 JSON 片段
      上游推理 API(请求体边生成边发)

**整条链路是流式的**,没有任何一处需要先把完整请求拼出来。网关的峰值内存
只与「单个 message」的大小成正比,与请求体总大小无关。

与客户端的关系
--------------
对客户端而言,路径、请求头、响应格式与直连上游完全一致,只是请求体换了编码。

两种模式:
  - ``application/x-tail-v2``:帧流,走去重
  - ``application/json``:普通 JSON,原样转发(便于灰度与调试)

降级
----
任何一块取不到(存储被清、服务重启、LRU 淘汰),解码器记录到 ``missing``。
此时不是返回错误,而是让客户端重发全文:响应头带 ``X-Tail-Retransmit: 1``
(状态码仍是上游的状态码),客户端清空已知块集合后重试。缓存故障对用户不可见。
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, Iterable, Iterator, List, Optional, Tuple

from . import chunking, codec, render
from .codec import BlockSource, Decoder, MissingBlock

#: 帧流的 MIME
CONTENT_TYPE_BLOCKS = "application/x-tail-v2"
CONTENT_TYPE_JSON = "application/json"

#: 协议版本
TAIL_VERSION = "2"

HEADER_VERSION = "X-Tail-Version"
HEADER_RETRANSMIT = "X-Tail-Retransmit"
HEADER_STATUS = "X-Tail-Status"
HEADER_HIT = "X-Tail-Hit"

CACHE_HIT = "hit"
CACHE_MISS = "miss"
CACHE_DEGRADED = "degraded"


def _json_message(msg: Dict[str, Any]) -> str:
    """把一条 message 序列化成 JSON 片段(不含逗号与包裹括号)。"""
    return json.dumps(msg, ensure_ascii=False, separators=(",", ":"))


class TailGateway:
    """无框架依赖的网关核心。

    与 FastAPI / Starlette / ASGI 无关,便于测试与嵌入。上层只需要:
      1. 判断请求是否走帧流(:meth:`wants_blocks`)
      2. 把 body 片段喂给 :meth:`iter_payload_json`
      3. 把产出的 JSON 片段边生成边发给上游
    """

    def __init__(self, storage: BlockSource) -> None:
        self.storage = storage

    # -- 请求判定 ---------------------------------------------------------

    @staticmethod
    def wants_blocks(content_type: Optional[str]) -> bool:
        return bool(content_type) and CONTENT_TYPE_BLOCKS in content_type.lower()

    # -- 解码 -------------------------------------------------------------

    def make_decoder(self) -> Decoder:
        """构造一个带「新块自动入库」回调的解码器。"""
        return Decoder(
            self.storage,
            on_new_block=lambda bid, content: self.storage.put_many({bid: content}),
        )

    def iter_payload_json(self, body_chunks: Iterable[bytes]) -> "DecodeSession":
        """流式把帧流转成上游需要的 JSON 请求体片段。

        返回一个可迭代对象,每次产出 JSON 字节片段。迭代结束后可读统计信息。

        用法::

            session = gw.iter_payload_json(body_chunks)
            for json_piece in session:
                send_to_upstream(json_piece)
            if session.missing:
                retransmit()
        """
        return DecodeSession(self, body_chunks)

    def decode_stream(self, body_chunks: Iterable[bytes]) -> "DecodeSession":
        """:meth:`iter_payload_json` 的别名,语义上强调"解码"。"""
        return DecodeSession(self, body_chunks)

    def collect_payload(self, body_chunks: Iterable[bytes]) -> Tuple[Dict[str, Any], "DecodeSession"]:
        """一次性拿到完整 payload(测试用)。

        有缺块(降级)时返回空 dict——调用方应据此要求客户端重发全文,
        而不是把空的 messages 数组转发给上游。
        """
        session = DecodeSession(self, body_chunks)
        buf = b"".join(session)
        if session.missing or not buf:
            return {}, session
        return json.loads(buf), session


class DecodeSession:
    """一次请求的解码会话:产出的既是 JSON 片段流,也带统计信息。

    产出的字节拼起来就是完整的 OpenAI 请求体 JSON。
    """

    def __init__(self, gateway: TailGateway, body_chunks: Iterable[bytes]) -> None:
        self._gateway = gateway
        self._chunks = body_chunks
        self._decoder = gateway.make_decoder()
        self._parser = render.RenderParser()

        # 统计
        self.text_bytes = 0
        self.new_block_count = 0
        self.ref_block_count = 0
        self.message_count = 0
        self.missing: List[bytes] = []
        self.params: Dict[str, Any] = {}
        self._consumed = False
        self._error: Optional[str] = None

    # -- 迭代 -------------------------------------------------------------

    def __iter__(self) -> Iterator[bytes]:
        return self._iterate()

    def _iterate(self) -> Iterator[bytes]:
        if self._consumed:
            return
        self._consumed = True

        # 已经开始输出正文的标记。JSON 的第一个字段必须是 "messages",
        # 但渲染文本把顶层参数放在 messages 之前,所以要缓冲参数、
        # 先把 messages 数组开出来,参数在数组结束后补上。
        # 这不会撑爆内存:参数只有固定十几个。
        opened = False

        def emit_head() -> Iterator[bytes]:
            nonlocal opened
            if opened:
                return
            opened = True
            yield b'{"messages":['

        first_msg = [True]

        try:
            for chunk in self._chunks:
                for piece in self._decoder.feed(chunk):
                    self.text_bytes += len(piece)
                    for event in self._parser.feed(piece):
                        yield from self._handle_event(event, emit_head, first_msg)
            self._decoder.finish()
            for event in self._parser.finish():
                yield from self._handle_event(event, emit_head, first_msg)
        except MissingBlock as exc:
            # 缺块:**先把统计落到 session 上**再抛。否则调用方拿到的是空
            # missing、拿不到命中率,响应里也就没法告诉客户端「该重发了」。
            self._snapshot()
            self.missing = list(self._decoder.missing)
            self._error = f"missing block {exc.block_hash.hex()[:16]}"
            raise
        except ValueError as exc:
            # 渲染文本不合法(不含缺块)。同样先落统计。
            self._snapshot()
            self._error = str(exc)
            raise

        # 结束 messages 数组,补上顶层参数
        yield from emit_head()
        yield b"]"
        for key, value in self.params.items():
            yield ("," + json.dumps(key, ensure_ascii=False) + ":"
                   + json.dumps(value, ensure_ascii=False,
                                separators=(",", ":"))).encode("utf-8")
        yield b"}"

        self._snapshot()
        self.missing = list(self._decoder.missing)

    def _snapshot(self) -> None:
        """把解码器当前的统计搬到 session 上(正常结束与异常退出都走这里)。"""
        self.new_block_count = len(self._decoder.new_blocks)
        self.ref_block_count = self._decoder.ref_count

    def _handle_event(self, event, emit_head, first_msg) -> Iterator[bytes]:
        kind, value = event
        if kind == render.EventKind.PARAM:
            name, param_value = value
            self.params[name] = param_value
            return
        if kind != render.EventKind.MESSAGE_END:
            return
        # 一条 message 完成:立刻序列化并发出,不累积
        msg = value
        self.message_count += 1
        yield from emit_head()
        if not first_msg[0]:
            yield b","
        first_msg[0] = False
        yield _json_message(msg).encode("utf-8")

    # -- 统计 -------------------------------------------------------------

    @property
    def ok(self) -> bool:
        return not self.missing and self._error is None

    @property
    def total_blocks(self) -> int:
        return self.new_block_count + self.ref_block_count

    @property
    def hit_ratio(self) -> float:
        """引用块占比。0 = 全冷启动,1 = 全部命中。"""
        total = self.total_blocks
        return (self.ref_block_count / total) if total else 0.0

    @property
    def status(self) -> str:
        """粗略状态,仅供日志/监控。真实指标看 :attr:`hit_ratio`。"""
        if self.missing:
            return CACHE_DEGRADED
        if self.ref_block_count == 0:
            return CACHE_MISS
        return CACHE_HIT


class TailClient:
    """客户端侧:把 JSON 请求体流式编码成帧流。

    与 HTTP 库无关。上层把 :meth:`iter_encode_request` 产出的片段直接当 body
    流式发出,并把 ``Content-Type`` 设为 ``application/x-tail-v2``。
    """

    def __init__(self, block_size: int = chunking.BLOCK_SIZE) -> None:
        self._encoder = codec.Encoder(block_size=block_size)

    def iter_encode_request(self, payload: Dict[str, Any]) -> Iterator[bytes]:
        """请求体 dict → 帧流片段(流式)。"""
        return self._encoder.iter_encode(render.iter_render(payload))

    def encode_request(self, payload: Dict[str, Any]) -> bytes:
        """一次性版本。测试与短请求用。"""
        return b"".join(self.iter_encode_request(payload))

    def encode_text(self, text: bytes) -> bytes:
        return self._encoder.encode(text)

    def invalidate(self) -> None:
        """清空已知块集合。

        收到 ``X-Tail-Retransmit: 1`` 时调用:下一轮把全文当新块重发,
        网关会重新填充存储。
        """
        self._encoder.clear()

    @property
    def known_blocks(self) -> int:
        return self._encoder.known_count


# ---------------------------------------------------------------------------
# 便捷:一次性往返(测试 / 单机嵌入)
# ---------------------------------------------------------------------------

def roundtrip(payload: Dict[str, Any], storage: BlockSource,
              client: Optional[TailClient] = None) -> Dict[str, Any]:
    """完整走一遍客户端编码 → 网关解码 → 反渲染,返回还原的请求体。

    用于测试与「不部署网关也能验证」的场景。
    """
    client = client or TailClient()
    wire = client.encode_request(payload)
    gw = TailGateway(storage)
    back, _ = gw.collect_payload([wire])
    return back
