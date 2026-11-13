"""网关:接收帧流,还原成文本,反渲染成 JSON,转发给上游。

数据流::

    POST /v1/chat/completions
      Content-Type: application/x-tail-blocks
      ↓ 帧流(增量喂入)
      解码器(Decoder)  ── 引用帧查存储,新块帧直传 + 写存储
      ↓ 一维文本流(边收边拼)
      反渲染器(Unrenderer)  ── 文本 → messages JSON
      ↓ JSON 请求体
      上游推理 API
      ↓ 响应(SSE 或 JSON,原样透传回客户端)

路由与响应
----------
对客户端而言,**路径、请求头、响应格式与直连上游完全一致**,只是请求体换了
编码。客户端不感知网关存在。

两种模式:
  - ``application/x-tail-blocks``:帧流,走去重
  - ``application/json``:普通 JSON,原样转发(便于灰度与调试)

降级
----
任何一块取不到(存储被清、服务重启、LRU 淘汰),解码器记录到 ``missing``。
此时**不是返回错误**,而是把这次请求当直传处理:向上游重新要一次全文。
实际做法是——客户端收到 ``X-Tail-Retransmit: 1`` 响应头后,下一轮清空已知块
集合重发。这样缓存故障对用户完全不可见。
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, Optional

from . import codec, render
from .codec import BlockSource, Decoder
from .storage import make_storage

#: 帧流的 MIME
CONTENT_TYPE_BLOCKS = "application/x-tail-blocks"
CONTENT_TYPE_JSON = "application/json"

#: 协议版本
TAIL_VERSION = "1"

HEADER_VERSION = "X-Tail-Version"
HEADER_VERSIONED = HEADER_VERSION
HEADER_RETRANSMIT = "X-Tail-Retransmit"
HEADER_BLOCKS = "X-Tail-Blocks"
HEADER_HIT = "X-Tail-Hit"
HEADER_CACHE = "X-Tail-Cache"

CACHE_HIT = "hit"
CACHE_MISS = "miss"
CACHE_DEGRADED = "degraded"


class TailGateway:
    """无框架依赖的网关核心。

    与 FastAPI / Starlette / ASGI 无关,便于测试与嵌入。上层只需要:
      1. 判断请求是否走帧流(:meth:`wants_blocks`)
      2. 把 body 增量喂给 :meth:`decode_stream`
      3. 拿还原出的 JSON 去请求上游
    """

    def __init__(self, storage: BlockSource, *,
                 strip_unknown: bool = True) -> None:
        """
        Args:
          storage: 块存储
          strip_unknown: 反渲染时是否丢弃上游不认识的字段(如 ``stream_options``
                         在某些旧版本上游不支持)
        """
        self.storage = storage
        self._strip_unknown = strip_unknown

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

    def decode_stream(self, body_chunks) -> "DecodeResult":
        """把帧流的字节片段逐段解码。

        返回对象既是迭代器(产出文本片段),也带统计信息。用法::

            for piece in gw.decode_stream(chunks):
                ...
        """
        return DecodeResult(self, body_chunks)


class DecodeResult:
    """流式解码结果。迭代产出文本片段,迭代结束后可读统计。"""

    def __init__(self, gateway: TailGateway, body_chunks) -> None:
        self._gateway = gateway
        self._chunks = body_chunks
        self._decoder = gateway.make_decoder()
        self.text_bytes = 0
        self.new_block_count = 0
        self.ref_block_count = 0
        self.missing: list = []
        self._consumed = False
        self._frame_kinds: list = []

    def __iter__(self) -> "AsyncIterator[bytes] | Any":
        return self._iterate()

    def _iterate(self):
        if self._consumed:
            return
        self._consumed = True
        for chunk in self._chunks:
            for piece in self._decoder.feed(chunk):
                self.text_bytes += len(piece)
                yield piece
        self._decoder.finish()
        self.new_block_count = len(self._decoder.new_blocks)
        self.ref_block_count = self._decoder.ref_count
        self.missing = list(self._decoder.missing)

    @property
    def ok(self) -> bool:
        return self._decoder.done and not self.missing

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
        if self.new_block_count == 0:
            return CACHE_HIT
        return CACHE_HIT  # 部分命中仍记 hit,细节看 hit_ratio


class TailClient:
    """客户端侧:把 JSON 请求体编码成帧流。

    与 HTTP 库无关。上层把 :meth:`encode_request` 的返回值直接当 body 发出,
    并把 ``Content-Type`` 设为 ``application/x-tail-blocks``。
    """

    def __init__(self) -> None:
        self._encoder = codec.Encoder()

    def encode_request(self, payload: Dict[str, Any]) -> bytes:
        """请求体 JSON(dict) → 帧流字节。"""
        text = render.render_request(payload)
        return self._encoder.encode(text)

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
    pieces = list(gw.decode_stream([wire]))
    text = b"".join(pieces)
    return render.unrender_request(text)
