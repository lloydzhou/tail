"""线缆帧格式:引用帧 / 新块帧 / 结束帧。

格式(二进制,按原始顺序):

    0x01 <16 字节 block_id>          引用:这段内容网关已有
    0x02 <4 字节长度(大端)> <原始字节>  新块:直传
    0x03                             结束

为什么用二进制而不是 JSON:
  - 无二次转义。JSON 里新块内容要转义引号和反斜杠,体积与 CPU 翻倍
  - 可流式解析。读 1 字节识别类型、读 4 字节得到长度,不必等整个 body
  - 引用帧固定 17 字节,1000 块仅 17KB
"""

from __future__ import annotations

import struct
from typing import Iterator, List, Optional, Tuple, Union

from .chunking import BLOCK_ID_SIZE

FRAME_REF = 0x01
FRAME_NEW = 0x02
FRAME_END = 0x03

_REF_HEADER = struct.Struct(">B")            # 1 字节类型
_REF_BODY = struct.Struct(f">{BLOCK_ID_SIZE}s")
_NEW_HEADER = struct.Struct(">BI")           # 类型 + 4 字节长度
_END = bytes([FRAME_END])

#: 一个帧:("ref", block_id) 或 ("new", content)
Frame = Tuple[str, Union[bytes, bytes]]


# ---------------------------------------------------------------------------
# 编码
# ---------------------------------------------------------------------------

def encode_ref(block_id: bytes) -> bytes:
    if len(block_id) != BLOCK_ID_SIZE:
        raise ValueError(f"block_id 必须是 {BLOCK_ID_SIZE} 字节,得到 {len(block_id)}")
    return _REF_HEADER.pack(FRAME_REF) + _REF_BODY.pack(block_id)


def encode_new(content: bytes) -> bytes:
    return _NEW_HEADER.pack(FRAME_NEW, len(content)) + content


def encode_end() -> bytes:
    return _END


def encode_frames(frames: List[Frame]) -> bytes:
    """把帧列表编码成字节流(测试与短请求用)。"""
    out = bytearray()
    for kind, payload in frames:
        if kind == "ref":
            out += encode_ref(payload)
        elif kind == "new":
            out += encode_new(payload)
        else:
            raise ValueError(f"未知帧类型: {kind}")
    out += _END
    return bytes(out)


# ---------------------------------------------------------------------------
# 流式解码
# ---------------------------------------------------------------------------

class FrameDecoder:
    """增量式帧解码器:喂多少解多少,不要求完整 body。

    用法::

        dec = FrameDecoder()
        for chunk in body_chunks:      # 任意切分的字节片段
            for frame in dec.feed(chunk):
                ...
            if dec.done:
                break
    """

    __slots__ = ("_buf", "done")

    def __init__(self) -> None:
        self._buf = bytearray()
        self.done = False

    def feed(self, data: bytes) -> Iterator[Frame]:
        """喂入字节,产出已完整的帧。未完整的部分留到下次。"""
        if self.done:
            return
        self._buf += data
        while self._buf:
            kind = self._buf[0]
            if kind == FRAME_REF:
                need = _REF_HEADER.size + _REF_BODY.size
                if len(self._buf) < need:
                    return
                (bid,) = _REF_BODY.unpack(bytes(self._buf[1:need]))
                del self._buf[:need]
                yield ("ref", bid)
            elif kind == FRAME_NEW:
                if len(self._buf) < _NEW_HEADER.size:
                    return
                _, length = _NEW_HEADER.unpack(bytes(self._buf[:_NEW_HEADER.size]))
                need = _NEW_HEADER.size + length
                if len(self._buf) < need:
                    return
                content = bytes(self._buf[_NEW_HEADER.size:need])
                del self._buf[:need]
                yield ("new", content)
            elif kind == FRAME_END:
                del self._buf[:1]
                self.done = True
                return
            else:
                raise ValueError(f"非法帧类型: 0x{kind:02x}")

    @property
    def eof_ok(self) -> bool:
        """是否收到结束帧且无残留。"""
        return self.done and not self._buf


def decode_frames(data: bytes) -> List[Frame]:
    """一次性解码(测试用)。"""
    dec = FrameDecoder()
    frames = list(dec.feed(data))
    if not dec.eof_ok:
        raise ValueError("帧流不完整或缺少结束帧")
    return frames
