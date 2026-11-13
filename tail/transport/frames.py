"""线缆帧格式:引用帧 / 新块帧 / 结束帧。

格式(二进制,按到达顺序):

    0x01 <block_hash>                 引用:这段内容网关已有
    0x02 <block_hash> <4字节长度> <块字节>  新块:直传内容
    0x03                              结束

为什么新块帧也带 block_hash
---------------------------
块哈希是 vLLM 式的**哈希链**:``hash(父哈希 + 本块)``。解码端收到新块时
并不知道它的父哈希(前一个块可能是引用帧、可能被跳过),因此无法自己算出
正确哈希。哈希由**发送方**算好随块一起发,解码端直接采用——
这与 vLLM 里 block_hash 由调度侧计算、随 KVCacheBlock 一起流转是同一个道理。

为什么用二进制而不是 JSON
-------------------------
  - 无二次转义。JSON 里块内容要转义引号和反斜杠,体积与 CPU 翻倍。
  - 可流式解析。读 1 字节类型 + 定长头部,不必等整个 body。
  - 引用帧固定 ``1 + BLOCK_ID_SIZE`` 字节,1000 块仅 17KB。
"""

from __future__ import annotations

import struct
from typing import Iterator, List, Tuple, Union

from .chunking import BLOCK_ID_SIZE

FRAME_REF = 0x01
FRAME_NEW = 0x02
FRAME_END = 0x03

_TYPE = struct.Struct(">B")
_REF_BODY = struct.Struct(f">{BLOCK_ID_SIZE}s")
_NEW_HEADER = struct.Struct(f">{BLOCK_ID_SIZE}sI")   # 哈希 + 长度
_END = _TYPE.pack(FRAME_END)

_REF_SIZE = _TYPE.size + _REF_BODY.size
_NEW_HEADER_SIZE = _TYPE.size + _NEW_HEADER.size

#: 一个帧:("ref", block_hash) 或 ("new", (block_hash, content))
Frame = Tuple[str, Union[bytes, Tuple[bytes, bytes]]]


# ---------------------------------------------------------------------------
# 编码
# ---------------------------------------------------------------------------

def encode_ref(block_hash: bytes) -> bytes:
    """引用帧:定长,不拷贝内容。"""
    if len(block_hash) != BLOCK_ID_SIZE:
        raise ValueError(
            f"block_hash 必须是 {BLOCK_ID_SIZE} 字节,得到 {len(block_hash)}")
    return _TYPE.pack(FRAME_REF) + _REF_BODY.pack(block_hash)


def encode_new(block_hash: bytes, content: bytes) -> bytes:
    """新块帧:头部 + 内容。

    **为什么每块要多花 16 字节带上 ``block_hash``,而不让解码端自己算?**

    因为解码端算不出来。哈希链是 ``hash(父哈希 + 本块)``,而解码端看到的是
    **帧序列**:其中的引用帧不带内容、只带哈希。要独立重算某个新块的哈希,
    就必须知道它前一块的真实哈希——那个块可能正是被跳过(引用)的。

    唯一的替代是让引用帧也带父哈希,那引用帧会从 17 B 涨到 33 B,比新块帧
    多带的 16 B 更亏(稳态下引用帧远多于新块帧)。

    所以:发送方算哈希并随帧传来,是这块唯一划算的方案。
    详见 docs/TRANSPORT-PIPELINE.md §5.2。
    """
    if len(block_hash) != BLOCK_ID_SIZE:
        raise ValueError(
            f"block_hash 必须是 {BLOCK_ID_SIZE} 字节,得到 {len(block_hash)}")
    return _TYPE.pack(FRAME_NEW) + _NEW_HEADER.pack(block_hash, len(content)) + content


def encode_end() -> bytes:
    return _END


def encode_frames(items: List[Frame]) -> bytes:
    """把帧列表编码成字节流(测试与短请求用)。"""
    out = bytearray()
    for kind, payload in items:
        if kind == "ref":
            out += encode_ref(payload)
        elif kind == "new":
            block_hash, content = payload
            out += encode_new(block_hash, content)
        else:
            raise ValueError(f"未知帧类型: {kind}")
    out += _END
    return bytes(out)


# ---------------------------------------------------------------------------
# 流式解码
# ---------------------------------------------------------------------------

class FrameDecoder:
    """增量式帧解码器:喂多少解多少,不要求完整 body。

    **零拷贝读取**:块内容直接从缓冲切片得到,不做中间累积拷贝;
    缓冲用 ``del`` 前缀的方式推进,避免每帧整段复制。

    用法::

        dec = FrameDecoder()
        for piece in body_chunks:       # 任意切分的字节片段
            for frame in dec.feed(piece):
                ...
    """

    __slots__ = ("_buf", "_pos", "done")

    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0            # 已消费到 _buf 的哪个下标(惰性裁剪)
        self.done = False

    def feed(self, data: bytes) -> Iterator[Frame]:
        """喂入字节,产出已完整的帧。未完整的部分留到下次。"""
        if self.done:
            return
        if data:
            # 先裁掉已消费的前缀再追加,避免缓冲无限增长
            if self._pos:
                del self._buf[:self._pos]
                self._pos = 0
            self._buf += data

        buf = self._buf
        pos = self._pos
        n = len(buf)
        try:
            while pos < n:
                kind = buf[pos]
                if kind == FRAME_REF:
                    if n - pos < _REF_SIZE:
                        break
                    (bh,) = _REF_BODY.unpack_from(buf, pos + _TYPE.size)
                    pos += _REF_SIZE
                    yield ("ref", bh)
                elif kind == FRAME_NEW:
                    if n - pos < _NEW_HEADER_SIZE:
                        break
                    bh, length = _NEW_HEADER.unpack_from(buf, pos + _TYPE.size)
                    end = pos + _NEW_HEADER_SIZE + length
                    if n < end:
                        break
                    # 切片即内容;bytearray 切片本身就是新 bytes,无需再转
                    content = bytes(buf[pos + _NEW_HEADER_SIZE:end])
                    pos = end
                    yield ("new", (bh, content))
                elif kind == FRAME_END:
                    pos += _TYPE.size
                    self.done = True
                    return
                else:
                    raise ValueError(f"非法帧类型: 0x{kind:02x}")
        finally:
            self._pos = pos

    @property
    def eof_ok(self) -> bool:
        """是否收到结束帧且无残留。"""
        return self.done and self._pos >= len(self._buf)


def decode_frames(data: bytes) -> List[Frame]:
    """一次性解码(测试用)。"""
    dec = FrameDecoder()
    items = list(dec.feed(data))
    if not dec.eof_ok:
        raise ValueError("帧流不完整或缺少结束帧")
    return items
