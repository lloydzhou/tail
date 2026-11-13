"""分块与块哈希,按 vLLM Automatic Prefix Caching 的实现。

参考实现
--------
vLLM:https://docs.vllm.ai/en/v0.8.3/_sources/design/v1/prefix_caching.md

文档原文的图示与规则::

       Block 1: |<--- block tokens ---->|
       Block 2: |<------- prefix ------>| |<--- block tokens --->|
       Block 3: |<------------------ prefix -------------------->| |<--- block tokens ---->|

  1. **定长块(block_size)**,块边界只由位置决定,与内容无关。
  2. **块哈希 = hash(父块哈希 + 本块 tokens + 额外哈希)**::

         block_hash = hash(parent_hash, block_tokens)

     父哈希让相同内容挂在不同前缀下得到不同 id,这就是"前缀"语义。
  3. **只缓存满块**:"We only cache full blocks"。

DeepSeek 侧对应的是"按固定 token 间隔落盘"(上下文硬盘缓存文档),
同样是固定 token 间隔,不是按内容找切点。

本层工作在**字节**上:网关这一侧拿不到 tokenizer,所以块大小按字节定义,
而不是 vLLM 的 16 token(中文约 48 字节)。粒度依据见 ``BLOCK_SIZE`` 与
``docs/TRANSPORT-PIPELINE.md`` §4.3。
"""

from __future__ import annotations

import hashlib
from typing import Iterator, List, Tuple

#: 每块的字节数。对应 vLLM 的 ``block_size``(默认 16 tokens)。
#:
#: 为什么不照抄 16 token(中文约 48 字节):v3 里每读一次前缀都要从链尾一跳一跳
#: 走到链头,**走链跳数 = 上下文长度 / 块大小**。829 KB 按 512 B 切是 1,774 跳,
#: 按 48 B 切就是 17,706 跳(存储读成本差 10 倍),换来的只是「每轮少重发
#: 400 字节的尾巴」。粒度要按走链成本定,不是按 token 定。
#:
#: 必须全局固定,不能按请求体大小自适应:同一个前缀在 1 KB 请求和 1 MB 请求里
#: 必须切出**相同**的块边界,否则跨轮复用一个块都命中不了。
#:
#: 详见 ``docs/TRANSPORT-PIPELINE.md`` §4.3 的「跳数 / 尾块」对照表。
BLOCK_SIZE = 512

#: 块哈希字节数。vLLM 用整型 hash;Borg 要求 id 必须是密码学强哈希,
#: 这里取 16 字节(128 bit),碰撞概率可忽略。
BLOCK_ID_SIZE = 16

#: 摘要算法。Borg 明确要求 id_hash 为密码学强哈希;blake2b 比 sha256 快,
#: 且不像内置 ``hash()`` 那样带进程随机盐(缓存要跨进程稳定)。
_HASH_NAME = "blake2b"


def block_hash(block: bytes, parent_hash: bytes = b"") -> bytes:
    """块哈希 = hash(父哈希 + 本块),与 vLLM 一致。

    Args:
      block: 本块内容。
      parent_hash: 前一个满块的哈希;第一块为 ``b""``(vLLM 的 ``None``)。
    """
    h = hashlib.new(_HASH_NAME, digest_size=BLOCK_ID_SIZE)
    h.update(parent_hash)
    h.update(block)
    return h.digest()


def block_hash_hex(block: bytes, parent_hash: bytes = b"") -> str:
    """十六进制形式,便于日志与调试。"""
    return block_hash(block, parent_hash).hex()


def split(data: bytes, block_size: int = BLOCK_SIZE) -> List[Tuple[int, int]]:
    """按定长切块,返回 ``[(start, end), ...]``。最后一块可能不满。"""
    if not data:
        return []
    n = len(data)
    return [(i, min(i + block_size, n)) for i in range(0, n, block_size)]


def split_blocks(data: bytes,
                 block_size: int = BLOCK_SIZE) -> Iterator[bytes]:
    """产出块字节(切片,不拷贝内容)。"""
    for s, e in split(data, block_size):
        yield data[s:e]


def blocked_hashes(data: bytes,
                   block_size: int = BLOCK_SIZE) -> List[Tuple[bytes, bool]]:
    """切块并算哈希,返回 ``[(block_hash, is_full), ...]``。

    哈希链只串满块——与原实现一致,不满的尾块不进入链,
    因此追加内容不会改变已满块及其后续的哈希。
    """
    out: List[Tuple[bytes, bool]] = []
    parent = b""
    for s, e in split(data, block_size):
        chunk = data[s:e]
        full = (e - s) == block_size
        bid = block_hash(chunk, parent)
        out.append((bid, full))
        if full:
            parent = bid
    return out


def blocked_to_blocks(data: bytes,
                      block_size: int = BLOCK_SIZE) -> List[Tuple[bytes, bytes]]:
    """切块并算哈希,返回 ``[(block_hash, block_bytes), ...]``。"""
    return [(bid, data[s:e])
            for (bid, _full), (s, e) in zip(blocked_hashes(data, block_size),
                                            split(data, block_size))]


class BlockHasher:
    """**流式**切块与哈希,按 vLLM 的 ``block_size`` 累积满块。

    只保留当前未完成块的缓冲,内存占用与块大小成正比。
    产出的 ``block_hash`` 与 :func:`blocked_hashes` 完全一致。

    用法::

        hasher = BlockHasher()
        for piece in byte_stream:
            for block_hash, block in hasher.feed(piece):
                ...
        for block_hash, block in hasher.flush():
            ...
    """

    __slots__ = ("_block_size", "_buf", "_parent")

    def __init__(self, block_size: int = BLOCK_SIZE,
                 parent: bytes = b"") -> None:
        """
        Args:
          block_size: 定长块字节数
          parent: 链上前一个满块的哈希。从头切时为空;v3 网关续写一段前缀时,
                  传入那段前缀的链尾,于是新块的哈希天然挂在前缀之下。
        """
        self._block_size = block_size
        self._buf = bytearray()
        self._parent = parent

    def feed(self, data: bytes) -> Iterator[Tuple[bytes, bytes]]:
        """喂入字节,产出 ``(block_hash, block)``。只产出**满块**。"""
        if data:
            self._buf += data
        size = self._block_size
        while len(self._buf) >= size:
            block = bytes(self._buf[:size])
            del self._buf[:size]
            bid = block_hash(block, self._parent)
            self._parent = bid
            yield bid, block

    def take_pending(self) -> bytes:
        """取走「已喂入但还不满一块」的字节,并清空缓冲。

        v3 客户端扫到匹配边界后要接着发尾部:那些字节已经被 ``feed`` 吃掉存在
        缓冲里,必须显式取出来交给 body,否则尾部会丢掉这一段。
        """
        if not self._buf:
            return b""
        out = bytes(self._buf)
        self._buf = bytearray()
        return out

    def flush(self) -> Iterator[Tuple[bytes, bytes]]:
        """冲刷不满的尾块。调用方不应把它当"可复用块"缓存。

        注意:尾块的哈希**不进入**链,所以它不影响后续块。
        """
        if self._buf:
            block = bytes(self._buf)
            self._buf = bytearray()
            yield block_hash(block, self._parent), block
