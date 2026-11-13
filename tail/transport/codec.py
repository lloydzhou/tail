"""v3 编解码:前缀凭据 + 尾部字节。

职责划分(这是 v3 与 v2 最本质的区别):

  客户端                                网关
  ------                                ------
  渲染文本                               切块、算块哈希、写存储
  自检「前 L 字节没变」(只读校验)         按 (L, tip) 走链取回前缀
  发出第 L 字节之后的内容                 把新块按父链链式写入

客户端**不产生任何会被存储的哈希**——它的哈希只用于本地比对。因此 v2 那个
「网关信任客户端声称的块哈希 → 可投毒」的缺口在结构上不存在了。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

from . import chunking, render
from .chunking import BLOCK_SIZE

#: 一片文本的目标大小。只影响 socket 写入次数,不影响语义。
OUTPUT_CHUNK = 16 * 1024

#: 网关每批取多少块内容 / 每批写多少块。
BLOCK_BATCH = 256


class MissingPrefix(Exception):
    """按 (L, tip) 走链拿不到完整前缀(被淘汰 / 存储被清 / 重启)。

    可恢复:调用方回 409 + 重发信号,客户端从 0 重发。
    """

    def __init__(self, detail: str) -> None:
        super().__init__(f"前缀不完整: {detail}")
        self.detail = detail


def _coalesce(pieces: Iterator[bytes], limit: int = OUTPUT_CHUNK) -> Iterator[bytes]:
    """把小片段合并成 ~limit 的片,减少 socket 写入 / TLS record 次数。"""
    buf = bytearray()
    for piece in pieces:
        if not piece:
            continue
        buf += piece
        if len(buf) >= limit:
            yield bytes(buf)
            buf.clear()
    if buf:
        yield bytes(buf)


# ---------------------------------------------------------------------------
# 客户端:渲染 → 自检前缀 → 尾部字节
# ---------------------------------------------------------------------------

@dataclass
class PreparedRequest:
    """一次准备好的请求:头里的凭据 + 要发的尾部字节流。"""

    session: bytes
    have_len: int
    have_tip: bytes
    body: Iterator[bytes]
    text_bytes: int          # 渲染文本总长(扫描到的部分;完整长度见 full_bytes)
    reused_bytes: int        # 其中判定为「网关已有」的字节数


class PrefixClient:
    """客户端侧:渲染、自检、产出尾部字节。

    本地状态只有「会话键 → [(L, tip), ...]」,每个凭据 8+16 字节,与会话数线性、
    与上下文长度无关。v2 的 ``set[bytes]`` 已知块集合(131 B/块)不再需要。
    """

    def __init__(self, block_size: int = BLOCK_SIZE,
                 max_credentials: int = 4) -> None:
        self._bs = block_size
        self._max = max_credentials
        #: 会话键 → 候选凭据(最近的在前)。保留多个是为了容忍并发/乱序的响应。
        self._creds: Dict[bytes, List[Tuple[int, bytes]]] = {}

    # -- 凭据维护 ---------------------------------------------------------

    def credentials(self, session: bytes) -> List[Tuple[int, bytes]]:
        return list(self._creds.get(session, ()))

    def set_credential(self, session: bytes, length: int, tip: bytes) -> None:
        """记录服务端确认的前缀(来自响应头 ``X-Tail-Have``)。"""
        if not session or length <= 0 or len(tip) != chunking.BLOCK_ID_SIZE:
            return
        lst = [(l, t) for (l, t) in self._creds.get(session, ()) if l != length]
        lst.insert(0, (length, tip))
        self._creds[session] = lst[: self._max]

    def invalidate(self, session: Optional[bytes] = None) -> None:
        """丢弃凭据。下一轮从 0 重发。"""
        if session is None:
            self._creds.clear()
        else:
            self._creds.pop(session, None)

    @property
    def sessions(self) -> int:
        return len(self._creds)

    @property
    def credential_bytes(self) -> int:
        """本地状态占用(纯数据,不含容器开销)。"""
        return sum(8 + len(t) for lst in self._creds.values() for _, t in lst)

    # -- 编码 -------------------------------------------------------------

    def prepare(self, payload: Dict) -> PreparedRequest:
        """扫描渲染文本,找出与对端共同持有的最长前缀,产出尾部字节流。"""
        gen = render.iter_render(payload)
        hasher = chunking.BlockHasher(self._bs)
        session = b""
        candidates: List[Tuple[int, bytes]] = []
        max_len: Optional[int] = None
        best_len, best_tip = 0, b""
        pos = 0

        for piece in gen:
            for bid, _block in hasher.feed(piece):
                pos += self._bs
                if max_len is None:
                    # 首块哈希就是会话键(内容寻址,不需要客户端发明 id)
                    session = bid
                    candidates = self._creds.get(session, ())
                    max_len = max((length for length, _ in candidates), default=0)
                for length, tip in candidates:
                    if length == pos and tip == bid:
                        if length > best_len:
                            best_len, best_tip = length, bid
                        break
            if max_len is not None and pos >= max_len:
                break

        if best_len and best_len == pos:
            # 正好停在匹配边界:hasher 缓冲里那截「已吃进但不满一块」的字节
            # 加上渲染器后续输出,就是尾部
            body = _coalesce(_chain(hasher.take_pending(), gen))
            text_bytes = pos
        else:
            # 前缀变了(compact / 改中间消息 / 改顶层参数)或没有凭据:
            # 从头重渲染,全量发出。dict 还在手里,重渲染只是一次 CPU。
            body = _coalesce(_skip(self._bs, best_len, payload))
            text_bytes = 0
        return PreparedRequest(session=session, have_len=best_len,
                               have_tip=best_tip, body=body,
                               text_bytes=text_bytes, reused_bytes=best_len)


def _chain(head: bytes, rest: Iterator[bytes]) -> Iterator[bytes]:
    if head:
        yield head
    yield from rest


def _skip(block_size: int, skip: int, payload: Dict) -> Iterator[bytes]:
    """重新渲染并跳过前 ``skip`` 字节。"""
    left = skip
    for piece in render.iter_render(payload):
        if left:
            if len(piece) <= left:
                left -= len(piece)
                continue
            piece = piece[left:]
            left = 0
        yield piece


# ---------------------------------------------------------------------------
# 网关:前缀 + 尾部 → 文本,顺便把新块链式写回
# ---------------------------------------------------------------------------

class PrefixReader:
    """网关侧:把「存储里的前缀」和「请求带来的尾部」拼成一条文本流。

    切块与哈希都在这里发生——**块哈希的权威只有这一处**。
    """

    def __init__(self, store, block_size: int = BLOCK_SIZE,
                 have_len: int = 0, have_tip: bytes = b"",
                 batch: int = BLOCK_BATCH) -> None:
        self._store = store
        self._bs = block_size
        self._have_len = max(0, have_len - have_len % block_size)
        self._have_tip = have_tip
        self._batch = batch

        self.block_size = block_size
        self.prefix_blocks = self._have_len // block_size
        self.new_blocks = 0
        self.prefix_bytes = 0
        self.tail_bytes = 0
        #: 最终链尾(可以交给客户端作为下一轮的 tip)
        self.tip = have_tip if self.prefix_blocks else b""
        self._chain_parent = self.tip

    @property
    def total_blocks(self) -> int:
        return self.prefix_blocks + self.new_blocks

    @property
    def hit_ratio(self) -> float:
        total = self.prefix_bytes + self.tail_bytes
        return (self.prefix_bytes / total) if total else 0.0

    def iter_text(self, tail_chunks) -> Iterator[bytes]:
        """产出完整渲染文本片段:先是存储里的前缀,再是请求带来的尾部。"""
        if self.prefix_blocks:
            ids = self._store.resolve(self._have_tip, self.prefix_blocks)
            if ids is None:
                raise MissingPrefix(f"tip={self._have_tip.hex()[:16]} "
                                    f"blocks={self.prefix_blocks}")
            for i in range(0, len(ids), self._batch):
                want = ids[i:i + self._batch]
                got = self._store.get_many(want)
                for bid in want:
                    content = got.get(bid)
                    if content is None:
                        raise MissingPrefix(f"block={bid.hex()[:16]}")
                    self.prefix_bytes += len(content)
                    yield content

        hasher = chunking.BlockHasher(self._bs, parent=self.tip)
        pending: List[Tuple[bytes, bytes]] = []
        for chunk in tail_chunks:
            self.tail_bytes += len(chunk)
            for bid, block in hasher.feed(chunk):
                pending.append((bid, block))
                self.new_blocks += 1
                if len(pending) >= self._batch:
                    self._chain_parent = self._store.put_chain(
                        self._chain_parent, pending)
                    pending = []
                yield block
        # 尾块(不满 512 B)照发但不入库:它的内容还会被后续追加改变
        for _bid, block in hasher.flush():
            yield block
        if pending:
            self._chain_parent = self._store.put_chain(self._chain_parent, pending)
        self.tip = self._chain_parent
