"""编码器 / 解码器:文本字节流 ⇄ 帧流。

**这两层完全不认识聊天协议。** 它们只看到字节:按 vLLM 的定长 block_size
切块、查缓存决定发引用帧还是新块帧。换成任何其他格式的转换器,这两层一行都
不用改。

**两层都是流式的。** 文本片段进、帧片段出,不需要先把完整文本拼出来。
内存占用只与「单个块」的大小成正比,与请求体总大小无关。

数据流(上行)::

    渲染文本片段流
      → 累积到够一个块 → 算 block_hash
          → 客户端已知该 id  → 发引用帧(17 字节)
          → 未知              → 发新块帧(内容直传)
            → 流结束时冲刷尾块,追加 END

数据流(下行,网关侧)::

    帧流片段(增量喂入)
      → 引用帧:从存储取块内容
      → 新块帧:内容直传
        → 按到达顺序产出文本片段
          → 交给流式反渲染器

退化保证:任何一块取不到,解码器记录到 ``missing`` 并跳过,调用方据此
让客户端重发全文。缓存全空、存储挂掉、服务重启时系统仍能正常工作。
"""

from __future__ import annotations

from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from . import chunking, frames
from .frames import Frame


# ---------------------------------------------------------------------------
# 客户端侧编码器
# ---------------------------------------------------------------------------

#: 编码输出合并到这么大再交给上层。
#:
#: 一个 512B 块编成引用帧只有 17B。若每帧单独 yield,一个 800 KB 上下文会产生
#: 1500+ 个 17B 片段,每个片段在 HTTP 层都要单独成一个 chunk,网关那边还要
#: 逐片过一遍「事件循环 ↔ 线程池」的桥接。实测 800 KB 热请求因此多付约
#: 170 ms——远超编码本身(3.7 ms)。
#:
#: 取值依据:
#:
#: - **nginx 的请求体缓冲区默认就是这一档**。``client_body_buffer_size`` 默认
#:   ``8k|16k``——文档原文是「等于两个内存页,x86 与 x86-64 是 8K,其它 64 位
#:   平台通常是 16K」(https://nginx.org/en/docs/http/ngx_http_core_module.html)。
#:   所以 16 KB 落在主流反向代理的默认区间内,不需要对方调参。
#: - **本地实测无副作用**:888 KB 热请求按 4K/8K/16K/32K/64K 分别合并,端到端
#:   中位数 38.4 / 39.1 / 37.3 / 37.7 / 37.7 ms——≥8 KB 后已在噪声范围内,
#:   4 KB 略差。取 16 KB 而不是 64 KB,是为了让首个片段尽早发出(对端收到第一个
#:   块就能开始解码),不为了更少的 chunk 去推迟首字节。
OUTPUT_CHUNK = 16 * 1024


class Encoder:
    """把渲染文本编码成帧流。

    客户端需要维护一个「已知块 id 集合」——即对端(网关/存储)已经有的块。
    只发未知块的内容,已知块只发引用。

    集合的维护策略:
      - 首次请求:集合为空,全部发新块帧(冷启动)
      - 编码过后:把本次发过的块 id 加入集合(对端已存下)
      - 进程重启后集合丢失:退化为冷启动,仍能正常完成
    """

    def __init__(self, known: Optional[Iterable[bytes]] = None,
                 block_size: int = chunking.BLOCK_SIZE) -> None:
        self._known = set(known or ())
        self._block_size = block_size

    @property
    def known_count(self) -> int:
        return len(self._known)

    def iter_encode(self, text_pieces: Iterable[bytes]) -> Iterator[bytes]:
        """流式编码:喂文本片段流,产出帧流片段(已合并成 ~16 KB 一片)。"""
        hasher = chunking.BlockHasher(self._block_size)

        def raw_frames() -> Iterator[bytes]:
            for piece in text_pieces:
                for bid, block in hasher.feed(piece):
                    yield self._encode_block(bid, block)
            for bid, block in hasher.flush():
                yield self._encode_block(bid, block)
            yield frames.encode_end()

        yield from _coalesce(raw_frames())

    def iter_encode_events(self, events) -> Iterator[bytes]:
        """流式编码**事件流**。

        事件经 ``event_to_bytes`` 转成字节后直接喂给 :class:`BlockHasher`——
        切块只看字节,不看事件边界(与 vLLM 的 block_size 对齐方式一致)。
        编码器不需要认识任何渲染标记。
        """
        from .render import EventKind, event_to_bytes

        hasher = chunking.BlockHasher(self._block_size)

        def raw_frames() -> Iterator[bytes]:
            for kind, payload in events:
                for piece in event_to_bytes((kind, payload)):
                    for bid, block in hasher.feed(piece):
                        yield self._encode_block(bid, block)
            for bid, block in hasher.flush():
                yield self._encode_block(bid, block)
            yield frames.encode_end()

        yield from _coalesce(raw_frames())

    def _encode_block(self, block_hash: bytes, block: bytes) -> bytes:
        """已知块发引用帧,否则发新块帧(帧里带哈希)。"""
        if block_hash in self._known:
            return frames.encode_ref(block_hash)
        self._known.add(block_hash)
        return frames.encode_new(block_hash, block)

    def encode(self, text: bytes) -> bytes:
        """一次性编码。测试与短请求用;生产路径用 :meth:`iter_encode`。"""
        return b"".join(self.iter_encode([text]))

    def encode_frames(self, text: bytes) -> List[Frame]:
        """编码成帧列表(测试/调试用)。"""
        out: List[Frame] = []
        for block_hash, block in chunking.blocked_to_blocks(text):
            if block_hash in self._known:
                out.append(("ref", block_hash))
            else:
                out.append(("new", (block_hash, block)))
                self._known.add(block_hash)
        return out

    def forget(self, block_hashes: Iterable[bytes]) -> None:
        """把块从已知集合里移除(缓存被淘汰、存储不可用时调用)。

        下一轮这些块会被当新块重发。**不需要任何补发协议**——
        新块帧本身就带完整内容,这是扁平块集合设计的好处。
        """
        for bh in block_hashes:
            self._known.discard(bh)

    def clear(self) -> None:
        self._known.clear()


# ---------------------------------------------------------------------------
# 网关侧解码器
# ---------------------------------------------------------------------------

class MissingBlock(Exception):
    """引用帧指向的块在存储里不存在。

    这是**可恢复**的:调用方应让客户端重发全文,而不是返回错误给用户。
    正常情况下不会发生(客户端只引用自己发过的块),除非存储被淘汰或重启。
    """

    def __init__(self, block_hash: bytes) -> None:
        super().__init__(f"块不存在: {block_hash.hex()}")
        self.block_hash = block_hash


class BlockSource:
    """块内容来源的抽象。网关侧由存储实现它。"""

    def get_many(self, block_hashes: List[bytes]) -> Dict[bytes, bytes]:
        """批量取块。返回 id → 内容,缺失的 id 不出现在结果里。"""
        raise NotImplementedError


class DictBlockSource(BlockSource):
    """内存块表。测试与单机部署用。"""

    def __init__(self, data: Optional[Dict[bytes, bytes]] = None) -> None:
        self._data = dict(data or {})

    def get_many(self, block_hashes: List[bytes]) -> Dict[bytes, bytes]:
        return {bh: self._data[bh] for bh in block_hashes if bh in self._data}

    def put_many(self, blocks: Dict[bytes, bytes]) -> None:
        self._data.update(blocks)

    def __len__(self) -> int:
        return len(self._data)


class NullBlockSource(BlockSource):
    """什么都取不到。用于"缓存完全不可用"的降级路径。"""

    def get_many(self, block_hashes: List[bytes]) -> Dict[bytes, bytes]:
        return {}


class Decoder:
    """**流式**解码:边收帧边产出文本片段,不构造完整对象。

    内存占用与「单块大小」成正比,与请求体总大小无关。

    用法::

        dec = Decoder(source)
        for body_chunk in body_chunks:
            for text_piece in dec.feed(body_chunk):
                forward(text_piece)      # 边解边转发
        dec.finish()
        if dec.missing: retransmit()
    """

    __slots__ = ("_frames", "_source", "_on_new_block",
                 "missing", "new_blocks", "ref_count", "_closed")

    def __init__(self, source: BlockSource,
                 on_new_block=None) -> None:
        """
        Args:
          source: 块内容来源
          on_new_block: 收到新块帧时的回调 ``(block_hash, content)``,
                        用于把新块写入存储

        缺块**必须立刻抛** :class:`MissingBlock`,没有「跳过继续解」这一档。
        跳过会产出带洞的文本:洞落在 UTF-8 多字节字符中间时,后续片段的首字节
        不合法(实测 ``UnicodeDecodeError``);洞落在 message 边界时,拼出的
        prompt 语义已经变了却仍是合法 JSON,会被**静默转发给上游**,白跑一次
        推理再把结果丢掉。两种都不该发生。
        """
        self._frames = frames.FrameDecoder()
        self._source = source
        self._on_new_block = on_new_block
        self.missing: List[bytes] = []
        self.new_blocks: Dict[bytes, bytes] = {}
        self.ref_count = 0
        self._closed = False

    def feed(self, data: bytes) -> Iterator[bytes]:
        """喂入帧流字节,产出已还原的文本片段(可能为空)。"""
        if self._closed:
            return
        for kind, payload in self._frames.feed(data):
            if kind == "new":
                # 块哈希由发送方算好随帧传来(哈希链依赖父哈希,解码端算不出)
                bh, content = payload
                self.new_blocks[bh] = content
                if self._on_new_block is not None:
                    self._on_new_block(bh, content)
                yield content
            else:
                bid = payload
                self.ref_count += 1
                content = self.new_blocks.get(bid)
                if content is None:
                    content = self._source.get_many([bid]).get(bid)
                if content is None:
                    self.missing.append(bid)
                    raise MissingBlock(bid)
                yield content

    def finish(self) -> None:
        self._closed = True

    @property
    def done(self) -> bool:
        return self._frames.done

    @property
    def ok(self) -> bool:
        """是否完整收到结束帧且没有缺块。"""
        return self._frames.eof_ok and not self.missing


def decode_all(data: bytes, source: BlockSource) -> Tuple[bytes, Decoder]:
    """一次性解码(测试用)。返回 (文本, 解码器)。缺块抛 :class:`MissingBlock`。"""
    dec = Decoder(source)
    out = b"".join(dec.feed(data))
    dec.finish()
    return out, dec


# ---------------------------------------------------------------------------
# 便捷:一次性编解码
# ---------------------------------------------------------------------------

def encode_text(text: bytes, known: Optional[Iterable[bytes]] = None) -> bytes:
    return Encoder(known).encode(text)


def collect_blocks(text: bytes) -> Dict[bytes, bytes]:
    """把文本切成块并返回 ``block_hash → 内容``(只含满块)。

    与 vLLM 的 "only cache full blocks" 一致。复用 storage 的实现,
    避免两处各写一遍"只存满块"的规则。
    """
    from .storage import _blocks_of
    return _blocks_of(text)


def _coalesce(chunks: Iterator[bytes], limit: int = OUTPUT_CHUNK) -> Iterator[bytes]:
    """把细碎帧合并成 ``limit`` 左右的片段再吐出去。

    只做缓冲,不改内容:拼起来与逐帧输出的字节流完全相同。最后一片即使不满
    也会吐出,不会截断。
    """
    buf = bytearray()
    for chunk in chunks:
        buf += chunk
        if len(buf) >= limit:
            yield bytes(buf)
            buf.clear()
    if buf:
        yield bytes(buf)
