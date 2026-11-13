"""编码器 / 解码器:文本字节流 ⇄ 帧流。

**这两层完全不认识聊天协议。** 它们只看到字节:按 CDC 切块、算 block_id、
查缓存决定发引用帧还是新块帧。换成任何其他格式的转换器,这两层一行都不用改。

这是本方案与原 v2.1 方案的本质区别:原方案里网关要理解 role / system / tools
的位置和结构,任何协议变化都要改缓存逻辑;现在缓存逻辑只认字节。

数据流(上行):

    文本字节流
      → 按 CDC 切块
        → 每块算 block_id
          → 客户端本地已知该 id  → 发引用帧(17 字节)
          → 未知                  → 发新块帧(内容直传)
            → 追加 END

数据流(下行,网关侧):

    帧流(增量喂入)
      → 引用帧:从存储取块内容
      → 新块帧:内容直传
        → 按到达顺序拼成一维文本流
          → 交给反渲染器

**退化保证**:任何一块取不到,就退化为"整段文本直传"而不是报错。缓存全空、
存储挂掉、服务重启时系统仍能正常工作,只是没有去重收益。
"""

from __future__ import annotations

from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from . import chunking, frames
from .frames import Frame


# ---------------------------------------------------------------------------
# 客户端侧编码器
# ---------------------------------------------------------------------------

class Encoder:
    """把一整段文本编码成帧流。

    客户端需要维护一个「已知块 id 集合」——即对端(网关/存储)已经有的块。
    只发未知块的内容,已知块只发引用。

    这个集合的维护策略:
      - 首次请求:集合为空,全部发新块帧(冷启动)
      - 收到响应后:把本次发过的块 id 加入集合(对端已存下)
      - 进程重启后集合丢失:退化为冷启动,仍能正常完成

    也可以只存 id 不存内容(省内存),此时无法离线重建完整请求。
    """

    def __init__(self, known: Optional[Iterable[bytes]] = None) -> None:
        self._known = set(known or ())

    @property
    def known_count(self) -> int:
        return len(self._known)

    def encode(self, text: bytes) -> bytes:
        """把完整文本编码成帧流字节。"""
        out = bytearray()
        for chunk in chunking.split_blocks(text):
            bid = chunking.block_id(chunk)
            if bid in self._known:
                out += frames.encode_ref(bid)
            else:
                out += frames.encode_new(chunk)
                self._known.add(bid)
        out += frames.encode_end()
        return bytes(out)

    def encode_frames(self, text: bytes) -> List[Frame]:
        """编码成帧列表(测试/调试用)。"""
        out: List[Frame] = []
        for chunk in chunking.split_blocks(text):
            bid = chunking.block_id(chunk)
            if bid in self._known:
                out.append(("ref", bid))
            else:
                out.append(("new", chunk))
                self._known.add(bid)
        return out

    def forget(self, block_ids: Iterable[bytes]) -> None:
        """把块从已知集合里移除(缓存被淘汰、存储不可用时调用)。

        下一轮这些块会被当新块重发。**不需要任何补发协议**——
        新块帧本身就带完整内容,这一点是扁平块集合设计的好处。
        """
        for bid in block_ids:
            self._known.discard(bid)

    def clear(self) -> None:
        self._known.clear()


# ---------------------------------------------------------------------------
# 网关侧解码器
# ---------------------------------------------------------------------------

class MissingBlock(Exception):
    """引用帧指向的块在存储里不存在。

    这是**可恢复**的:调用方应退化为让客户端重发全文,而不是返回错误给用户。
    正常情况下不会发生(客户端只引用自己发过的块),除非存储被淘汰或重启。
    """

    def __init__(self, block_id: bytes) -> None:
        super().__init__(f"块不存在: {block_id.hex()}")
        self.block_id = block_id


class BlockSource:
    """块内容来源的抽象。网关侧由存储实现它。"""

    def get_many(self, block_ids: List[bytes]) -> Dict[bytes, bytes]:
        """批量取块。返回 id → 内容,缺失的 id 不出现在结果里。"""
        raise NotImplementedError


class DictBlockSource(BlockSource):
    """内存块表。测试与单机部署用。"""

    def __init__(self, data: Optional[Dict[bytes, bytes]] = None) -> None:
        self._data = dict(data or {})

    def get_many(self, block_ids: List[bytes]) -> Dict[bytes, bytes]:
        return {bid: self._data[bid] for bid in block_ids if bid in self._data}

    def put_many(self, blocks: Dict[bytes, bytes]) -> None:
        self._data.update(blocks)

    def __len__(self) -> int:
        return len(self._data)


class NullBlockSource(BlockSource):
    """什么都取不到。用于"缓存完全不可用"的降级路径。"""

    def get_many(self, block_ids: List[bytes]) -> Dict[bytes, bytes]:
        return {}


class Decoder:
    """增量式解码:边收帧边拼文本,不构造完整对象。

    内存占用与「单块大小」成正比,与请求体总大小无关——这是流式的关键。

    用法::

        dec = Decoder(source)
        for chunk in body_chunks:
            for piece in dec.feed(chunk):
                send_downstream(piece)     # 边解边转发
        dec.finish()
    """

    def __init__(self, source: BlockSource,
                 strict: bool = False,
                 on_new_block=None) -> None:
        """
        Args:
          source: 块内容来源
          strict: True 时缺块抛 :class:`MissingBlock`;False 时记录到
                  :attr:`missing` 并跳过(调用方据此决定是否要求全文重传)
          on_new_block: 收到新块帧时的回调 ``(block_id, content)``,
                        用于把新块写入存储
        """
        self._frames = frames.FrameDecoder()
        self._source = source
        self._strict = strict
        self._on_new_block = on_new_block
        self.missing: List[bytes] = []
        self.new_blocks: Dict[bytes, bytes] = {}
        self.ref_count = 0          # 成功解出的引用帧数量
        self._closed = False

    def feed(self, data: bytes) -> Iterator[bytes]:
        """喂入帧流字节,产出已还原的文本片段(可能为空)。"""
        if self._closed:
            return
        for kind, payload in self._frames.feed(data):
            if kind == "new":
                # payload 是新块内容
                bid = chunking.block_id(payload)
                self.new_blocks[bid] = payload
                if self._on_new_block is not None:
                    self._on_new_block(bid, payload)
                yield payload
            else:
                # payload 是 block_id。先看本轮新块,再看存储。
                bid = payload
                self.ref_count += 1
                content = self.new_blocks.get(bid)
                if content is None:
                    got = self._source.get_many([bid])
                    content = got.get(bid)
                if content is None:
                    self.missing.append(bid)
                    if self._strict:
                        raise MissingBlock(bid)
                    continue
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


def decode_all(data: bytes, source: BlockSource,
               strict: bool = False) -> Tuple[bytes, Decoder]:
    """一次性解码(测试用)。返回 (文本, 解码器)。"""
    dec = Decoder(source, strict=strict)
    out = b"".join(dec.feed(data))
    dec.finish()
    return out, dec


# ---------------------------------------------------------------------------
# 便捷:一次性编解码
# ---------------------------------------------------------------------------

def encode_text(text: bytes, known: Optional[Iterable[bytes]] = None) -> bytes:
    return Encoder(known).encode(text)


def collect_blocks(text: bytes) -> Dict[bytes, bytes]:
    """把文本切成块并返回 id → 内容。网关写入存储时用。"""
    return {chunking.block_id(c): c for c in chunking.split_blocks(text)}
