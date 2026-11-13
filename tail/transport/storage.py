"""块存储:一块一个 key,value = 父哈希 ‖ 正文。

    blk:{block_hash_hex} → parent_hex(32 ASCII) ‖ content(≤块大小)

因为 ``block_hash = blake2b(parent_hash ‖ block)[:16]``,「链尾摘要」等价于
「整条前缀的入口」:给定 tip 和前缀块数,可以从后往前走回第一块。

与 vLLM 的关系:vLLM 的 ``block_hash = hash(parent_hash, block)`` 一模一样;
差异只在链放在哪——vLLM 的推理进程手里有完整 token 序列,可以从第 0 块**正向
重算**整条哈希再逐个查表,所以它的索引只有 ``hash → block_id`` 一张字典,不需要
父指针。网关按定义拿不到前缀(只收 ``(L, tip)`` + 尾部),只能从 tip **反向**走,
父指针就必须真的存下来:那 32 B/块是「看不到全文」这个前提的代价,不是冗余。

**为什么读只有一次往返**:``read_prefix`` 用一个 Lua 脚本走完 N 跳,把整条前缀的
value 一次带回。正文里的 32 字节父哈希在 **Python 侧**用 ``memoryview`` 切掉——
零拷贝,只挪指针;真正的一次拷贝发生在最后 ``b"".join``。早先在 Lua 里
``string.sub(v, 33)`` 切,那是每块一次真实拷贝:1,774 块时慢 16%,4 KB 块时慢 30%
(块越大越亏),所以脚本只负责走链,不碰正文。

**注意返回顺序**:脚本按走链方向返回,**链尾在前、链头在后**。``read_prefix``
在 Python 侧倒过来,对外保证「从前到后」;实现细节别改成「按走链顺序返回」,
上层直接喂解析器,顺序反了不会报错,只会把上下文拼反。

读写都用 Lua,保证原子性:

  - **写**:`put_chain` 用一个脚本一次写完整串(连同 TTL)。否则网关在只写了一半
    时就把新 tip 告诉客户端,对端拿到的是一个悬挂的链——读的时候
    `read_prefix` 返回 None,表现为「刚发过的前缀立刻 miss」。脚本原子,要么整串
    可见,要么全不可见。
  - **读**:`read_prefix` 用一个脚本完成「走链 + 逐块续期」。per-block TTL 下,
    读和续期必须是同一件事,否则出现「读到一半某块过期」的中间态。

**递归必须在脚本里做**:每跳都依赖上一跳的结果,由网关逐跳发命令就是 N 次网络往返。
"""

from __future__ import annotations

from collections import OrderedDict
from typing import List, Optional, Sequence, Tuple, Union

#: 块 key 前缀
KEY_BLOCK = "blk:"

#: parent 在 value 里的编码长度(16 字节哈希的 hex)
_PARENT_HEX = 32

#: 链根的父哈希。必须**定长**:value 是 ``parent_hex(32) ‖ content``,
#: 早先把根写成空串,读回来时 ``v[32:]`` 会吃掉根块正文的前 32 字节。
_ZERO_PARENT = b"\x00" * 16

#: 一块正文。Redis 后端返回 ``memoryview``(零拷贝切掉父哈希),内存后端就是
#: ``bytes``;两者都支持 len/迭代/拼接,上层按 bytes 用即可。
BlockContent = Union[bytes, memoryview]


def _encode_parent(parent: bytes) -> bytes:
    return (parent or _ZERO_PARENT).hex().encode("ascii")


class ChainStore:
    """链式块存储接口。"""

    def put_chain(self, parent: bytes,
                  items: Sequence[Tuple[bytes, bytes]]) -> bytes:
        """按顺序写入一串块,第一块的父哈希是 ``parent``。返回新的链尾。"""
        raise NotImplementedError

    def read_prefix(self, tip: bytes,
                    n_blocks: int) -> Optional[List[BlockContent]]:
        """从链尾走 ``n_blocks`` 步,返回**从前到后**的正文列表。

        每项已切掉 value 里的父哈希前缀。前缀不完整(中间有块被淘汰/清空/
        过期)时返回 ``None``——上层据此回 409 让客户端重发,绝不拼出带洞的前缀。
        """
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError


class MemoryChainStore(ChainStore):
    """进程内链式存储。测试与单机嵌入用。

    LRU 用 ``OrderedDict`` + ``move_to_end`` 做 O(1) 触碰。早先用 ``list`` 记顺序、
    每次读取 ``list.remove(bid)``,单次读取就退化成 O(n²)——1,774 块的前缀读一次要
    137 ms,把基准里「网关解码」这一项彻底污染(§9 记录了这个坑)。
    """

    def __init__(self, capacity: Optional[int] = None) -> None:
        self._data: "OrderedDict[bytes, Tuple[bytes, bytes]]" = OrderedDict()
        self._capacity = capacity

    def put_chain(self, parent: bytes,
                  items: Sequence[Tuple[bytes, bytes]]) -> bytes:
        tip = parent
        for bid, content in items:
            self._data[bid] = (tip, content)
            self._data.move_to_end(bid)
            tip = bid
        if self._capacity is not None:
            while len(self._data) > self._capacity:
                self._data.popitem(last=False)
        return tip

    def read_prefix(self, tip: bytes,
                    n_blocks: int) -> Optional[List[BlockContent]]:
        if n_blocks <= 0:
            return []
        out: List[BlockContent] = []
        cur = tip
        for _ in range(n_blocks):
            item = self._data.get(cur)
            if item is None:
                return None
            self._data.move_to_end(cur)
            out.append(item[1])
            cur = item[0]
        out.reverse()
        return out

    def clear(self) -> None:
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

#: 原子写入整串块。KEYS = 完整 key 列表,ARGV[1] = TTL 秒,ARGV[2..] = value。
#: 一次脚本内写完,保证「链要么整串可见,要么全不可见」。
_PUT_CHAIN_LUA = """
local ttl = tonumber(ARGV[1])
for i = 1, #KEYS do
  if ttl > 0 then
    redis.call('SET', KEYS[i], ARGV[i + 1], 'EX', ttl)
  else
    redis.call('SET', KEYS[i], ARGV[i + 1])
  end
end
return #KEYS
"""

#: 走链 + 逐块续期,一次往返、原子。返回 value 原样(父哈希 + 正文)。
#: KEYS[1] = "blk:" 前缀,ARGV[1] = tip hex,ARGV[2] = 块数,ARGV[3] = TTL 秒
#: 返回从链尾到链头的 value 列表;任何一跳缺失返回空表(上层判 miss)。
#:
#: 脚本**不切正文**:切是每块一次真实拷贝,交给 Python 侧 memoryview 零拷贝切。
#: 它只负责走链,``string.sub(v, 1, 32)`` 取的是下一跳的 key,不是正文。
_RESOLVE_LUA = """
local prefix = KEYS[1]
local cur = ARGV[1]
local n = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local out = {}
for i = 1, n do
  local k = prefix .. cur
  local v = redis.call('GET', k)
  if not v then return {} end
  if ttl > 0 then redis.call('EXPIRE', k, ttl) end
  out[#out + 1] = v
  cur = string.sub(v, 1, 32)
end
return out
"""


class RedisChainStore(ChainStore):
    """Redis 链式存储(生产唯一后端)。

    value = parent_hex(32 B) ‖ content。用 hex 而不是裸 16 字节,省在 Lua 里少一次
    编解码;代价是每块多 16 B(约 3%),换来的是脚本能直接用 ``string.sub`` 走链。
    """

    def __init__(self, client, ttl: Optional[int] = None,
                 namespace: str = "tail") -> None:
        """
        Args:
          client: ``redis.Redis`` 同步客户端
          ttl: 每块的过期秒数;``read_prefix`` 时会顺带把整条前缀续到这个值
          namespace: key 前缀,便于多环境隔离
        """
        self._client = client
        self._ttl = int(ttl or 0)
        self._ns = namespace + ":"
        self._resolve = client.register_script(_RESOLVE_LUA)
        self._put_chain = client.register_script(_PUT_CHAIN_LUA)

    # -- key ---------------------------------------------------------------
    def _key(self, bid: bytes) -> str:
        return self._ns + KEY_BLOCK + bid.hex()

    # -- 写 ---------------------------------------------------------------
    def put_chain(self, parent: bytes,
                  items: Sequence[Tuple[bytes, bytes]]) -> bytes:
        """用**一个 Lua 脚本**原子写入整串块(一块一个 key,同一个 TTL)。

        不能用 pipeline:非事务管线只保证「按顺序发」,中途出错(或进程被杀)会留下
        半条链——而网关随后会把那段前缀的 tip 告诉客户端,对端下一次拿着一个
        悬挂的 tip 来,`read_prefix` 返回 None,表现为「刚发过的内容立刻 miss」。
        脚本内写完才返回,链要么整串可见,要么全不可见。

        块数很多时调用方要自己分批(每批几百块),避免脚本长时间占住实例。
        """
        if not items:
            return parent
        keys: List[str] = []
        values: List[bytes] = []
        tip = parent
        for bid, content in items:
            keys.append(self._key(bid))
            values.append(_encode_parent(tip) + content)
            tip = bid
        self._put_chain(keys=keys, args=[self._ttl, *values])
        return tip

    # -- 读 ---------------------------------------------------------------
    def read_prefix(self, tip: bytes,
                    n_blocks: int) -> Optional[List[BlockContent]]:
        """走链:一个 Lua 脚本、一次往返,整条前缀的正文一起带回来。

        返回的每项是 ``memoryview``:从 value 的 byte 33 开始,父哈希没有拷贝、
        正文也没有拷贝,只切了一层视图。
        """
        if n_blocks <= 0:
            return []
        raw = self._resolve(keys=[self._ns + KEY_BLOCK],
                            args=[tip.hex(), n_blocks, self._ttl])
        if not raw or len(raw) != n_blocks:
            return None
        # 脚本按走链方向返回(链尾在前),对外倒成「从前到后」。
        return [memoryview(v)[_PARENT_HEX:] for v in reversed(raw)]

    def clear(self) -> None:
        # 只删本命名空间下的块:SCAN 而不是 KEYS,避免阻塞
        pattern = self._ns + KEY_BLOCK + "*"
        batch: List[bytes] = []
        for key in self._client.scan_iter(match=pattern, count=1000):
            batch.append(key)
            if len(batch) >= 1000:
                self._client.delete(*batch)
                batch = []
        if batch:
            self._client.delete(*batch)

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception:
            return False

    #: 故意不实现 ``__len__``:整个实例的 key 数不是「本命名空间的块数」,
    #: 而 SCAN 全量在健康检查里太贵。要看块数就直接查 Redis。

    def __len__(self) -> int:
        return int(self._client.dbsize())


# ---------------------------------------------------------------------------
# 构造
# ---------------------------------------------------------------------------

def make_store(url: str = "", ttl: Optional[int] = None) -> ChainStore:
    """按 URL 构造存储。

    ``redis://...``  → Redis 链式存储(生产,唯一后端)
    ``""``           → 进程内实现(仅测试,不落盘、不跨进程)
    """
    if url.startswith("redis://") or url.startswith("rediss://"):
        import redis  # 可选依赖

        client = redis.Redis.from_url(url, decode_responses=False)
        return RedisChainStore(client, ttl=ttl)
    return MemoryChainStore()


def text_blocks(text: bytes, block_size: int) -> List[Tuple[bytes, bytes]]:
    """把一段文本切满块并算哈希(测试/冷启动用)。"""
    from .chunking import block_hash

    out: List[Tuple[bytes, bytes]] = []
    parent = b""
    for s in range(0, len(text) - len(text) % block_size, block_size):
        block = text[s:s + block_size]
        bid = block_hash(block, parent)
        out.append((bid, block))
        parent = bid
    return out
