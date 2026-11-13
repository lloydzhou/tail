"""块存储:链式块(v3)。

v3 的核心变化:**链和内容在同一个 value 里**

    blk:{block_hash_hex} → parent_hex(32 ASCII) ‖ content(≤512 B)

因为 ``block_hash = blake2b(parent_hash ‖ block)[:16]``,所以「链尾摘要」等价于
「整条前缀的入口」:给定 tip 和前缀块数,可以从后往前走回第一块。

与 vLLM 的关系:vLLM 的 ``block_hash = hash(parent_hash, block)`` 一模一样;
差异只在链放在哪——vLLM 放在进程内存的 block table 里,这里放进存储,
好让多个网关实例共享,也不必重启即丢。

为什么不再需要单独的索引 key:链本身就是索引。v2 把「扁平集合 + 帧序列表达顺序」
作为设计亮点,那是因为顺序信息当时由帧序列携带;v3 没有帧序列了,顺序无处可去,
链是它的唯一载体。

API(所有后端一致)
------------------
    put_chain(parent, [(block_hash, content), ...]) -> 新的链尾
    resolve(tip, n_blocks)  -> 从前到后的 block_hash 列表,或 None(前缀不完整)
    get_many([block_hash, ...]) -> {block_hash: content}

读写都用 Lua,保证原子性:

  - **写**:`put_chain` 用一个脚本一次写完整串块(连同 TTL)。否则网关在脚本
    只写了一半时就把新 tip 告诉客户端,对端拿到的是一个悬挂的链——读的时候
    `resolve` 返回 None,表现为「刚发过的前缀立刻 miss」。脚本是原子的,
    要么整串可见,要么全不可见。
  - **读**:`resolve` 用一个脚本完成「走链 + 整条续期」。per-block TTL 下,
    读和续期必须是同一件事,否则出现「读到一半某块过期」的中间态。
  - `get_many` 是单条 MGET,本身原子;它与 `resolve` 之间隔了一次往返,
    期间若有块过期,表现为该前缀 miss(安全地退化成全量重发),不会拼出错前缀。
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Dict, List, Optional, Sequence, Tuple

#: 块 key 前缀
KEY_PREFIX = "blk:"

#: parent 在 value 里的编码长度(16 字节哈希的 hex)
_PARENT_HEX = 32

#: 链根的父哈希。必须**定长**:value 是 ``parent_hex(32) ‖ content``,
#: 早先把根写成空串,读回来时 ``v[32:]`` 会吃掉根块正文的前 32 字节。
_ZERO_PARENT = b"\x00" * 16


def _encode_parent(parent: bytes) -> bytes:
    return (parent or _ZERO_PARENT).hex().encode("ascii")


class ChainStore:
    """链式块存储接口。"""

    def put_chain(self, parent: bytes,
                  items: Sequence[Tuple[bytes, bytes]]) -> bytes:
        """按顺序写入一串块,第一块的父哈希是 ``parent``。返回新的链尾。"""
        raise NotImplementedError

    def resolve(self, tip: bytes, n_blocks: int) -> Optional[List[bytes]]:
        """从链尾走 ``n_blocks`` 步,返回**从前到后**的 block_hash 列表。

        前缀不完整(中间有块被淘汰/清空)时返回 ``None``。
        """
        raise NotImplementedError

    def get_many(self, ids: Sequence[bytes]) -> Dict[bytes, bytes]:
        """批量取块内容。缺失的 id 不出现在结果里。"""
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

    def resolve(self, tip: bytes, n_blocks: int) -> Optional[List[bytes]]:
        if n_blocks <= 0:
            return []
        out: List[bytes] = []
        cur = tip
        for _ in range(n_blocks):
            item = self._data.get(cur)
            if item is None:
                return None
            out.append(cur)
            cur = item[0]
        out.reverse()
        return out

    def get_many(self, ids: Sequence[bytes]) -> Dict[bytes, bytes]:
        out: Dict[bytes, bytes] = {}
        for bid in ids:
            item = self._data.get(bid)
            if item is not None:
                out[bid] = item[1]
                self._data.move_to_end(bid)
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

#: 走链 + 续期,一次往返、原子。
#: KEYS[1]=key 前缀("blk:"),ARGV[1]=tip hex,ARGV[2]=块数,ARGV[3]=TTL 秒
#: 返回从链尾到链头的 hex 列表;前缀不完整返回空表。
_RESOLVE_LUA = """
local prefix = KEYS[1]
local cur = ARGV[1]
local n = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local ids = {}
for i = 1, n do
  local k = prefix .. cur
  local v = redis.call('GET', k)
  if not v then return {} end
  if ttl > 0 then redis.call('EXPIRE', k, ttl) end
  ids[#ids + 1] = cur
  cur = string.sub(v, 1, 32)
end
return ids
"""


class RedisChainStore(ChainStore):
    """Redis 链式存储(生产)。

    value = parent_hex(32 B) ‖ content。用 hex 而不是裸 16 字节省在 Lua 里少一次
    编解码;代价是每块多 16 B(约 3%),换来的是脚本能直接用 ``string.sub`` 走链。
    """

    def __init__(self, client, ttl: Optional[int] = None,
                 namespace: str = "tail") -> None:
        """
        Args:
          client: ``redis.Redis`` 同步客户端
          ttl: 每块的过期秒数;``resolve`` 时会顺带把整条前缀续到这个值
          namespace: key 前缀,便于多环境隔离
        """
        self._client = client
        self._ttl = int(ttl or 0)
        self._ns = namespace + ":"
        self._resolve = client.register_script(_RESOLVE_LUA)
        self._put_chain = client.register_script(_PUT_CHAIN_LUA)

    def _key(self, bid: bytes) -> str:
        return self._ns + KEY_PREFIX + bid.hex()

    def put_chain(self, parent: bytes,
                  items: Sequence[Tuple[bytes, bytes]]) -> bytes:
        """用**一个 Lua 脚本**原子写入整串块。

        不能用 pipeline:非事务管线只保证「按顺序发」,中途出错(或进程被杀)会留下
        半条链——而网关随后会把那段前缀的 tip 告诉客户端,对端下一次拿着一个
        悬挂的 tip 来,`resolve` 返回 None,表现为「刚发过的内容立刻 miss」。
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

    def resolve(self, tip: bytes, n_blocks: int) -> Optional[List[bytes]]:
        """走链:一个 Lua 脚本、一次往返,顺带把整条前缀续到 TTL。"""
        if n_blocks <= 0:
            return []
        raw = self._resolve(keys=[self._ns + KEY_PREFIX],
                            args=[tip.hex(), n_blocks, self._ttl])
        if not raw or len(raw) != n_blocks:
            return None
        out = [bytes.fromhex(i.decode() if isinstance(i, bytes) else i)
               for i in raw]
        out.reverse()
        return out

    def get_many(self, ids: Sequence[bytes]) -> Dict[bytes, bytes]:
        if not ids:
            return {}
        keys = [self._key(bid) for bid in ids]
        values = self._client.mget(keys)
        out: Dict[bytes, bytes] = {}
        for bid, v in zip(ids, values):
            if v is not None:
                out[bid] = bytes(v)[_PARENT_HEX:]
        return out

    def clear(self) -> None:
        # 只删本命名空间下的块:SCAN 而不是 KEYS,避免阻塞
        pattern = self._ns + KEY_PREFIX + "*"
        batch = []
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
