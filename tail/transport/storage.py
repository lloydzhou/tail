"""块存储:链与正文分开放。

两个命名空间,都是纯 STRING:

    p:{block_hash_hex} → parent_hex(32 ASCII)     只有父哈希,不掺正文
    b:{block_hash_hex} → content(≤块大小)         只有正文

因为 ``block_hash = blake2b(parent_hash ‖ block)[:16]``,「链尾摘要」等价于
「整条前缀的入口」:给定 tip 和前缀块数,可以从后往前走回第一块。

**为什么分开**(v4 相对 v3 的改动):v3 把父哈希和正文拼在同一个 value 里,走链时
每一步都要把 544 B 的 value 搬进 Lua,而走链只需要 16 B 的父哈希——一块 829 KB
前缀就是白白多搬 887 KB。分开之后走链只搬 16 B/跳,正文由一条 MGET 批量取回,
按顺序拼接(拼接本身 <0.05 ms,从来不是瓶颈)。

两种 key 分开以后 TTL 必须**一起**续:走链时对 ``p:`` 用 ``GETEX``(读+续期一条命令),
对 ``b:`` 在同一脚本里补一次 ``EXPIRE``;写的时候两组 key 在同一个脚本里用同一个 TTL
写入。两把 key 的 TTL 因此永远同步,不存在「链还在、正文先死」的中间态;真出现某块
缺失(淘汰/清空),``resolve`` 返回 None ⇒ 网关回 409 让客户端重发,不会拼出错前缀。

API(所有后端一致)
------------------
    put_chain(parent, [(block_hash, content), ...]) -> 新的链尾
    resolve(tip, n_blocks)  -> 从前到后的 block_hash 列表,或 None(前缀不完整)
    get_many([block_hash, ...]) -> {block_hash: content}

读写都用 Lua,保证原子性:

  - **写**:`put_chain` 用一个脚本一次写完整串(两块 key + 同一个 TTL)。否则网关在
    只写了一半时就把新 tip 告诉客户端,对端拿到的是一个悬挂的链——读的时候
    `resolve` 返回 None,表现为「刚发过的前缀立刻 miss」。脚本原子,要么整串可见,
    要么全不可见。
  - **读**:`resolve` 用一个脚本完成「递归走链 + 批量续期」,一次往返。
  - `get_many` 是单条 MGET,本身原子;它与 `resolve` 之间隔了一次往返,
    期间若有块过期,表现为该前缀 miss(安全地退化成全量重发),不会拼出错前缀。

**递归必须在脚本里做**:每跳都依赖上一跳的结果,由网关逐跳发命令就是 N 次网络往返。
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Dict, List, Optional, Sequence, Tuple

#: 正文 key 前缀
KEY_BLOCK = "b:"

#: 父链 key 前缀
KEY_PARENT = "p:"

#: 链根的父哈希(hex 后是 32 个 "0")。定长,走链脚本才能直接拿 value 当下一跳。
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

#: 原子写入整串:正文 key 与父链 key 用同一个 TTL 一次写完。
#: KEYS      = [b:key_1..b:key_n, p:key_1..p:key_n]
#: ARGV[1]   = TTL 秒
#: ARGV[2..n+1]        = 正文(value)
#: ARGV[n+2..2n+1]     = 父哈希 hex(32 ASCII)
_PUT_LUA = """
local n = #KEYS / 2
local ttl = tonumber(ARGV[1])
for i = 1, n do
  if ttl > 0 then
    redis.call('SET', KEYS[i], ARGV[i + 1], 'EX', ttl)
    redis.call('SET', KEYS[n + i], ARGV[n + 1 + i], 'EX', ttl)
  else
    redis.call('SET', KEYS[i], ARGV[i + 1])
    redis.call('SET', KEYS[n + i], ARGV[n + 1 + i])
  end
end
return n
"""

#: 递归走链 + 批量续期,一次往返、原子。
#: KEYS[1] = "p:" 前缀,KEYS[2] = "b:" 前缀
#: ARGV[1] = tip hex,ARGV[2] = 块数,ARGV[3] = TTL 秒
#: 返回从链尾到链头的 hex 列表;前缀不完整返回空表。
#:
#: ``GETEX`` 读父链的同时把 ``p:`` 续到 TTL(一条命令,不额外花一次调用);
#: ``b:`` 没有 MGETEX,所以同一脚本里补一次 ``EXPIRE``——两条 key 的 TTL 因此始终
#: 一起走,不会出现「链还在、正文先过期」的中间态。
_WALK_LUA = """
local pk = KEYS[1]
local bk = KEYS[2]
local cur = ARGV[1]
local n = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local out = {}
for i = 1, n do
  local pkey = pk .. cur
  local v
  if ttl > 0 then
    v = redis.call('GETEX', pkey, 'EX', ttl)
  else
    v = redis.call('GET', pkey)
  end
  if not v then return {} end
  if ttl > 0 then redis.call('EXPIRE', bk .. cur, ttl) end
  out[#out + 1] = cur
  cur = v
end
return out
"""


class RedisChainStore(ChainStore):
    """Redis 链式存储(生产唯一后端)。

    两个命名空间各一个 key:``p:{hash}`` 只存父哈希(32 B hex),``b:{hash}``
    只存正文。走链只搬 16 字节/跳,正文一条 MGET 批量取回。

    所有 key 用 hex 而不是裸 16 字节:脚本里可以直接 ``string.sub``/拼接,
    读回来的 id 也能直接当 key 用,省两轮编解码;代价是每块多 32 B 的 key 与
    value 长度,相对正文可以忽略。
    """

    def __init__(self, client, ttl: Optional[int] = None,
                 namespace: str = "tail") -> None:
        """
        Args:
          client: ``redis.Redis`` 同步客户端
          ttl: key 的过期秒数;``resolve`` 读的时候会把链和正文一起续到这个值
          namespace: key 前缀,便于多环境隔离
        """
        self._client = client
        self._ttl = int(ttl or 0)
        self._ns = namespace + ":"
        self._walk = client.register_script(_WALK_LUA)
        self._put_chain = client.register_script(_PUT_LUA)

    # -- key ---------------------------------------------------------------
    def _bkey(self, bid: bytes) -> str:
        return self._ns + KEY_BLOCK + bid.hex()

    def _pkey(self, bid: bytes) -> str:
        return self._ns + KEY_PARENT + bid.hex()

    # -- 写 ---------------------------------------------------------------
    def put_chain(self, parent: bytes,
                  items: Sequence[Tuple[bytes, bytes]]) -> bytes:
        """用**一个 Lua 脚本**原子写入整串:正文 + 父链 + 同一个 TTL。

        不能用 pipeline:非事务管线只保证「按顺序发」,中途出错(或进程被杀)会留下
        半条链——而网关随后会把那段前缀的 tip 告诉客户端,对端下一次拿着一个
        悬挂的 tip 来,`resolve` 返回 None,表现为「刚发过的内容立刻 miss」。
        脚本内写完才返回,链要么整串可见,要么全不可见。

        块数很多时调用方要自己分批(每批几百块),避免脚本长时间占住实例。
        """
        if not items:
            return parent
        bkeys: List[str] = []
        pkeys: List[str] = []
        contents: List[bytes] = []
        parents: List[bytes] = []
        tip = parent
        for bid, content in items:
            bkeys.append(self._bkey(bid))
            pkeys.append(self._pkey(bid))
            contents.append(content)
            parents.append(_encode_parent(tip))
            tip = bid
        self._put_chain(keys=[*bkeys, *pkeys],
                        args=[self._ttl, *contents, *parents])
        return tip

    # -- 读 ---------------------------------------------------------------
    def resolve(self, tip: bytes, n_blocks: int) -> Optional[List[bytes]]:
        """递归走链:一个 Lua 脚本、一次往返,链和正文的 TTL 一起续。"""
        if n_blocks <= 0:
            return []
        raw = self._walk(keys=[self._ns + KEY_PARENT, self._ns + KEY_BLOCK],
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
        values = self._client.mget([self._bkey(bid) for bid in ids])
        return {bid: bytes(v) for bid, v in zip(ids, values) if v is not None}

    def clear(self) -> None:
        # 只删本命名空间下的 key:SCAN 而不是 KEYS,避免阻塞
        for prefix in (KEY_BLOCK, KEY_PARENT):
            pattern = self._ns + prefix + "*"
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
