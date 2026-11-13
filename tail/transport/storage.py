"""块存储:内容寻址的扁平块集合。

设计要点(与原 v2.1 链式方案的本质区别)
--------------------------------------
原方案存的是**链**:`pfx:{hash} → [segment hashes...]`,读取时要顺着链往下走,
因此需要维护 prev 指针、引用计数、访问驱动续期,以及"链在块在"的不变量。

新方案存的是**扁平块集合**::

    blk:{block_id} → 块字节

**顺序不存进存储,而是由请求里的帧序列直接表达。** 一次请求的块顺序在
帧流里是显式的,网关边解边拼即可。因此:

  - 不需要链、不需要 prev 指针
  - 不需要引用计数
  - 不需要访问驱动续期
  - 不需要缺块补发协议
  - 不存在"部分命中"概念——缺块就退化为直传

淘汰策略:交给存储自身的 LRU / TTL,网关不管。

后端
----
  - :class:`DbmStorage`  零依赖,Python 标准库 dbm。单机/开发默认。
  - :class:`RedisStorage` 支持 MGET 批量取块,一次往返取回全部引用块。生产用。

两者接口一致,由 :class:`BlockSource` 协议约束。
"""

from __future__ import annotations

import dbm
import os
import time
from typing import Dict, Iterable, List, Optional

from .chunking import BLOCK_ID_SIZE, block_id
from .codec import BlockSource

#: 块 key 前缀
KEY_PREFIX = b"blk:"


def _key(bid: bytes) -> bytes:
    return KEY_PREFIX + bid


class DbmStorage(BlockSource):
    """基于 Python 标准库 ``dbm`` 的块存储。零依赖。

    适合单机部署与开发调试。并发写依赖底层 dbm 实现(通常带文件锁),
    不适合多进程高并发场景。
    """

    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._db = dbm.open(path, "c")

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "DbmStorage":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- BlockSource ------------------------------------------------------

    def get_many(self, block_ids: List[bytes]) -> Dict[bytes, bytes]:
        out: Dict[bytes, bytes] = {}
        for bid in block_ids:
            try:
                val = self._db.get(_key(bid))
            except KeyError:
                continue
            if val is not None:
                out[bid] = bytes(val)
        return out

    # -- 写入 -------------------------------------------------------------

    def put_many(self, blocks: Dict[bytes, bytes]) -> None:
        for bid, content in blocks.items():
            self._db[_key(bid)] = content

    def put_text(self, text: bytes) -> int:
        """把一段文本切块后全部写入。返回写入的块数。"""
        from .chunking import split_blocks
        n = 0
        for chunk in split_blocks(text):
            self._db[_key(block_id(chunk))] = chunk
            n += 1
        return n

    def __len__(self) -> int:
        return sum(1 for k in self._db.keys() if k.startswith(KEY_PREFIX))

    def clear(self) -> None:
        for k in list(self._db.keys()):
            if k.startswith(KEY_PREFIX):
                del self._db[k]

    def ping(self) -> bool:
        try:
            self._db.get(KEY_PREFIX + b"\x00" * BLOCK_ID_SIZE, None)
            return True
        except Exception:
            return False


class RedisStorage(BlockSource):
    """基于 Redis 的块存储。支持 MGET 批量取块。

    ``redis`` 是可选依赖:没有安装时 :class:`DbmStorage` 仍然可用。

    注意:Redis 是单线程的,不要在 Lua 脚本里做「取块 + 拼接」这类大操作,
    那会阻塞整个实例。本实现只做 MGET / SET 这类 O(1) 单键操作。
    """

    def __init__(self, client, ttl: Optional[int] = None,
                 namespace: str = "tail") -> None:
        """
        Args:
          client: ``redis.Redis`` 实例(同步客户端)
          ttl: 块过期秒数。None 表示交给 maxmemory-policy 淘汰
          namespace: key 前缀,便于多环境隔离
        """
        self._client = client
        self._ttl = ttl
        self._ns = namespace.encode() + b":"

    def _key(self, bid: bytes) -> bytes:
        return self._ns + _key(bid)

    def get_many(self, block_ids: List[bytes]) -> Dict[bytes, bytes]:
        if not block_ids:
            return {}
        keys = [self._key(bid) for bid in block_ids]
        values = self._client.mget(keys)
        return {bid: bytes(v) for bid, v in zip(block_ids, values) if v is not None}

    def put_many(self, blocks: Dict[bytes, bytes]) -> None:
        if not blocks:
            return
        pipe = self._client.pipeline(transaction=False)
        for bid, content in blocks.items():
            if self._ttl:
                pipe.setex(self._key(bid), self._ttl, content)
            else:
                pipe.set(self._key(bid), content)
        pipe.execute()

    def put_text(self, text: bytes) -> int:
        from .chunking import split_blocks
        blocks = {block_id(c): c for c in split_blocks(text)}
        self.put_many(blocks)
        return len(blocks)

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception:
            return False


class MemoryStorage(BlockSource):
    """进程内字典存储。测试用。

    带可选的容量上限与 LRU 淘汰,便于测试"块被淘汰后如何退化"。
    """

    def __init__(self, capacity: Optional[int] = None) -> None:
        self._data: Dict[bytes, bytes] = {}
        self._order: List[bytes] = []
        self._capacity = capacity

    def get_many(self, block_ids: List[bytes]) -> Dict[bytes, bytes]:
        out: Dict[bytes, bytes] = {}
        for bid in block_ids:
            if bid in self._data:
                out[bid] = self._data[bid]
                # 触摸:移到末尾
                try:
                    self._order.remove(bid)
                except ValueError:
                    pass
                self._order.append(bid)
        return out

    def put_many(self, blocks: Dict[bytes, bytes]) -> None:
        for bid, content in blocks.items():
            if bid not in self._data:
                self._order.append(bid)
            self._data[bid] = content
        while self._capacity is not None and len(self._data) > self._capacity:
            evict = self._order.pop(0)
            self._data.pop(evict, None)

    def put_text(self, text: bytes) -> int:
        from .chunking import split_blocks
        blocks = {block_id(c): c for c in split_blocks(text)}
        self.put_many(blocks)
        return len(blocks)

    def clear(self) -> None:
        self._data.clear()
        self._order.clear()

    def __len__(self) -> int:
        return len(self._data)

    def ping(self) -> bool:
        return True


def make_storage(url: str = "", **kw):
    """按 URL 构造存储后端。

    Args:
      url: ``""`` / ``"dbm:/path/blocks"`` / ``"redis://host:6379/0"``

    无 URL 时退化为内存存储(重启即丢,但仍能正确工作)。
    """
    if not url:
        return MemoryStorage()
    if url.startswith("dbm:"):
        return DbmStorage(url[4:])
    if url.startswith("redis://") or url.startswith("rediss://"):
        try:
            import redis  # 可选依赖
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "使用 redis 后端需要安装 redis 包: pip install redis"
            ) from exc
        client = redis.Redis.from_url(url)
        return RedisStorage(client, ttl=kw.get("ttl"), namespace=kw.get("namespace", "tail"))
    raise ValueError(f"不支持的存储 URL: {url}")
