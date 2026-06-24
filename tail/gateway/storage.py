"""Storage 抽象 + DbmStorage 默认实现。

对齐 OpenResty store.lua 的接口,保证 Python gateway 与 OpenResty gateway
可互换(同样的 messages → 同样的 cache_key,存储格式一致)。

抽象基类 Storage 定义 7 个方法;DbmStorage 用标准库 dbm 实现(零依赖)。
未来加 RedisStorage 只需继承基类。
"""

from __future__ import annotations

import dbm
import json
import os
import threading
import time
from abc import ABC, abstractmethod
from typing import List, Optional

from . import merkle, segment
from .hashing import sha256_hex16
from .protocol import GatewayConfig

NULL_HASH = "0"  # 缺失段(system/tools)占位,等同 merkle.EMPTY_HASH


class Storage(ABC):
    """存储抽象基类(对齐 store.lua 接口)。"""

    NULL_HASH = NULL_HASH

    @abstractmethod
    def get_meta(self, cache_key: str) -> Optional[dict]:
        """读 meta。软过期或不存在返回 None。"""

    @abstractmethod
    def get_segment_field(self, kind: str, hash_value: str) -> Optional[str]:
        """读 system/tools 全文。kind ∈ {"sys","tools"};NULL_HASH 返回 None。"""

    @abstractmethod
    def reconstruct(self, pfx_hash: str) -> Optional[List[dict]]:
        """沿 Merkle prev 链回溯还原 messages。链断/损坏返回 None。"""

    @abstractmethod
    def put_request(self, request: dict, expire_at: float) -> str:
        """写三段(sys/tools/seg/pfx)+ meta,返回 cache_key。"""

    @abstractmethod
    def ping(self) -> bool:
        """健康检查。"""

    @abstractmethod
    def clear(self) -> None:
        """清空(测试/运维用)。"""

    # ---------------- 共享算法(子类复用)----------------
    @staticmethod
    def compute_cache_key(request: dict) -> str:
        """纯算三段 hash 拼 cache_key(无需存储)。

        与 Lua gateway.compute_cache_key 一致:
          sys_hash = system ? sha256(tostring(system)) : "0"
          tools_hash = tools ? sha256(json(tools, sort_keys)) : "0"
          pfx_hash = chain_hash([segment_hash(s) for s in split(messages)])
          return f"{sys_hash}::{tools_hash}::{pfx_hash}"
        """
        sys_hash = NULL_HASH
        tools_hash = NULL_HASH
        if request.get("system") is not None:
            sys_hash = sha256_hex16(str(request["system"]))
        if request.get("tools") is not None:
            tools_blob = json.dumps(request["tools"], separators=(",", ":"),
                                    sort_keys=True, ensure_ascii=False)
            tools_hash = sha256_hex16(tools_blob)
        segs = segment.split(request.get("messages", []))
        seg_hashes = [merkle.segment_hash(s) for s in segs]
        pfx_hash = merkle.chain_hash(seg_hashes)
        return f"{sys_hash}::{tools_hash}::{pfx_hash}"


class DbmStorage(Storage):
    """基于标准库 dbm 的存储(零依赖)。

    - value 统一包装为 {"v": ..., "expire_at": ...},实现 TTL 懒清理
    - key 格式与 Lua 完全一致:{ns}:{sys|tools|seg|pfx|meta}:{hash}
    - 并发写用 threading.Lock 保护(dbm.dumb 不支持并发写)
    - TTL 分级:meta=ttl, sys/tools/seg=ttl_stable, pfx=renew_ttl
    """

    def __init__(self, cfg: GatewayConfig, dbm_path: str):
        self.cfg = cfg
        self.dbm_path = dbm_path
        self._lock = threading.Lock()
        self._db = dbm.open(dbm_path, "c")

    def _k(self, kind: str, h: str) -> str:
        return f"{self.cfg.hash_ns}:{kind}:{h}"

    def _now(self) -> float:
        return time.time()

    def _get_raw(self, key: str):
        """读取并软过期检查。返回反序列化的 dict(含 v/expire_at)或 None。"""
        try:
            raw = self._db[key.encode("utf-8")]
        except KeyError:
            return None
        try:
            d = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None
        if d.get("expire_at", 0) <= self._now():
            return None  # 软过期
        return d

    def _set_with_ttl(self, key: str, value, ttl: int):
        """写入(带 TTL 包装)。value 会被放进 {"v": value, "expire_at": now+ttl}。"""
        blob = json.dumps({"v": value, "expire_at": self._now() + ttl},
                          ensure_ascii=False).encode("utf-8")
        with self._lock:
            self._db[key.encode("utf-8")] = blob

    def _setnx_with_ttl(self, key: str, value, ttl: int) -> bool:
        """只在不存在时写入(内容寻址去重)。返回是否实际写入。"""
        with self._lock:
            kb = key.encode("utf-8")
            if kb in self._db:
                # 已存在:刷新 TTL(reconstruct 续期等价)
                try:
                    d = json.loads(self._db[kb].decode("utf-8"))
                    d["expire_at"] = self._now() + ttl
                    self._db[kb] = json.dumps(d, ensure_ascii=False).encode("utf-8")
                except Exception:
                    pass
                return False
            blob = json.dumps({"v": value, "expire_at": self._now() + ttl},
                              ensure_ascii=False).encode("utf-8")
            self._db[kb] = blob
            return True

    def _renew(self, key: str, ttl: int):
        """刷新 TTL(访问驱动续期,§7.4)。"""
        with self._lock:
            kb = key.encode("utf-8")
            if kb in self._db:
                try:
                    d = json.loads(self._db[kb].decode("utf-8"))
                    d["expire_at"] = self._now() + ttl
                    self._db[kb] = json.dumps(d, ensure_ascii=False).encode("utf-8")
                except Exception:
                    pass

    # ============ 接口实现 ============

    def get_meta(self, cache_key: str) -> Optional[dict]:
        d = self._get_raw(self._k("meta", cache_key))
        if d is None:
            return None
        meta = d["v"]
        # 双层过期:① 包装层物理 TTL(_get_raw 已查);② meta 内逻辑 expire_at(put_request 传入)
        if meta.get("expire_at", 0) <= self._now():
            return None  # 逻辑过期
        return meta

    def get_segment_field(self, kind: str, hash_value: str) -> Optional[str]:
        if hash_value == NULL_HASH:
            return None
        d = self._get_raw(self._k(kind, hash_value))
        return d["v"] if d else None

    def reconstruct(self, pfx_hash: str) -> Optional[List[dict]]:
        """沿 prev 链回溯 + 批量取 seg + flatten。

        回溯时对每个 pfx 节点续期(§7.4)。
        """
        if pfx_hash == merkle.EMPTY_HASH:
            return []
        # 1. 回溯收集 seg_ref,续期 pfx
        seg_refs: List[str] = []
        cur = pfx_hash
        seen = set()
        while cur != merkle.EMPTY_HASH:
            if cur in seen:
                return None  # 防环
            seen.add(cur)
            d = self._get_raw(self._k("pfx", cur))
            if d is None:
                return None  # 链断
            node = d["v"]
            self._renew(self._k("pfx", cur), self.cfg.renew_ttl)
            seg_refs.append(node["seg_ref"])
            cur = node["prev"]
        seg_refs.reverse()
        # 2. 批量取 seg,flatten
        messages: List[dict] = []
        for sh in seg_refs:
            d = self._get_raw(self._k("seg", sh))
            if d is None:
                return None  # 某 segment 缺失
            seg_msgs = d["v"]
            messages.extend(seg_msgs)
        return messages

    def put_request(self, request: dict, expire_at: float) -> str:
        """写三段 + meta,返回 cache_key。"""
        cfg = self.cfg
        sys_hash = NULL_HASH
        tools_hash = NULL_HASH

        system = request.get("system")
        if system is not None:
            sys_hash = sha256_hex16(str(system))
            self._setnx_with_ttl(self._k("sys", sys_hash), str(system), cfg.ttl_stable)

        tools = request.get("tools")
        if tools is not None:
            tools_blob = json.dumps(tools, separators=(",", ":"), sort_keys=True,
                                    ensure_ascii=False)
            tools_hash = sha256_hex16(tools_blob)
            self._setnx_with_ttl(self._k("tools", tools_hash), tools_blob, cfg.ttl_stable)

        messages = request.get("messages", [])
        segs = segment.split(messages)
        seg_hashes = []
        for s in segs:
            sh = merkle.segment_hash(s)
            seg_hashes.append(sh)
            self._setnx_with_ttl(self._k("seg", sh), s, cfg.ttl_stable)

        nodes = merkle.build_nodes(seg_hashes)
        for n in nodes:
            self._setnx_with_ttl(self._k("pfx", n["pfx_hash"]), n["node"], cfg.renew_ttl)
        pfx_hash = nodes[-1]["pfx_hash"] if nodes else merkle.EMPTY_HASH

        cache_key = f"{sys_hash}::{tools_hash}::{pfx_hash}"
        meta = {
            "sys_hash": sys_hash, "tools_hash": tools_hash, "pfx_hash": pfx_hash,
            "len": len(messages), "expire_at": expire_at,
        }
        self._set_with_ttl(self._k("meta", cache_key), meta, cfg.ttl)
        # 强制刷盘:防止进程被杀(Ctrl+C / OOM)时最后一次写入停留在 dbm 内部缓冲区
        try:
            self._db.sync()
        except Exception:
            pass  # 部分 dbm 后端不支持 sync,忽略
        return cache_key

    def ping(self) -> bool:
        try:
            # dbm 总是可用的(本地文件)
            return True
        except Exception:
            return False

    def clear(self) -> None:
        """清空本命名空间。dbm 不支持前缀扫描,直接清空整个 db。"""
        with self._lock:
            self._db.close()
            # 删除文件重建
            for ext in ("", ".db", ".dir", ".dat", ".bak", ".pag"):
                p = self.dbm_path + ext
                if os.path.exists(p):
                    os.remove(p)
            self._db = dbm.open(self.dbm_path, "c")
