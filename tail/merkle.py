"""Merkle 前缀链 —— messages 段的增量 hash 链。

对应设计文档 §3.4。

核心定义:
    H(prefix_0) = "0"                                  # 空(0 个 segment)
    H(prefix_k) = sha256_hex16( H(prefix_{k-1}) || seg_hash_k )

其中 seg_hash_k = sha256_hex16(stable_encode(segment_k))。

性质:
    - H(prefix_k) 只依赖 H(prefix_{k-1}) 和 segment_k,无需知道前面 k-1 段全文
    - 加一个 segment = 算 1 个 hash,无需重算前面

节点结构(存 KV):
    { prev: H(prefix_{k-1}), seg_ref: seg_hash_k, seg_count: k }
"""

from __future__ import annotations

import hashlib
from typing import List, Optional

from .segment import split_segments

EMPTY_HASH = "0"  # 空前缀的固定哈希


def _sha256_hex16(data: bytes) -> str:
    """SHA256 取前 16 hex(与 Lua hashing.lua / openai_patch._messages_digest 一致)。"""
    return hashlib.sha256(data).hexdigest()[:16]


def stable_encode_message(msg: dict) -> bytes:
    """单条 message 的稳定字节编码(防边界碰撞)。

    与 tail.openai_patch._messages_digest 算法一致,保证 SDK/网关哈希对齐。
    """
    role = str(msg.get("role", "")).encode("utf-8")
    content = msg.get("content", "")
    if isinstance(content, list):
        content = repr(content).encode("utf-8")
    else:
        content = str(content).encode("utf-8")
    parts = [
        len(role).to_bytes(4, "big"), role, b"\x00",
        len(content).to_bytes(4, "big"), content, b"\x01",
    ]
    return b"".join(parts)


def segment_hash(segment: List[dict]) -> str:
    """一个 segment 的哈希。"""
    h = hashlib.sha256()
    for m in segment:
        h.update(stable_encode_message(m))
    return h.hexdigest()[:16]


def messages_prefix_hash(messages: List[dict]) -> str:
    """完整 messages 的前缀哈希(链式累积到末尾)。

    用于:网关收到完整 messages 后,计算其对应的 pfx_hash。
    """
    segments = split_segments(messages)
    return chain_hash([segment_hash(s) for s in segments])


def chain_hash(seg_hashes: List[str]) -> str:
    """从一组 segment hash 计算链式前缀哈希。

    >>> chain_hash([]) == "0"
    True
    """
    cur = EMPTY_HASH
    for sh in seg_hashes:
        cur = _sha256_hex16(cur.encode("ascii") + b"||" + sh.encode("ascii"))
    return cur


def chain_step(prev_hash: str, seg_hash_value: str) -> str:
    """单步推进:H(prefix_{k}) = sha256_hex16(H(prefix_{k-1}) || seg_hash_k)。"""
    return _sha256_hex16(prev_hash.encode("ascii") + b"||" + seg_hash_value.encode("ascii"))


# ---------------------------------------------------------------------------
# 节点数据结构(存 KV 的 value)
# ---------------------------------------------------------------------------


class PrefixNode:
    """Merkle 链的一个节点(对应 KV 里 pfx:{hash} 的 value)。"""

    __slots__ = ("prev", "seg_ref", "seg_count")

    def __init__(self, prev: str, seg_ref: str, seg_count: int):
        self.prev = prev          # H(prefix_{k-1}),"0" 表根
        self.seg_ref = seg_ref    # 本节点的 segment hash
        self.seg_count = seg_count

    def to_dict(self) -> dict:
        return {"prev": self.prev, "seg_ref": self.seg_ref, "seg_count": self.seg_count}

    @classmethod
    def from_dict(cls, d: dict) -> "PrefixNode":
        return cls(d["prev"], d["seg_ref"], d["seg_count"])


def build_chain_nodes(seg_hashes: List[str]) -> List[tuple]:
    """从 segment hash 列表构建完整的 Merkle 链节点。

    返回 [(pfx_hash, PrefixNode), ...],pfx_hash 是该节点的 key(= 它代表的前缀的 hash)。
    每个节点的 prev 指向前一个节点的 pfx_hash(根为 "0")。

    用于:首次写入一个完整 messages 前缀时,一次性生成所有节点。
    """
    nodes = []
    prev_hash = EMPTY_HASH
    for i, sh in enumerate(seg_hashes, start=1):
        cur_hash = chain_step(prev_hash, sh)
        node = PrefixNode(prev=prev_hash, seg_ref=sh, seg_count=i)
        nodes.append((cur_hash, node))
        prev_hash = cur_hash
    return nodes


def next_node(prev_pfx_hash: str, seg_hash_value: str, prev_seg_count: int) -> tuple:
    """加一个 segment:返回 (new_pfx_hash, PrefixNode)。

    用于:对话增长一个回合,只新增一个节点(O(1) 写)。
    prev_seg_count:前一个前缀的 segment 数(调用方从 prev 节点查得)。
    """
    new_hash = chain_step(prev_pfx_hash, seg_hash_value)
    node = PrefixNode(prev=prev_pfx_hash, seg_ref=seg_hash_value, seg_count=prev_seg_count + 1)
    return new_hash, node
