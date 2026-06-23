"""Merkle 前缀链(移植自 Lua merkle.lua,设计文档 §3.4)。

H(prefix_0) = "0"
H(prefix_k) = sha256_hex16( H(prefix_{k-1}) + "|" + seg_hash_k )
"""

from __future__ import annotations

from typing import List

from .hashing import encode_message, sha256_hex16

EMPTY_HASH = "0"  # 空前缀固定哈希


def segment_hash(seg: List[dict]) -> str:
    """一个 segment 的哈希。段内消息用 encode_message 编码后空分隔拼接。"""
    blob = "".join(encode_message(m) for m in seg)
    return sha256_hex16(blob)


def chain_step(prev_hash: str, seg_hash: str) -> str:
    """单步推进:H(prefix_k) = sha256_hex16(prev + "|" + seg_hash)。"""
    return sha256_hex16(prev_hash + "|" + seg_hash)


def chain_hash(seg_hashes: List[str]) -> str:
    """从一组 segment hash 计算链式前缀哈希(到末尾)。空列表返回 "0"。"""
    cur = EMPTY_HASH
    for sh in seg_hashes:
        cur = chain_step(cur, sh)
    return cur


def build_nodes(seg_hashes: List[str]) -> List[dict]:
    """构建完整 Merkle 链节点。

    返回 [{"pfx_hash":..., "node": {"prev":..., "seg_ref":..., "seg_count":...}}, ...]。
    """
    nodes = []
    prev_hash = EMPTY_HASH
    for i, sh in enumerate(seg_hashes, start=1):
        cur_hash = chain_step(prev_hash, sh)
        nodes.append({
            "pfx_hash": cur_hash,
            "node": {"prev": prev_hash, "seg_ref": sh, "seg_count": i},
        })
        prev_hash = cur_hash
    return nodes
