"""前缀哈希算法。

提供两种哈希:
  - compute_prefix_hash(token_ids):基于 token id 序列(v1.0,需分词器)
  - messages_hash(messages):基于 messages 的字符串编码(v2.1,无需分词器,
    与 Lua hashing.lua / openai_patch._messages_digest 算法一致)

网关侧统一用 messages_hash(字符串版),不再依赖 tiktoken。
"""

from __future__ import annotations

import hashlib
from typing import Iterable, List

# 哈希长度:取 SHA256 十六进制的前 16 个字符(64 bit),文档建议值。
HASH_HEX_LENGTH = 16


def compute_prefix_hash(token_ids: Iterable[int]) -> str:
    """对一段 Token ID 序列计算稳定哈希(v1.0 接口,保留兼容)。

    每个 token id 编码为 8 字节大端序后拼接,再做 SHA256。
    """
    h = hashlib.sha256()
    for tid in token_ids:
        h.update(int(tid).to_bytes(8, "big", signed=False))
    return h.hexdigest()[:HASH_HEX_LENGTH]


def _encode_message(msg: dict) -> bytes:
    """单条 message 的稳定字节编码(防边界碰撞)。

    与 Lua hashing.lua.encode_message / openai_patch._messages_digest 完全一致,
    保证 SDK 与网关哈希对齐。定长长度前缀 + 分隔符,杜绝 "ab"+"c" 歧义。
    """
    role = str(msg.get("role", "")).encode("utf-8")
    content = msg.get("content", "")
    if isinstance(content, list):
        content = repr(content).encode("utf-8")
    else:
        content = str(content).encode("utf-8")
    return (
        len(role).to_bytes(4, "big") + role + b"\x00" +
        len(content).to_bytes(4, "big") + content + b"\x01"
    )


def messages_hash(messages: List[dict]) -> str:
    """对一段 messages 列表计算稳定哈希(字符串版,v2.1 默认)。

    与 Lua hashing.lua.prefix_hash 算法一致,无需分词器。
    空列表返回空前缀的固定哈希。
    """
    h = hashlib.sha256()
    if not messages:
        return h.hexdigest()[:HASH_HEX_LENGTH]  # 空前缀
    for msg in messages:
        h.update(_encode_message(msg))
    return h.hexdigest()[:HASH_HEX_LENGTH]
