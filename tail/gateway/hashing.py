"""哈希算法(移植自 Lua hashing.lua,保证跨实现哈希一致)。

关键:encode_message 的字节格式必须与 Lua 版逐字节一致,
否则 Python gateway 与 OpenResty gateway 产出的 cache_key 会对不上。
"""

from __future__ import annotations

import hashlib
import json
from typing import List

HASH_HEX_LENGTH = 16


def sha256_hex16(s: str) -> str:
    """SHA256 取前 16 个十六进制字符(64 bit)。等价 Lua hashing.sha256_hex16。"""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:HASH_HEX_LENGTH]


def _stable_content(content) -> str:
    """content 为 list/dict(多模态/工具)时,用固定字节序的 JSON 序列化。

    用 sort_keys + 紧凑分隔符,保证跨进程/跨语言稳定。
    Lua cjson 无 sort,但 Python 必须固定才能一致;此处是 Python 侧的规范。
    """
    if isinstance(content, (list, dict)):
        return json.dumps(content, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
    return str(content if content is not None else "")


def encode_message(msg: dict) -> str:
    """单条 message 的稳定字符串编码(等价 Lua hashing.encode_message)。

    字节布局:<role_len>:<role>\\x00<content_len>:<content>\\x01
    长度用字节长度(UTF-8 编码后),防边界碰撞。
    """
    role = str(msg.get("role", ""))
    content = _stable_content(msg.get("content", ""))
    # 字节长度(Lua #role 是字节长度)
    rl = len(role.encode("utf-8"))
    cl = len(content.encode("utf-8"))
    return f"{rl}:{role}\x00{cl}:{content}\x01"


def prefix_hash(messages: List[dict]) -> str:
    """对 messages 数组算整体 hash(等价 Lua hashing.prefix_hash)。

    空列表返回对字面量 "empty" 的 hash(与 Lua 一致)。
    """
    if not messages:
        return sha256_hex16("empty")
    blob = "".join(encode_message(m) for m in messages)
    return sha256_hex16(blob)
