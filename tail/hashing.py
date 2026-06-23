"""前缀哈希算法 —— 基于前缀消息的 Token 序列生成唯一标识。

对应设计文档第 5.2 节「哈希生成算法」:
  对请求中前缀部分(从第 0 条消息开始)的所有 Token ID 列表进行 SHA256,
  取前 16 个字符(64 bit)。具备稳定性和低冲突率。
"""

from __future__ import annotations

import hashlib
from typing import Iterable

# 哈希长度:取 SHA256 十六进制的前 16 个字符(64 bit),文档建议值。
HASH_HEX_LENGTH = 16


def compute_prefix_hash(token_ids: Iterable[int]) -> str:
    """对一段 Token ID 序列计算稳定哈希。

    实现要点(满足稳定性 / 低冲突率 / 平台无关):
      - 每个 token id 编码为 8 字节大端序后拼接,再做 SHA256;
        大端定长编码保证 ``[1, 2, 12]`` 与 ``[12, 12]`` 不会碰撞,
        也避免把 token id 转字符串引入的歧义。
      - 取前 ``HASH_HEX_LENGTH`` 个十六进制字符作为最终哈希。
    """
    h = hashlib.sha256()
    for tid in token_ids:
        h.update(int(tid).to_bytes(8, "big", signed=False))
    return h.hexdigest()[:HASH_HEX_LENGTH]
