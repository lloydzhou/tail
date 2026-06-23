"""Tail —— 传输层 KV Cache 优化系统。

两个核心组件:
  - openai_patch:  openai 官方 SDK 的 monkey patch(客户端,用户直接用)
  - protocol:      协议头常量(供参考,patch 内部已自包含)

辅助算法库(供参考 / 未来移植 Lua 用):
  - hashing:   前缀哈希(token 版 + 字符串版)
  - segment:   按 LLM 回合切分 messages(m·n=0 约束)
  - merkle:    Merkle 前缀链(增量 hash)

服务端实现见 openresty/lua/kvcache/(OpenResty + Kvrocks)。
"""

from . import openai_patch, protocol
from .hashing import compute_prefix_hash, messages_hash

__all__ = [
    "openai_patch",
    "protocol",
    "compute_prefix_hash",
    "messages_hash",
]

__version__ = "2.1.0"
