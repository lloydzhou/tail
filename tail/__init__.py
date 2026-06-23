"""传输层 KV Cache 优化系统 —— 参考实现。

对应设计文档《传输层KV Cache优化系统设计文档 v1.0》。

组件:
  - protocol:  协议头常量与缓存策略(第 4、9 章)
  - hashing:   基于前缀 Token 序列的 SHA256 哈希(第 5.2 节)
  - tokenizer: tiktoken 分词器封装(第 5.2、Phase 1)
  - store:     LRU + TTL 前缀缓存存储(第 5.1 节)
  - gateway:   OpenAI 兼容网关(协商核心,第 3、5 章)
  - sdk:       带缓存协商的客户端 SDK(第 6 章)
  - backend:   模拟推理服务(测试/演示用,第 3 章「推理服务」)
"""

from . import protocol
from .backend import build_backend_app
from .gateway import build_app
from .hashing import compute_prefix_hash
from .sdk import AsyncCachedClient, CachedClient
from . import openai_patch
from .store import CacheEntry, PrefixCacheStore
from .kvrocks_store import KvrocksStore
from .chunked_store import ChunkedStore
from .tokenizer import Tokenizer

__all__ = [
    "protocol",
    "build_app",
    "build_backend_app",
    "CachedClient",
    "AsyncCachedClient",
    "PrefixCacheStore",
    "KvrocksStore",
    "CacheEntry",
    "Tokenizer",
    "compute_prefix_hash",
]

__version__ = "1.0.0"
