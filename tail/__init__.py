"""Tail —— 传输层 KV Cache 优化系统。

客户端:openai 官方 SDK 的 monkey patch。
服务端:openresty/lua/kvcache/(OpenResty + Kvrocks)。

用法:
    from tail import openai_patch
    openai_patch.install()
    # 之后照常用 openai.OpenAI(...),自动启用前缀缓存协商
"""

from . import openai_patch

__all__ = ["openai_patch"]

__version__ = "2.1.0"
