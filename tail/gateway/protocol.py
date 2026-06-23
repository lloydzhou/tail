"""协议常量与配置(移植自 Lua protocol.lua,设计文档第 4、9 章)。"""

from __future__ import annotations

import random
import time

# 请求方向 Header(Client -> Gateway)
HEADER_CACHE_HASH = "X-Cache-Hash"
HEADER_CACHE_PREFIX_LENGTH = "X-Cache-Prefix-Length"

# 响应方向 Header(Gateway -> Client)
HEADER_RESP_CACHE_HASH = "X-Cache-Hash"
HEADER_RESP_CACHE_EXPIRE = "X-Cache-Expire"
HEADER_RESP_CACHE_HIT = "X-Cache-Hit"

# 默认策略
DEFAULT_CACHE_TTL = 6 * 3600              # meta 入口 TTL(秒)
DEFAULT_TTL_JITTER = 600                  # ±秒,防雪崩
DEFAULT_RENEW_TTL = 30 * 60               # pfx 访问驱动续期 TTL(秒),§7.4
DEFAULT_STABLE_TTL = 24 * 3600            # sys/tools/seg 稳定内容 TTL(秒)
DEFAULT_HASH_NS = "prefix_cache"          # key 命名空间

# 缓存未命中处理模式
MISS_FAST_FAIL = "fast_fail"              # 默认:不转发,返回 422 由 SDK 重试
MISS_PASSTHROUGH = "passthrough"          # 文档字面:把当前 messages 当完整请求转发


def compute_expire(ttl: int = DEFAULT_CACHE_TTL, jitter: int = DEFAULT_TTL_JITTER,
                   now: float | None = None) -> int:
    """带 ±jitter 抖动的过期时间(Unix 秒)。防雪崩。"""
    if now is None:
        now = time.time()
    delta = random.randint(-jitter, jitter) if jitter > 0 else 0
    return int(now + ttl + delta)


class GatewayConfig:
    """网关运行时配置(从命令行参数构造)。"""

    def __init__(
        self,
        backend_url: str,
        *,
        miss_mode: str = MISS_FAST_FAIL,
        ttl: int = DEFAULT_CACHE_TTL,
        jitter: int = DEFAULT_TTL_JITTER,
        renew_ttl: int = DEFAULT_RENEW_TTL,
        ttl_stable: int = DEFAULT_STABLE_TTL,
        hash_ns: str = DEFAULT_HASH_NS,
    ):
        self.backend_url = backend_url.rstrip("/")
        self.miss_mode = miss_mode
        self.ttl = ttl
        self.jitter = jitter
        self.renew_ttl = renew_ttl
        self.ttl_stable = ttl_stable
        self.hash_ns = hash_ns
