"""传输层 KV Cache 优化协议 —— 常量与协议层工具。

定义客户端 SDK 与网关之间协商用的自定义 HTTP 头部、默认缓存策略,
以及带随机抖动的缓存过期时间(用于缓解缓存雪崩)。

对应设计文档:
  - 第 4 章「协议设计」(请求/响应头部表)
  - 第 5.4 节「降级与容错」
  - 第 9 章「风险评估」中「缓存雪崩(大量哈希同时过期)」的缓解措施
"""

from __future__ import annotations

import random
import time

# ---------------------------------------------------------------------------
# 请求方向(Client -> Gateway)的自定义头部
# ---------------------------------------------------------------------------
# 可选。客户端上次从响应里收到的缓存哈希。
HEADER_CACHE_HASH = "X-Cache-Hash"
# 可选。X-Cache-Hash 对应的前缀消息条数,用于辅助网关校验。
HEADER_CACHE_PREFIX_LENGTH = "X-Cache-Prefix-Length"

# ---------------------------------------------------------------------------
# 响应方向(Gateway -> Client)的自定义头部
# ---------------------------------------------------------------------------
# 服务器为本次请求前缀生成的唯一哈希,客户端应保存并在下次请求携带。
HEADER_RESP_CACHE_HASH = "X-Cache-Hash"
# Unix 时间戳(秒)。预估的缓存过期时间。
HEADER_RESP_CACHE_EXPIRE = "X-Cache-Expire"
# true=本次请求网关层缓存命中,false=未命中。
HEADER_RESP_CACHE_HIT = "X-Cache-Hit"

# ---------------------------------------------------------------------------
# 默认缓存策略
# ---------------------------------------------------------------------------
# 与服务端 KV Cache 策略对齐(数小时)。
DEFAULT_CACHE_TTL_SECONDS = 6 * 3600
# 过期时间随机抖动范围(±),防雪崩。
DEFAULT_TTL_JITTER_SECONDS = 600
# 本地缓存条目上限(LRU 淘汰)。
DEFAULT_MAX_CACHE_ENTRIES = 10000

_TRUE = "true"
_FALSE = "false"


def bool_header(value: bool) -> str:
    """把布尔值序列化为 HTTP 头部字符串。"""
    return _TRUE if value else _FALSE


def parse_cache_hit(value: str | None) -> bool:
    """解析响应头 X-Cache-Hit 为布尔值;缺失或无法识别时返回 False。"""
    if value is None:
        return False
    return value.strip().lower() == _TRUE


def compute_expire(
    now: float | None = None,
    ttl: int = DEFAULT_CACHE_TTL_SECONDS,
    jitter: int = DEFAULT_TTL_JITTER_SECONDS,
) -> int:
    """计算缓存过期 Unix 时间戳(秒),加入 [-jitter, +jitter] 的随机抖动。

    对应设计文档第 9 章「设置随机抖动过期时间」缓解缓存雪崩。
    """
    if now is None:
        now = time.time()
    delta = random.randint(-jitter, jitter) if jitter > 0 else 0
    return int(now + ttl + delta)
