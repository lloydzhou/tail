"""单元测试:协议层工具(头部序列化、过期时间计算)。

对应设计文档第 4 章与第 9 章。
"""

from tail import protocol


def test_bool_header_roundtrip():
    assert protocol.bool_header(True) == "true"
    assert protocol.bool_header(False) == "false"


def test_parse_cache_hit():
    assert protocol.parse_cache_hit("true") is True
    assert protocol.parse_cache_hit("TRUE") is True
    assert protocol.parse_cache_hit("false") is False
    assert protocol.parse_cache_hit(None) is False
    assert protocol.parse_cache_hit("garbage") is False


def test_compute_expire_is_future():
    now = 1_000_000
    expire = protocol.compute_expire(now=now, ttl=3600, jitter=0)
    assert expire == now + 3600


def test_compute_expire_jitter_within_bounds():
    now = 1_000_000
    for _ in range(200):
        e = protocol.compute_expire(now=now, ttl=100, jitter=10)
        assert now + 100 - 10 <= e <= now + 100 + 10


def test_header_constants_match_design_doc():
    """第 4 章头部表里的名称必须存在且拼写正确。"""
    assert protocol.HEADER_CACHE_HASH == "X-Cache-Hash"
    assert protocol.HEADER_CACHE_PREFIX_LENGTH == "X-Cache-Prefix-Length"
    assert protocol.HEADER_RESP_CACHE_HASH == "X-Cache-Hash"
    assert protocol.HEADER_RESP_CACHE_EXPIRE == "X-Cache-Expire"
    assert protocol.HEADER_RESP_CACHE_HIT == "X-Cache-Hit"
