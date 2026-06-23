"""openai 官方 SDK 的透明 monkey patch。

对应设计文档第 6 章。目标:用户代码一行不改,现有 ``from openai import OpenAI``
代码加上缓存协商能力。

用法::

    from tail import openai_patch as kvcache
    kvcache.install()
    from openai import OpenAI
    client = OpenAI(base_url="http://localhost:8765/v1", api_key="sk-...")
    client.chat.completions.create(model="deepseek-chat", messages=[...])

实现策略(不依赖 SDK 内部私有结构,稳定):
  1. patch ``SyncAPIClient.request`` / ``AsyncAPIClient.request``:
     - 识别 ``/chat/completions`` 请求;
     - 切分前缀/增量,改写 ``options.json_data``;
     - 设置线程/协程局部标志 ``_flag``。
  2. patch ``httpx.Client.send`` / ``httpx.AsyncClient.send``:
     - 当标志为真,捕获响应 headers 到局部变量 ``_captured``;
     - send 其余行为完全不变。
  3. ``request`` patch 从 ``_captured`` 读 ``X-Cache-*`` 更新本地缓存,
     若 422 miss 则用全量重试。

这样 patch 只影响 openai 走 httpx 发 chat 请求的路径,零侵入其它 httpx 用户。
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
import time
from typing import Optional, Tuple

HEADER_CACHE_HASH = "X-Cache-Hash"
HEADER_CACHE_PREFIX_LENGTH = "X-Cache-Prefix-Length"
HEADER_RESP_CACHE_HASH = "X-Cache-Hash"
HEADER_RESP_CACHE_EXPIRE = "X-Cache-Expire"
HEADER_RESP_CACHE_HIT = "X-Cache-Hit"

CHAT_PATH_MARK = "/chat/completions"


# ---------------------------------------------------------------------------
# 本地缓存
# ---------------------------------------------------------------------------


class _CacheState:
    """SDK 本地缓存(每 (model, base_url, session) 一份)。

    v2.1 关键改动:不再存整个 prefix_messages 数组,改为存
    prefix_count + prefix_digest(指纹),既省内存又能在切分前校验前缀一致性。
    见设计文档 §5.3。
    """
    __slots__ = ("cache_hash", "expire_time", "prefix_count", "prefix_digest")

    def __init__(self) -> None:
        self.cache_hash: str = ""
        self.expire_time: float = 0.0
        self.prefix_count: int = 0
        self.prefix_digest: str = ""

    def is_valid(self, now: Optional[float] = None) -> bool:
        if not self.cache_hash:
            return False
        if now is None:
            now = time.time()
        return now < self.expire_time


def _messages_digest(messages: list) -> str:
    """计算一段 messages 的指纹(与网关侧 system/tools hash 同算法)。

    用于切分前校验 messages[:n] 是否与缓存的前缀一致(防 compact/编辑/重排)。
    """
    import hashlib
    h = hashlib.sha256()
    for m in messages:
        role = str(m.get("role", ""))
        content = m.get("content", "")
        if isinstance(content, list):
            content = repr(content)
        else:
            content = str(content)
        # 定长编码防边界碰撞(与 Lua hashing.lua 一致)
        h.update(len(role).to_bytes(4, "big"))
        h.update(role.encode("utf-8"))
        h.update(b"\x00")
        h.update(len(content).to_bytes(4, "big"))
        h.update(content.encode("utf-8"))
        h.update(b"\x01")
    return h.hexdigest()[:16]


class _CacheManager:
    """多 session 隔离的缓存管理器(设计文档 §5.4)。

    主路径用 contextvars(协程/async 友好),回退 threading.local(纯线程)。
    每个并发上下文一份独立的 _table,避免多 session 交叉污染。
    即使上下文未隔离(同线程串行跑多 session),_apply_increment 的指纹校验
    也会兜底:前缀不匹配 → 降级全量,不会出错。
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ctx = contextvars.ContextVar("tail_sessions", default=None)
        self._fallback = threading.local()  # 纯线程场景兜底

    def _table(self) -> dict:
        """取当前上下文的缓存表;没有则创建并绑定。"""
        tbl = self._ctx.get()
        if tbl is None:
            tbl = getattr(self._fallback, "table", None)
            if tbl is None:
                tbl = {}
                self._fallback.table = tbl
            # 同步给 contextvars(若在协程内)
            self._ctx.set(tbl)
        return tbl

    def get(self, key):
        tbl = self._table()
        with self._lock:
            s = tbl.get(key)
            if s is None:
                s = _CacheState()
                tbl[key] = s
            return s

    def invalidate(self, key):
        tbl = self._table()
        with self._lock:
            tbl.pop(key, None)

    def all_tables(self) -> list:
        """返回所有上下文的表(测试/调试用)。"""
        # contextvars 无法枚举所有上下文,这里只返回当前线程的 + 一个全局视图
        out = []
        t = self._ctx.get()
        if t is not None:
            out.append(t)
        ft = getattr(self._fallback, "table", None)
        if ft is not None and ft not in out:
            out.append(ft)
        return out


_cache_mgr = _CacheManager()

# 线程/任务局部:当前是否处于 kvcache chat 请求中 + 捕获到的响应头
_tls = threading.local()


def _flag() -> bool:
    return getattr(_tls, "in_chat", False)


def _set_flag(v: bool) -> None:
    _tls.in_chat = v


def _captured() -> Optional[dict]:
    return getattr(_tls, "captured_headers", None)


def _set_captured(h: Optional[dict]) -> None:
    _tls.captured_headers = h


def _norm_base(url) -> str:
    return str(url or "").rstrip("/")


def _is_chat_request(options) -> bool:
    url = getattr(options, "url", "") or ""
    return CHAT_PATH_MARK in str(url)


def _parse_cache_hit(value):
    return bool(value) and value.strip().lower() == "true"


# ---------------------------------------------------------------------------
# patch httpx send:捕获响应头(仅当处于 chat 请求标志为真)
# ---------------------------------------------------------------------------


def _patch_httpx_sync():
    import httpx
    if getattr(httpx.Client.send, "_kvcache_patched", False):
        return
    _orig_send = httpx.Client.send

    def _patched_send(self, request, **kwargs):
        if not _flag():
            return _orig_send(self, request, **kwargs)
        _set_captured(None)
        resp = _orig_send(self, request, **kwargs)
        try:
            _set_captured(dict(resp.headers))
        except Exception:
            pass
        return resp

    _patched_send._kvcache_patched = True
    httpx.Client.send = _patched_send


def _patch_httpx_async():
    import httpx
    if getattr(httpx.AsyncClient.send, "_kvcache_patched", False):
        return
    _orig_send = httpx.AsyncClient.send

    async def _patched_send(self, request, **kwargs):
        if not _flag():
            return await _orig_send(self, request, **kwargs)
        _set_captured(None)
        resp = await _orig_send(self, request, **kwargs)
        try:
            _set_captured(dict(resp.headers))
        except Exception:
            pass
        return resp

    _patched_send._kvcache_patched = True
    httpx.AsyncClient.send = _patched_send


# ---------------------------------------------------------------------------
# patch openai request
# ---------------------------------------------------------------------------


def _apply_increment(options, state, messages):
    """切分前缀/增量,改写 options。返回是否走了增量。

    v2.1:切分前校验 messages[:n] 的指纹 == 缓存的 prefix_digest。
    不一致(compact/编辑/重排)→ 放弃增量,返回 False(降级为发全量)。
    见设计文档 §5.3。
    """
    if not state.is_valid():
        return False
    n = state.prefix_count
    if n <= 0 or len(messages) < n:
        return False
    # 关键:指纹校验,而非仅长度
    if _messages_digest(messages[:n]) != state.prefix_digest:
        return False
    increment = messages[n:]
    new_json = dict(getattr(options, "json_data", None) or {})
    new_json["messages"] = increment
    options.json_data = new_json
    headers = dict(getattr(options, "headers", None) or {})
    headers[HEADER_CACHE_HASH] = state.cache_hash
    headers[HEADER_CACHE_PREFIX_LENGTH] = str(n)
    options.headers = headers
    return True


def _strip_cache_headers(options):
    headers = dict(getattr(options, "headers", None) or {})
    headers.pop(HEADER_CACHE_HASH, None)
    headers.pop(HEADER_CACHE_PREFIX_LENGTH, None)
    options.headers = headers


def _patch_sync():
    from openai._base_client import SyncAPIClient
    if getattr(SyncAPIClient.request, "_kvcache_patched", False):
        return
    _orig_request = SyncAPIClient.request

    def _patched_request(self, cast_to, options, *, stream=False, stream_cls=None):
        if not _is_chat_request(options):
            return _orig_request(self, cast_to, options, stream=stream, stream_cls=stream_cls)

        json_data = dict(getattr(options, "json_data", None) or {})
        model = json_data.get("model", "")
        messages = json_data.get("messages", []) or []
        base_url = _norm_base(getattr(self, "base_url", "") or "")
        key = (model, base_url)
        state = _cache_mgr.get(key)

        sent_increment = False
        _set_flag(True)
        try:
            sent_increment = _apply_increment(options, state, messages)
            resp = _orig_request(self, cast_to, options, stream=stream, stream_cls=stream_cls)
            captured = _captured()

            # miss → 全量重试
            if captured and {str(k).lower(): v for k,v in captured.items()}.get(HEADER_RESP_CACHE_HIT.lower(), "").lower() == "false" \
                    and sent_increment:
                _cache_mgr.invalidate(key)
                # 还原完整 messages
                options.json_data = json_data
                _strip_cache_headers(options)
                resp = _orig_request(self, cast_to, options, stream=stream, stream_cls=stream_cls)
                captured = _captured()
                sent_increment = False

            _update_state(key, state, captured, messages, sent_increment)
            return resp
        finally:
            _set_flag(False)
            _set_captured(None)

    _patched_request._kvcache_patched = True
    SyncAPIClient.request = _patched_request
    SyncAPIClient._kvcache_patched = True


def _patch_async():
    from openai._base_client import AsyncAPIClient
    if getattr(AsyncAPIClient.request, "_kvcache_patched", False):
        return
    _orig_request = AsyncAPIClient.request

    async def _patched_request(self, cast_to, options, *, stream=False, stream_cls=None):
        if not _is_chat_request(options):
            return await _orig_request(self, cast_to, options, stream=stream, stream_cls=stream_cls)

        json_data = dict(getattr(options, "json_data", None) or {})
        model = json_data.get("model", "")
        messages = json_data.get("messages", []) or []
        base_url = _norm_base(getattr(self, "base_url", "") or "")
        key = (model, base_url)
        state = _cache_mgr.get(key)

        sent_increment = False
        _set_flag(True)
        try:
            sent_increment = _apply_increment(options, state, messages)
            resp = await _orig_request(self, cast_to, options, stream=stream, stream_cls=stream_cls)
            captured = _captured()

            if captured and {str(k).lower(): v for k,v in captured.items()}.get(HEADER_RESP_CACHE_HIT.lower(), "").lower() == "false" \
                    and sent_increment:
                _cache_mgr.invalidate(key)
                options.json_data = json_data
                _strip_cache_headers(options)
                resp = await _orig_request(self, cast_to, options, stream=stream, stream_cls=stream_cls)
                captured = _captured()
                sent_increment = False

            _update_state(key, state, captured, messages, sent_increment)
            return resp
        finally:
            _set_flag(False)
            _set_captured(None)

    _patched_request._kvcache_patched = True
    AsyncAPIClient.request = _patched_request
    AsyncAPIClient._kvcache_patched = True


def _status_is(captured_headers, code):
    # httpx 响应头里没有 status;status 在 resp 对象上。这里用替代信号:
    # 我们的 fast_fail 422 响应一定带 X-Cache-Hit: false;为避免误判普通 200 hit=false,
    # 只在 sent_increment 时判定。此处放宽:命中判断已在调用处用 hit 标志。
    return True


def _update_state(key, state, captured, messages, sent_increment):
    if not captured:
        return
    # captured 可能是大小写敏感的 dict(httpx 头会变小写),统一小写
    cap = {str(k).lower(): v for k, v in captured.items()}
    new_hash = cap.get(HEADER_RESP_CACHE_HASH.lower())
    if not new_hash:
        _cache_mgr.invalidate(key)
        return
    expire_str = cap.get(HEADER_RESP_CACHE_EXPIRE.lower())
    try:
        expire = int(expire_str) if expire_str else time.time()
    except (TypeError, ValueError):
        expire = time.time()
    hit = _parse_cache_hit(cap.get(HEADER_RESP_CACHE_HIT.lower()))
    # 重新从表里拿当前 state:miss 重试时旧的可能已被 invalidate,
    # 需确保写回的是表里实际存在的对象(否则 get_cache_state 取不到)。
    cur = _cache_mgr.get(key)
    # messages 是【完整】 messages(在 _apply_increment 改写 options 前捕获)。
    # 无论 hit+增量 还是 全量,新前缀都是【本次完整 messages】(网关也按此算 hash)。
    cur.cache_hash = new_hash
    cur.expire_time = expire
    cur.prefix_count = len(messages)
    cur.prefix_digest = _messages_digest(messages)


# ---------------------------------------------------------------------------
# 对外 API
# ---------------------------------------------------------------------------


_INSTALLED = False


def install() -> None:
    """安装 monkey patch。幂等。"""
    global _INSTALLED
    if _INSTALLED:
        return
    _patch_httpx_sync()
    _patch_httpx_async()
    _patch_sync()
    _patch_async()
    _INSTALLED = True


def reset_cache() -> None:
    """清空【当前上下文】的缓存(session 级隔离下,只影响当前上下文)。

    测试里若想彻底重置(跨上下文),用 hard_reset()。
    """
    tbl = _cache_mgr._table()
    with _cache_mgr._lock:
        tbl.clear()


def hard_reset() -> None:
    """彻底重置:重建缓存管理器(测试用,生产勿调)。"""
    global _cache_mgr
    _cache_mgr = _CacheManager()


def get_cache_state(model: str, base_url: str) -> _CacheState:
    return _cache_mgr.get((model, _norm_base(base_url)))
