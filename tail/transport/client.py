"""httpx / httpx2 传输适配:把 chat 请求的 JSON body 换成 v3 线缆。

用法(现有代码只多一个参数)::

    import httpx2
    from tail.transport.client import TailTransport

    http = httpx2.Client(transport=TailTransport())
    client = openai.OpenAI(http_client=http)

v3 做的事只有三件:

1. 请求方向:JSON body → (会话键, 前缀凭据) 两个头 + 尾部字节流。
2. 响应方向:读 ``X-Tail-Have`` 记下服务端确认的前缀,供下一轮使用。
3. 服务端说前缀失效(``X-Tail-Retransmit``)时,丢掉凭据、全量重发一次,
   对上层完全透明。

**没有任何整包拷贝**:body 是渲染器产出的片段流,httpx 边收边发。
"""

from __future__ import annotations

import importlib
import json
from typing import Any, Dict, Optional

from .gateway import (CONTENT_TYPE_BLOCKS, HEADER_HAVE, HEADER_RETRANSMIT,
                      HEADER_SESSION, TailClient)

#: 只处理这个路径的请求。其余(embeddings / models / files…)直接透传。
CHAT_PATH_MARKER = "/chat/completions"


def _is_chat_json_request(request: Any, marker: str) -> bool:
    """判断是否是需要走 Tail 的 chat 请求。"""
    if marker not in str(request.url):
        return False
    ctype = request.headers.get("content-type", "")
    return "json" in ctype.lower()


def _rebuild(request: Any, content: Any, *, tail: bool,
             extra: Optional[Dict[str, str]] = None) -> Any:
    """用新 body 重建请求对象,保留方法、URL、其余头。

    ``content`` 可以是 bytes,也可以是 ``Iterator[bytes]``(流式体)。

    原 ``Content-Length`` 必须去掉:body 变成流式后长度未知,由 HTTP 库用
    ``Transfer-Encoding: chunked`` 发。
    """
    drop = {"content-length", "content-type", "transfer-encoding"}
    headers = [(k, v) for k, v in request.headers.raw
               if k.decode("latin-1").lower() not in drop]
    headers.append((b"content-type",
                    (CONTENT_TYPE_BLOCKS if tail else "application/json")
                    .encode("latin-1")))
    for name, value in (extra or {}).items():
        if value:
            headers.append((name.encode("latin-1"), value.encode("latin-1")))
    return type(request)(
        method=request.method,
        url=request.url,
        headers=headers,
        content=content,
        extensions=request.extensions,
    )


class TailTransport:
    """同步传输适配。可传给 ``httpx.Client(transport=...)``
    或 ``openai.OpenAI(http_client=...)``。

    **不继承任何基类**:``httpx`` 与 ``httpx2`` 的 base class 不同,而传输层只被
    要求提供 ``handle_request``/``close`` 两个方法。鸭子类型让同一个适配器既能服务
    ``openai`` 3.x(依赖 ``httpx2``),也能服务 ``openai`` 1.x(依赖 ``httpx``)。
    """

    def __init__(self,
                 inner: Any = None,
                 client: Optional[TailClient] = None,
                 chat_path_marker: str = CHAT_PATH_MARKER) -> None:
        self._inner = inner              # None 时按请求所属库惰性创建
        self._client = client if client is not None else TailClient()
        self.chat_path_marker = chat_path_marker
        self._autoinner: Dict[str, Any] = {}

    # -- 对外 -------------------------------------------------------------

    @property
    def client(self) -> TailClient:
        """底层 :class:`TailClient`(可读 ``sessions`` 等统计)。"""
        return self._client

    def handle_request(self, request: Any) -> Any:
        inner = self._resolve_inner(request)
        if not _is_chat_json_request(request, self.chat_path_marker):
            return inner.handle_request(request)

        raw = request.read()
        try:
            payload = json.loads(raw)
        except ValueError:
            # 不是合法 JSON:不猜,原样发出去
            return inner.handle_request(_rebuild(request, raw, tail=False))

        response = self._send(request, payload)
        if response.headers.get(HEADER_RETRANSMIT):
            # 前缀失效(存储被清 / 重启 / 淘汰):丢掉凭据,全量重发一次。
            # 对调用方透明——它只看到一次正常的响应。
            self._client.invalidate()
            response.close()
            response = self._send(request, payload)
        return response

    def close(self) -> None:
        closers = [self._inner, *self._autoinner.values()]
        for obj in closers:
            close = getattr(obj, "close", None)
            if close is not None:
                close()

    # -- 内部 -------------------------------------------------------------

    def _resolve_inner(self, request: Any) -> Any:
        """按请求对象所属的 HTTP 库挑选内层 transport。

        项目里可能同时装了 ``httpx`` 与 ``httpx2``(openai 3.x 依赖后者),
        两者的流对象类型不兼容,必须用**同一个库**的 transport 去发。
        """
        if self._inner is not None:
            return self._inner
        lib_name = type(request).__module__.split(".")[0]
        inner = self._autoinner.get(lib_name)
        if inner is None:
            lib = importlib.import_module(lib_name)
            inner = lib.HTTPTransport()
            self._autoinner[lib_name] = inner
        return inner

    def _send(self, request: Any, payload: Dict[str, Any]) -> Any:
        prepared = self._client.prepare(payload)
        headers = {
            HEADER_SESSION: prepared.session.hex(),
            HEADER_HAVE: (f"{prepared.have_len};{prepared.have_tip.hex()}"
                          if prepared.have_len else ""),
        }
        response = self._resolve_inner(request).handle_request(
            _rebuild(request, prepared.body, tail=True, extra=headers))
        if not response.headers.get(HEADER_RETRANSMIT):
            self._client.update_from_headers(prepared.session, response.headers)
        return response


class TailAsyncTransport:
    """异步传输适配。可传给 ``httpx.AsyncClient(transport=...)``
    或 ``openai.AsyncOpenAI(http_client=...)``。
    """

    def __init__(self,
                 inner: Any = None,
                 client: Optional[TailClient] = None,
                 chat_path_marker: str = CHAT_PATH_MARKER) -> None:
        self._inner = inner
        self._client = client if client is not None else TailClient()
        self.chat_path_marker = chat_path_marker
        self._autoinner: Dict[str, Any] = {}

    @property
    def client(self) -> TailClient:
        return self._client

    async def handle_async_request(self, request: Any) -> Any:
        inner = self._resolve_inner(request)
        if not _is_chat_json_request(request, self.chat_path_marker):
            return await inner.handle_async_request(request)

        raw = await request.aread()
        try:
            payload = json.loads(raw)
        except ValueError:
            return await inner.handle_async_request(
                _rebuild(request, raw, tail=False))

        response = await self._send(request, payload)
        if response.headers.get(HEADER_RETRANSMIT):
            self._client.invalidate()
            await response.aclose()
            response = await self._send(request, payload)
        return response

    async def aclose(self) -> None:
        for obj in [self._inner, *self._autoinner.values()]:
            aclose = getattr(obj, "aclose", None)
            if aclose is not None:
                await aclose()

    def _resolve_inner(self, request: Any) -> Any:
        if self._inner is not None:
            return self._inner
        lib_name = type(request).__module__.split(".")[0]
        inner = self._autoinner.get(lib_name)
        if inner is None:
            lib = importlib.import_module(lib_name)
            inner = lib.AsyncHTTPTransport()
            self._autoinner[lib_name] = inner
        return inner

    async def _send(self, request: Any, payload: Dict[str, Any]) -> Any:
        prepared = self._client.prepare(payload)
        headers = {
            HEADER_SESSION: prepared.session.hex(),
            HEADER_HAVE: (f"{prepared.have_len};{prepared.have_tip.hex()}"
                          if prepared.have_len else ""),
        }
        response = await self._resolve_inner(request).handle_async_request(
            _rebuild(request, prepared.body, tail=True, extra=headers))
        if not response.headers.get(HEADER_RETRANSMIT):
            self._client.update_from_headers(prepared.session, response.headers)
        return response
