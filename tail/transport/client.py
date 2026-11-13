"""客户端接入:把 openai SDK(或任意 httpx 客户端)的请求透明换成帧流。

现有代码零改动
--------------
::

    import httpx2
    from openai import OpenAI
    from tail.transport.client import TailTransport

    client = OpenAI(
        base_url="http://127.0.0.1:8765/v1",
        api_key="sk-xxx",
        http_client=httpx2.Client(transport=TailTransport()),
    )

    # 下面这行和没有 Tail 时完全一样
    client.chat.completions.create(model="deepseek-chat", messages=messages)

异步同理,换成 ``AsyncOpenAI`` + ``httpx2.AsyncClient(transport=TailAsyncTransport())``。

库版本说明:openai 3.x 依赖 ``httpx2``;openai 1.x 依赖 ``httpx``。两个库把
请求对象传给 transport,所以本模块按请求所属的库自动选择内层 transport,
两种都能用,不需要调用方关心。

为什么不再 monkey-patch openai SDK
----------------------------------
旧实现 patch 了 ``SyncAPIClient.request`` 改写 ``options.json_data``、patch
``httpx.Client.send`` 抓 ``X-Cache-*`` 响应头。那对应的是「body 仍是标准 JSON,
只在头上做前缀协商」的旧协议。

新协议把**整个 body 换成二进制帧流**,并且客户端要维护「已知块哈希集合」——
这些在 openai SDK 的私有方法上改不动。

``httpx2`` 的 ``transport`` 是公开扩展点:SDK 内部结构一行不碰,只替换"字节怎么
发出去"这一段。比旧实现更稳,也不依赖 openai 的私有属性。

它做什么
--------
只对 ``/chat/completions`` 的 JSON 请求生效,其余请求(模型列表、文件上传、
embedding……)原样透传:

1. 读请求 body(此时已是 openai SDK 序列化好的 JSON);
2. ``render`` 成文本 → 定长切块 → 查本地已知块集合 → 编成帧流;
3. 把 ``Content-Type`` 换成 ``application/x-tail-v2``、去掉 ``Content-Length``
   (body 变成流式,由 httpx 自动用 chunked 发);
4. 内层 transport 真正发出;
5. 若响应带 ``X-Tail-Retransmit: 1``(网关缺块),清空本地已知集合并**自动重发一次**
   全量——对上层完全透明。
"""

from __future__ import annotations

import importlib
import json
from typing import Any, Dict, Iterator, Optional

from .gateway import CONTENT_TYPE_BLOCKS, HEADER_RETRANSMIT, TailClient

#: 只处理这个路径的请求。其余(embeddings / models / files…)直接透传。
CHAT_PATH_MARKER = "/chat/completions"


def _is_chat_json_request(request: Any, marker: str) -> bool:
    """判断是否是需要走 Tail 的 chat 请求。"""
    if marker not in str(request.url):
        return False
    ctype = request.headers.get("content-type", "")
    return "json" in ctype.lower()


def _rebuild(request: Any, content: Any, *, tail: bool) -> Any:
    """用新 body 重建请求对象,保留方法、URL、其余头。

    ``content`` 可以是 bytes,也可以是 ``Iterator[bytes]``(流式体)。

    原 ``Content-Length`` 必须去掉:body 变成流式后长度未知,由 httpx 用
    ``Transfer-Encoding: chunked`` 发。
    """
    drop = {"content-length", "content-type", "transfer-encoding"}
    headers = [(k, v) for k, v in request.headers.raw
               if k.decode("latin-1").lower() not in drop]
    if tail:
        headers.append((b"content-type", CONTENT_TYPE_BLOCKS.encode("latin-1")))
    else:
        headers.append((b"content-type", b"application/json"))
    return type(request)(
        method=request.method,
        url=request.url,
        headers=headers,
        content=content,
        extensions=request.extensions,
    )


class TailTransport:
    """同步传输适配。可传给 ``httpx2.Client(transport=...)``
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
        """底层 :class:`TailClient`(可读 ``known_blocks`` 等统计)。"""
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
            # 网关缺块(存储被清 / 重启 / 淘汰):清空本地已知集合,重发全量一次。
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
        # 流式:encoder 逐块产出,httpx 边收边发,不把整个 body 拼成一个大 bytes
        body = self._client.iter_encode_request(payload)
        return self._resolve_inner(request).handle_request(
            _rebuild(request, body, tail=True))


class TailAsyncTransport:
    """异步传输适配。可传给 ``httpx.AsyncClient(transport=...)``
    或 ``openai.AsyncOpenAI(http_client=...)``。
    """

    def __init__(self,
                 inner: Any = None,
                 client: Optional[TailClient] = None,
                 chat_path_marker: str = CHAT_PATH_MARKER) -> None:
        self._inner = inner              # None 时按请求所属库惰性创建
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

    # -- 内部 -------------------------------------------------------------

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
        return await self._resolve_inner(request).handle_async_request(
            _rebuild(request, _aiter_bytes(self._client.iter_encode_request(payload)),
                     tail=True))


async def _aiter_bytes(chunks: Iterator[bytes]):
    """把同步字节迭代器包成异步迭代器(httpx 异步流式请求体接受它)。"""
    for chunk in chunks:
        yield chunk
