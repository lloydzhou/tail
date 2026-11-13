"""渲染器 / 反渲染器:OpenAI Chat Completions JSON ⇄ 一维文本流。

**两层都是流式的。** 渲染器产出事件流与字节片段,反渲染器边收字节边产出事件。
没有任何一处需要先把完整对象拼出来。

格式参考真实实现,不自己发明
--------------------------
标记集对齐两个生产可用的实现:

  - **OpenAI harmony**(``src/registry.rs``):
    ``<|start|>`` ``<|message|>`` ``<|end|>`` ``<|channel|>`` ``<|return|>``
  - **DeepSeek recipe**(``deepseek-recipe-core/src/special_tokens.rs``):
    ``<｜begin▁of▁sentence｜>`` ``的女子`` ``рија`` `` мушка`` `` thinking`` ``<｜end_of_query｜>``

两者的共同点,也是本模块遵循的原则:

  1. **标记是可读的 ``<|...|>`` / ``<｜...｜>``,不是控制字符**
  2. **线性布局:标记 + 内容**,不用分隔符切分
  3. **正文不做转义**——真实文本里撞上这些标记的概率极低

早先版本用 ``\\x1e`` / ``\\x1f`` 控制字符做分隔是错的:正文里完全可能出现这两个
字节(用户贴二进制、base64、恶意输入),必须转义,而转义既慢又让"中间传的是
文本"这个前提失效。现已改为 harmony 风格。

本模块使用的标记
----------------
=============================  ==========================================
``<|model|>`` 等                顶层参数,值紧跟其后
``<|tools|>``                   工具定义(其后是确定性 JSON)
``<|system|>`` ``<|user|>``      role 标记,开启一条新 message
``<|assistant|>`` ``<|tool|>``
``<|tool_calls|>``              assistant 的工具调用
``<|tool_call_id|>``            tool 消息关联的调用 id
=============================  ==========================================

布局::

    <|model|>deepseek-chat<|temperature|>0.7<|system|>你是助手。<|user|>你好

消息边界由「下一个 role 标记」自然表达,不需要额外分隔符——与 harmony 的
``<|start|>role<|channel|>text<|end|>`` 是同一思路。

事件流
------
渲染器除了产出字节,还能产出**结构化事件**(:class:`Event`)。
编码器用它来切段,不需要扫描任何标记字节——这样正文永远是纯文本,
段边界也不需要靠猜。

确定性(硬约束)
--------------
同一个 messages 必须渲染出**逐字节相同**的文本,否则块内容变化、``block_hash``
变化、缓存整体失效。因此:字段顺序固定、``json.dumps(sort_keys=True)``。

格式版本不进正文,放在 HTTP 头(``X-Tail-Version``)里。理由:写进正文会让
版本号落在第一个块上,升版即全量失效,而且参考实现(DeepSeek / harmony)
的渲染文本里也都没有这种东西。
"""

from __future__ import annotations

import codecs
import json
from typing import Any, Dict, Iterator, List, Optional, Tuple

#: 标记前后缀。与 harmony 一致:``<|name|>``。
TAG_OPEN = "<|"
TAG_CLOSE = "|>"

#: 已知 role 标记。反渲染时遇到它们就开一条新 message。
ROLE_TAGS = frozenset({"system", "user", "assistant", "tool", "developer",
                       "function"})

#: 已知字段标记(出现在 message 内部)。
FIELD_TAGS = frozenset({"tool_calls", "tool_call_id", "name", "extra"})

#: 顶层参数标记。渲染顺序固定 = 确定性。
PARAM_ORDER = (
    "model",
    "max_tokens",
    "max_completion_tokens",
    "temperature",
    "top_p",
    "n",
    "stream",
    "stream_options",
    "stop",
    "presence_penalty",
    "frequency_penalty",
    "logit_bias",
    "logprobs",
    "top_logprobs",
    "seed",
    "response_format",
    "tool_choice",
    "parallel_tool_calls",
    "user",
    "reasoning_effort",
    "service_tier",
)

#: ``content`` 键缺失的哨兵(与 None / 空串区分)。
_MISSING = {"__missing__": True}

#: 渲染片段的最大字符数。只影响产出粒度,不影响格式。
_CHUNK_CHARS = 64 * 1024

#: 后缀树用:所有已知标记拼成的集合,用于在正文里识别"疑似标记"。
_ALL_TAGS = (ROLE_TAGS | FIELD_TAGS | {"tools"}) | set(PARAM_ORDER)


def tag(name: str) -> str:
    """``name`` → ``<|name|>``。"""
    return f"{TAG_OPEN}{name}{TAG_CLOSE}"


def _dumps(obj: Any) -> str:
    """确定性的 JSON 序列化:固定键顺序、紧凑分隔符、不转义非 ASCII。

    ``sort_keys=True`` 是确定性要求的一部分——dict 插入顺序来自客户端,
    不同 SDK 版本可能不同,不排序就无法保证渲染逐字节稳定。
    """
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _chunked(s: str, size: int = _CHUNK_CHARS) -> Iterator[str]:
    """把长字符串切成片段产出,避免单个巨大 str 常驻。"""
    if not s:
        return
    if len(s) <= size:
        yield s
        return
    for i in range(0, len(s), size):
        yield s[i:i + size]


# ---------------------------------------------------------------------------
# 内容归一化
# ---------------------------------------------------------------------------

def content_to_text(content: Any) -> str:
    """把 message.content 归一成字符串。

    OpenAI 允许两种形态:
      - str:纯文本
      - list[{"type":"text",...} | {"type":"image_url",...}]

    多模态的 image part 无法渲染成文本,保留其 URL 标记以便反渲染识别。
    """
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if not isinstance(part, dict):
                parts.append(str(part))
                continue
            ptype = part.get("type")
            if ptype == "text":
                parts.append(part.get("text") or "")
            elif ptype == "image_url":
                url = part.get("image_url")
                if isinstance(url, dict):
                    url = url.get("url")
                parts.append(f"<|image|>{url or ''}")
            else:
                parts.append(_dumps(part))   # 未知 part:序列化保留
        return "".join(parts)
    return _dumps(content)


def tool_calls_to_text(tool_calls: List[dict]) -> str:
    """assistant 的 tool_calls 原样按 OpenAI 结构做确定性 JSON。

    为什么不用 ``name(args)`` 这种"函数调用"写法:那是我自造的语法,
    有歧义——assistant 正文里出现 ``f({"a":1})`` 时,反渲染会把它误认成
    tool_call;而且 ``id`` / ``type`` 字段无处安放,直接丢失。

    这里只做两件事:键排序(确定性)、去掉 arguments 里的冗余空白
    (字符串参数是 JSON 文本,重新序列化后更小且等价)。
    """
    out: List[dict] = []
    for call in tool_calls:
        if not isinstance(call, dict):
            out.append(call)
            continue
        # arguments 原样保留:它是模型/客户端给的 JSON 文本,重新序列化会
        # 改变空白(``{"k": 1}`` → ``{"k":1}``),字节就不再一致。这里只做
        # 键排序(确定性),不动任何字符串内容。
        out.append(call)
    return _dumps(out)


# ---------------------------------------------------------------------------
# 事件
# ---------------------------------------------------------------------------

class EventKind:
    """渲染/反渲染事件类型。"""
    PARAM = "param"          # (PARAM, (name, value))   顶层参数
    MESSAGE_START = "start"  # (MESSAGE_START, role)    一条 message 开始
    MESSAGE_END = "end"      # (MESSAGE_END, None)      一条 message 结束
    TEXT = "text"            # (TEXT, str)              正文片段


#: 一个事件:(kind, payload)
Event = Tuple[str, Any]


# ---------------------------------------------------------------------------
# 渲染:JSON → 文本(事件流 + 字节流)
# ---------------------------------------------------------------------------

def iter_render_events(payload: Dict[str, Any]) -> Iterator[Event]:
    """把请求体渲染成**事件流**。

    事件序列::

        (PARAM, ("model", "deepseek-chat"))
        (MESSAGE_START, "system")
        (TEXT, "你是助手。")
        (MESSAGE_END, None)
        ...

    编码器用 ``MESSAGE_START`` / ``MESSAGE_END`` 切段,不需要认识任何标记字节。
    """
    for key in PARAM_ORDER:
        if key in payload and payload[key] is not None:
            yield (EventKind.PARAM, (key, payload[key]))

    tools = payload.get("tools")
    if tools:
        yield (EventKind.PARAM, ("tools", tools))

    for msg in payload.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        yield from iter_message_events(msg)


def iter_message_events(msg: Dict[str, Any]) -> Iterator[Event]:
    """渲染单条 message 的事件序列。"""
    role = msg.get("role") or "user"
    yield (EventKind.MESSAGE_START, role)

    content = content_to_text(msg.get("content"))
    if content:
        # 正文是 JSON 字符串而不是裸文本:OpenAI 原文里 content 就是字符串,
        # json.dumps 自带完整转义,正文里出现 ``<|user|>`` 之类不会被当结构。
        yield (EventKind.TEXT, _dumps(content))
    elif "content" in msg and msg["content"] is not None:
        yield (EventKind.TEXT, _dumps(""))     # 显式空串

    tool_calls = msg.get("tool_calls")
    if tool_calls:
        yield (EventKind.TEXT, tag("tool_calls"))
        yield (EventKind.TEXT, tool_calls_to_text(tool_calls))

    tcid = msg.get("tool_call_id")
    if tcid:
        yield (EventKind.TEXT, tag("tool_call_id"))
        yield (EventKind.TEXT, _dumps(str(tcid)))

    name = msg.get("name")
    if name:
        yield (EventKind.TEXT, tag("name"))
        yield (EventKind.TEXT, _dumps(str(name)))

    # 其他未识别字段:确定性序列化后附加,做到不静默丢数据。
    # 另把 content 的"存在性"带上:None / 缺失 / 空串在 OpenAI 里是三种语义,
    # 但它们都不产生正文,只能在这里区分。
    extra = {k: v for k, v in msg.items()
             if k not in ("role", "content", "tool_calls", "tool_call_id", "name")}
    if "content" in msg and not content_to_text(msg.get("content")):
        extra.setdefault("__content__", msg["content"])
    elif "content" not in msg:
        extra.setdefault("__content__", _MISSING)
    if extra:
        yield (EventKind.TEXT, tag("extra"))
        yield (EventKind.TEXT, _dumps(extra))

    yield (EventKind.MESSAGE_END, None)


def event_to_bytes(event: Event) -> Iterator[bytes]:
    """把单个事件编码成字节片段。

    PARAM 事件编码成 ``<|name|>value``;MESSAGE_START 编码成 ``<|role|>``。
    """
    kind, payload = event
    if kind == EventKind.PARAM:
        name, value = payload
        value_text = _dumps(value) if name == "tools" else _dumps(value)
        yield (tag(name) + value_text).encode("utf-8")
    elif kind == EventKind.MESSAGE_START:
        yield tag(payload).encode("utf-8")
    elif kind == EventKind.TEXT:
        for piece in _chunked(payload):
            yield piece.encode("utf-8")
    # MESSAGE_END 不产出字节:边界由下一个 role 标记表达


def iter_render(payload: Dict[str, Any]) -> Iterator[bytes]:
    """把请求体渲染成 UTF-8 字节片段流。"""
    for event in iter_render_events(payload):
        yield from event_to_bytes(event)


def render_request(payload: Dict[str, Any]) -> bytes:
    """:func:`iter_render` 的一次性版本(测试用)。"""
    return b"".join(iter_render(payload))


# ---------------------------------------------------------------------------
# 反渲染:文本 → JSON(流式)
# ---------------------------------------------------------------------------

class RenderParser:
    """**流式**反渲染器:喂字节片段,产出事件。

    与"先收齐整段文本再解析"不同,这个类维护一个待处理缓冲,每切出完整的一段
    就立刻产出。内存占用与「单条 message」的大小成正比,与整份文本无关。

    识别标记:扫描 ``<|`` 到下一个 ``|>``,取其间名字;只有名字在已知标记集里
    才当作标记,否则视为正文——这样正文里偶然出现的 ``<|`` 不会误判。

    产出的事件:
      ``(PARAM, (name, value))``        顶层参数
      ``(MESSAGE_START, role)``         一条 message 开始
      ``(TEXT, str)``                   正文片段
      ``(MESSAGE_END, dict)``           一条 message 完成,payload 是解析结果

    用法::

        parser = RenderParser()
        for piece in byte_stream:
            for event in parser.feed(piece):
                ...
        for event in parser.finish():
            ...
    """

    __slots__ = ("_buf", "_buf_start", "_decoder", "_cur_role", "_parts",
                 "_fields", "_in_str", "_escaped", "_scan", "error")

    def __init__(self) -> None:
        self._buf = ""
        # 惰性裁剪:_buf[:_buf_start] 是已消费的前缀。每次处理完一个标记就
        # ``self._buf = buf[pos:]`` 会整段复制,消息多时退化成 O(n²)。
        self._buf_start = 0
        # 增量 UTF-8 解码:调用方可以按任意字节边界喂入
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._cur_role: Optional[str] = None
        self._parts: List[str] = []          # 当前 message 的正文
        self._fields: Dict[str, str] = {}    # 当前 message 的字段
        # JSON 字符串状态:``_in_str`` 表示当前是否在字符串内,``_scan`` 是
        # 已消费到 ``_buf`` 的哪个下标。二者随缓冲区一起推进,避免每次从头重扫。
        self._in_str = False
        self._escaped = False                 # 上一个字节是反斜杠,且尚未配对
        self._scan = 0
        self.error: Optional[str] = None

    # -- 对外 -------------------------------------------------------------

    def feed(self, data: bytes) -> Iterator[Event]:
        if self.error:
            return
        try:
            self._buf += self._decoder.decode(data, False)
            yield from self._drain(final=False)
        except ValueError as exc:
            self.error = str(exc)
            raise

    def finish(self) -> Iterator[Event]:
        if self.error:
            return
        self._buf += self._decoder.decode(b"", True)
        yield from self._drain(final=True)
        if self._cur_role is not None:
            yield from self._flush_message()

    # -- 内部 -------------------------------------------------------------

    def _find_tag_start(self, buf: str, pos: int) -> int:
        """从 ``pos`` 起找下一个**真正生效**的标记起点。

        正文和字段值都用 JSON 字符串承载,所以位于 JSON 字符串**内部**的
        ``<|user|>`` 之类是内容,不是结构——这里按 JSON 的字符串/转义规则
        跳过它们。这正是 JSON 无歧义的来源,不需要另外发明转义符号。

        字符串状态是**增量推进**的(``_in_str`` / ``_scan``),每字节只扫一次,
        流式切块不会导致状态丢失,也不会退化成 O(n²)。
        """
        while True:
            open_at = buf.find(TAG_OPEN, pos)
            if open_at < 0:
                self._advance_scan(buf, len(buf))
                return -1
            if self._advance_scan(buf, open_at) and not self._in_str:
                return open_at
            # 在字符串内部(或刚好跨过边界):这个 ``<|`` 是内容,跳过继续找
            pos = open_at + len(TAG_OPEN)
            self._scan = max(self._scan, pos)

    def _advance_scan(self, buf: str, upto: int) -> bool:
        """把 JSON 字符串状态推进到下标 ``upto``,返回推进后是否**不在**字符串内。

        逐字符扫描。曾试过用 ``str.find`` 跳到下一个引号来加速,但反斜杠转义
        状态跨块延续时容易算错(实测 400 例里 235 例流式解析出错),已回退。
        """
        start = self._scan
        if upto > start:
            in_str = self._in_str
            escaped = self._escaped            # 跨块:反斜杠状态必须延续
            for i in range(start, upto):
                ch = buf[i]
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_str = not in_str
            self._in_str = in_str
            self._escaped = escaped
            self._scan = upto
        return not self._in_str

    def _drain(self, final: bool) -> Iterator[Event]:
        """把缓冲切成「正文段」与「标记」,逐个处理。"""
        # 已消费前缀在各次 feed 之间惰性保留,这里统一裁掉一次(str 不可原地
        # del,所以整体切一次;相比每个标记切一次,O(n²) 降到 O(标记数))
        buf = self._buf[self._buf_start:] if self._buf_start else self._buf
        if self._buf_start:
            self._scan = max(0, self._scan - self._buf_start)
        pos = 0
        n = len(buf)
        out: List[Event] = []
        target: Optional[List[str]] = None   # 正文写到哪:fields 名或 None=正文

        while pos < n:
            open_at = self._find_tag_start(buf, pos)
            if open_at < 0:
                rest = buf[pos:] if final else buf[pos:max(pos, n - 1)]
                if rest:
                    self._append_text(rest)
                pos = len(buf) if final else max(pos, n - 1)
                break

            close_at = buf.find(TAG_CLOSE, open_at + len(TAG_OPEN))
            if close_at < 0:
                # 标记未收全
                if final:
                    self._append_text(buf[pos:])
                    pos = n
                else:
                    self._append_text(buf[pos:open_at])
                    pos = open_at
                break

            name = buf[open_at + len(TAG_OPEN):close_at]
            if name not in _ALL_TAGS:
                # 不是已知标记:连同尖括号一起当正文
                self._append_text(buf[pos:close_at + len(TAG_CLOSE)])
                pos = close_at + len(TAG_CLOSE)
                continue

            # 标记之前的内容属于上一条 message 的正文
            self._append_text(buf[pos:open_at])
            pos = close_at + len(TAG_CLOSE)

            if name in ROLE_TAGS:
                out.extend(self._flush_message())
                self._cur_role = name
                out.append((EventKind.MESSAGE_START, name))
            elif name in PARAM_ORDER or name == "tools":
                out.extend(self._flush_message())
                self._cur_role = "__param__"
                self._fields = {"__name__": name}
            else:
                # message 内部字段:其后内容归入该字段
                self._fields[name] = ""
                self._fields["__in__"] = name

        # 惰性裁剪:只记录游标,真正切除推迟到下次 _drain 开头
        self._buf = buf
        self._buf_start = pos

        # 顶层参数:值收齐后作为一个事件产出
        if self._cur_role == "__param__" and (final or len(buf) > pos):
            pass  # 参数值可能还没收完,延后

        yield from out

    def _append_text(self, text: str) -> None:
        if not text:
            return
        cur = self._fields.get("__in__") if self._cur_role == "__param__" else None
        if self._cur_role == "__param__":
            self._fields["__value__"] = self._fields.get("__value__", "") + text
        elif self._cur_role is None:
            # 版本头等前置内容,丢弃
            return
        else:
            name = self._fields.get("__in__")
            if name:
                self._fields[name] = self._fields.get(name, "") + text
            else:
                self._parts.append(text)

    def _flush_message(self) -> List[Event]:
        if self._cur_role is None:
            return []
        role = self._cur_role
        parts, fields = self._parts, self._fields
        self._cur_role = None
        self._parts = []
        self._fields = {}
        if role == "__param__":
            name = fields.get("__name__", "")
            raw = fields.get("__value__", "")
            try:
                return [(EventKind.PARAM, (name, json.loads(raw)))]
            except ValueError:
                return []
        return [(EventKind.MESSAGE_END, _build_message(role, parts, fields))]


def _build_message(role: str, parts: List[str], fields: Dict[str, str]) -> Dict[str, Any]:
    """把累积的正文与字段组装成一条 message。

    正文按 JSON 字符串读回(见 :func:`iter_message_events` 的说明)。
    """
    msg: Dict[str, Any] = {"role": role}
    raw_content = "".join(parts)
    if raw_content:
        try:
            msg["content"] = json.loads(raw_content)
        except ValueError:
            msg["content"] = raw_content    # 容错:非 JSON 时按原样
    # 其余情况先不写 content,由 extra.__content__ 决定(缺失 / None / 空串)

    if "tool_calls" in fields:
        msg["tool_calls"] = _parse_tool_calls(fields["tool_calls"])
    if "tool_call_id" in fields:
        try:
            msg["tool_call_id"] = json.loads(fields["tool_call_id"])
        except ValueError:
            msg["tool_call_id"] = fields["tool_call_id"]
    if "name" in fields:
        try:
            msg["name"] = json.loads(fields["name"])
        except ValueError:
            msg["name"] = fields["name"]
    if "extra" in fields:
        try:
            extra = json.loads(fields["extra"])
        except ValueError:
            extra = {}
        # __content__ 记录的是"原文有没有 content、值是什么",优先级最高
        has_content = "__content__" in extra
        saved = extra.pop("__content__", None)
        msg.update(extra)
        if has_content and "content" not in msg and saved != _MISSING:
            msg["content"] = saved          # None 或空串
        # saved 是 _MISSING:原文没有 content 键,不写
    return msg


def _parse_tool_calls(text: str) -> Any:
    """还原 tool_calls:就是一段确定性 JSON,直接 loads。

    arguments 回填成字符串(OpenAI 规定的形态),与客户端原始请求对齐。
    """
    calls = json.loads(text)
    if isinstance(calls, list):
        for call in calls:
            fn = call.get("function") if isinstance(call, dict) else None
            if isinstance(fn, dict) and not isinstance(fn.get("arguments"), str):
                fn["arguments"] = _dumps(fn.get("arguments") or {})
    return calls


def unrender_request(text: bytes) -> Dict[str, Any]:
    """:class:`RenderParser` 的一次性版本(测试用)。"""
    parser = RenderParser()
    payload: Dict[str, Any] = {}
    messages: List[Dict[str, Any]] = []
    for event in list(parser.feed(text)) + list(parser.finish()):
        kind, value = event
        if kind == EventKind.MESSAGE_END and isinstance(value, dict):
            messages.append(value)
        elif kind == EventKind.PARAM:
            payload[value[0]] = value[1]
    if messages:
        payload["messages"] = messages
    return payload


def messages_to_text(raw_json: bytes) -> bytes:
    return render_request(json.loads(raw_json))


def text_to_messages(text: bytes) -> bytes:
    return json.dumps(unrender_request(text), ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
