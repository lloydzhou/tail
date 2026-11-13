"""渲染器 / 反渲染器:OpenAI Chat Completions JSON ⇄ 一维文本流。

这是整个传输层里<b>唯一懂 LLM 语义</b>的两层。Encoder / Decoder 只认字节,
完全不知道哪个字节是 role、哪个是 tools。

为什么渲染成文本能减小体积
--------------------------
OpenAI 的请求体是 JSON:

    {"model":"x","messages":[{"role":"user","content":"你好"},...]}

其中 `{"role":`、`","content":"`、`"},{"role":"` 这些结构字节每轮对话都要重传,
既不受前缀缓存影响(它们在每个 message 之间重复出现,不在前缀上),
也无法被内容定义分块合并(它们太短且位置分散)。

渲染成一维文本后:

    <|User|>你好<|Assistant|>...

结构信息被压缩成极少的分隔符,正文连续排列。同样的对话,文本形态通常只有
原始 JSON 的 60~75%(键名越长、message 越多,压缩越明显)。

确定性要求(硬约束)
------------------
同一个 messages 必须渲染出<b>逐字节相同</b>的文本,否则块内容变化、
`block_id` 变化、缓存整体失效。因此:

  - 字段出现顺序固定(不依赖 dict 插入顺序)
  - 所有可迭代对象顺序固定
  - 模板带版本号,模板改动必须升版本

不可逆性
--------
本渲染器是<b>面向字节去重</b>设计的,不是为无损往返设计的。反渲染器能恢复出
等价的 messages 列表,但以下情况会规范化:

  - message 里未识别的字段(如 `name`、`tool_call_id` 之外的扩展字段)会被丢弃
  - `content` 为 list 的多模态格式,只保留 text part

这在实际使用中不构成问题:网关只需要把语义等价的请求交给上游,上游不关心中
间传输用了什么格式。若某个字段确实必须保留,渲染器会显式支持它——但一旦支持,
就必须同时保证渲染确定性。

版本
----
``TEMPLATE_VERSION`` 参与 ``block_id`` 计算(见 encoder),模板改动时旧缓存自然
全部 miss 而不是静默错配。
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

#: 渲染模板版本。任何影响输出字节的改动都必须递增。
TEMPLATE_VERSION = "cc1"

#: 分隔符。选用在自然语言里极少出现的组合,避免与正文混淆。
ROLE_OPEN = "<|"
ROLE_CLOSE = "|>"
FIELD_SEP = "\x1e"       # record separator:字段边界
RECORD_SEP = "\x1f"      # unit separator:message 边界


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------

#: 正文转义表。正文里出现分隔符会让反渲染切错字段,必须转义。
#: 反转义顺序与转义相反,先处理 \x01 再处理其余,避免双重反转。
_ESC = "\\"
_ESC_MAP = {"\\": _ESC + "\\",
            FIELD_SEP: _ESC + "e",
            RECORD_SEP: _ESC + "r",
            "\x00": _ESC + "0"}


def _escape(text: str) -> str:
    """转义正文里的分隔符与控制字符。

    用户消息里确实可能出现 \x1e / \x1f(例如从文件里读出来的二进制片段),
    不转义就会被当成字段边界,反渲染出来的内容就错了。
    """
    out = []
    for ch in text:
        out.append(_ESC_MAP.get(ch, ch))
    return "".join(out)


def _unescape(text: str) -> str:
    """:func:`_escape` 的逆操作。"""
    if _ESC not in text:
        return text
    out = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == _ESC and i + 1 < n:
            nxt = text[i + 1]
            if nxt == "\\":
                out.append("\\")
            elif nxt == "e":
                out.append(FIELD_SEP)
            elif nxt == "r":
                out.append(RECORD_SEP)
            elif nxt == "0":
                out.append("\x00")
            else:
                # 未知转义:原样保留,不丢信息
                out.append(ch)
                out.append(nxt)
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _dumps(obj: Any) -> str:
    """确定性的 JSON 序列化:固定键顺序、紧凑分隔符、不转义非 ASCII。

    ``sort_keys=True`` 是确定性要求的一部分——dict 的插入顺序来自客户端,
    不同 SDK 版本可能不同,不排序就无法保证渲染结果逐字节稳定。
    """
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def _content_to_text(content: Any) -> str:
    """把 message.content 归一成字符串。

    OpenAI 允许两种形态:
      - str:纯文本
      - list[{"type":"text","text":...} | {"type":"image_url",...}]

    多模态的 image part 无法渲染成文本,这里保留其 URL 标记以便反渲染时识别
    (实际是否会用到取决于上游是否支持;本期只保证不丢信息地标记出来)。
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
                parts.append(f"<|image:{url or ''}|>")
            else:
                # 未知 part:序列化保留,避免静默丢数据
                parts.append(_dumps(part))
        return "".join(parts)
    # 其他类型(极少见):序列化
    return _dumps(content)


def _tool_calls_to_text(tool_calls: List[dict]) -> str:
    """assistant 的 tool_calls 渲染成可解析的单行片段。

    每个调用渲染成 ``name(args_json)``,多个调用用 FIELD_SEP 分隔。参数用
    确定性 JSON(键已排序),保证同一调用永远渲染成同样的字节。
    """
    out: List[str] = []
    for call in tool_calls:
        if not isinstance(call, dict):
            out.append(_dumps(call))
            continue
        fn = call.get("function") or {}
        name = fn.get("name") or ""
        args = fn.get("arguments")
        if isinstance(args, str):
            # 已经是字符串:尝试解析成对象后重新确定性序列化,
            # 否则直接保留原串(无法解析时不能丢信息)。
            try:
                args_obj = json.loads(args)
                args_text = _dumps(args_obj)
            except (ValueError, TypeError):
                args_text = args
        elif args is None:
            args_text = "{}"
        else:
            args_text = _dumps(args)
        out.append(f"{name}({args_text})")
    return FIELD_SEP.join(out)


# ---------------------------------------------------------------------------
# 渲染:messages JSON → 文本
# ---------------------------------------------------------------------------

def render_request(payload: Dict[str, Any]) -> bytes:
    """把完整的 Chat Completions 请求体渲染成一维文本(UTF-8 字节)。

    输出结构(所有分隔符都是单字节控制字符):

        <|cc1|>                                 模板版本头
        <|model|>模型名
        <|tools|>工具定义(确定性 JSON)
        <|User|>消息内容
        <|Assistant|>消息内容
        ...

    参数与采样设置的字段值也一并渲染,因为它们同样需要传到上游。

    Args:
      payload: 原始请求体的 dict(已 json.loads)

    Returns:
      UTF-8 编码的一维文本字节
    """
    out: List[str] = [f"{ROLE_OPEN}{TEMPLATE_VERSION}{ROLE_CLOSE}"]

    # --- 顶层标量参数:按固定顺序渲染,值为 None 的跳过 ---
    for key in _PARAM_ORDER:
        if key in payload and payload[key] is not None:
            out.append(FIELD_SEP)
            out.append(f"{ROLE_OPEN}{key}{ROLE_CLOSE}")
            out.append(_dumps(payload[key]))

    # --- tools:整段确定性 JSON。放在最前面是因为 tools 通常最长且最稳定,
    #     放在前缀里能最大化后续块命中的连续性。 ---
    tools = payload.get("tools")
    if tools:
        out.append(FIELD_SEP)
        out.append(f"{ROLE_OPEN}tools{ROLE_CLOSE}")
        out.append(_dumps(tools))

    # --- messages ---
    for msg in payload.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        out.append(RECORD_SEP)
        role = msg.get("role") or ""
        out.append(f"{ROLE_OPEN}{role}{ROLE_CLOSE}")
        content = _content_to_text(msg.get("content"))
        if content:
            # 正文前放一个 FIELD_SEP 作为"正文起点"标记:role 名被 <> 包住,
            # 正文从分隔符之后开始,反渲染才能无歧义地区分两者。
            # 正文为空时不写这个标记,避免产生空字段。
            out.append(FIELD_SEP)
            out.append(_escape(content))

        # assistant 的工具调用
        tool_calls = msg.get("tool_calls")
        if tool_calls:
            out.append(FIELD_SEP)
            out.append(f"{ROLE_OPEN}tool_calls{ROLE_CLOSE}")
            out.append(_tool_calls_to_text(tool_calls))

        # tool 角色的结果必须带上 tool_call_id,否则上游无法对应
        tcid = msg.get("tool_call_id")
        if tcid:
            out.append(FIELD_SEP)
            out.append(f"{ROLE_OPEN}tool_call_id{ROLE_CLOSE}{tcid}")

        name = msg.get("name")
        if name:
            out.append(FIELD_SEP)
            out.append(f"{ROLE_OPEN}name{ROLE_CLOSE}{name}")

        # 其他未识别字段:确定性序列化后附加,做到不静默丢数据
        extra = {k: v for k, v in msg.items()
                 if k not in ("role", "content", "tool_calls", "tool_call_id", "name")}
        if extra:
            out.append(FIELD_SEP)
            out.append(f"{ROLE_OPEN}extra{ROLE_CLOSE}{_dumps(extra)}")

    return "".join(out).encode("utf-8")


#: 顶层标量的渲染顺序。固定顺序 = 确定性。
_PARAM_ORDER = (
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


# ---------------------------------------------------------------------------
# 反渲染:文本 → messages JSON
# ---------------------------------------------------------------------------

def unrender_request(text: bytes) -> Dict[str, Any]:
    """把一维文本还原成 Chat Completions 请求体 dict。

    与 :func:`render_request` 严格对称。还原结果是<b>语义等价</b>的请求体,
    不保证与原始 JSON 逐字节相同(键顺序、被规范化的 content 形态会变),
    但上游收到的语义完全一致。

    Args:
      text: :func:`render_request` 产生的字节

    Returns:
      可直接 json.dumps 后 POST 给上游的 dict

    Raises:
      ValueError: 文本格式非法(缺少版本头、结构损坏)
    """
    s = text.decode("utf-8")
    head = f"{ROLE_OPEN}{TEMPLATE_VERSION}{ROLE_CLOSE}"
    if not s.startswith(head):
        # 版本不匹配:容忍,但要求至少有版本头,便于发现模板错配
        if not s.startswith(ROLE_OPEN):
            raise ValueError("渲染文本缺少版本头,不是本格式的产物")
    else:
        s = s[len(head):]

    payload: Dict[str, Any] = {}
    messages: List[Dict[str, Any]] = []

    # 先按 message 边界切,再解析第一个 message 之前的头部字段
    head_part, _, body = s.partition(RECORD_SEP)
    _parse_head(head_part, payload)

    for record in body.split(RECORD_SEP):
        if record:
            messages.append(_parse_message(record))
    if messages:
        payload["messages"] = messages

    return payload


def _split_fields(segment: str) -> List[str]:
    return segment.split(FIELD_SEP)


def _parse_head(segment: str, payload: Dict[str, Any]) -> None:
    """解析 messages 之前的头部:模型参数 + tools。"""
    if not segment:
        return
    for field in _split_fields(segment):
        if not field:
            continue
        name, value_text = _split_marked(field)
        if name is None:
            continue
        if name == "tools":
            payload["tools"] = json.loads(value_text)
        else:
            payload[name] = json.loads(value_text)


def _split_marked(field: str) -> "tuple[Optional[str], str]":
    """把 ``<|name|>value`` 拆成 (name, value)。不匹配返回 (None, field)。"""
    if not field.startswith(ROLE_OPEN):
        return None, field
    end = field.find(ROLE_CLOSE, len(ROLE_OPEN))
    if end < 0:
        return None, field
    name = field[len(ROLE_OPEN):end]
    return name, field[end + len(ROLE_CLOSE):]


def _parse_message(record: str) -> Dict[str, Any]:
    """解析单条 message 记录。

    记录形如 ``<|role|>ROLE\x1e正文\x1e<field|>值...``。
    role 名由 FIELD_SEP 收尾,正文里出现的 FIELD_SEP 属于正文本身,
    因此只在找到形如 ``<|xxx|>`` 的标记时才切出新字段。
    """
    msg: Dict[str, Any] = {}
    name, rest = _split_marked(record)
    if name is None:
        # 没有 role 标记:整段当作用户内容,保证不丢数据
        return {"role": "user", "content": record}

    # role 名就在标记里(已被 _split_marked 取出);rest 以 FIELD_SEP 开头,
    # 其后才是正文与后续字段。
    if not rest.startswith(FIELD_SEP):
        return {"role": name, "content": rest}
    msg["role"] = name

    content_parts: List[str] = []
    for field in rest[len(FIELD_SEP):].split(FIELD_SEP):
        fname, fvalue = _split_marked(field)
        if fname is None:
            content_parts.append(field)
        elif fname == "tool_calls":
            msg["tool_calls"] = _parse_tool_calls(fvalue)
        elif fname == "tool_call_id":
            msg["tool_call_id"] = fvalue
        elif fname == "name":
            msg["name"] = fvalue
        elif fname == "extra":
            msg.update(json.loads(fvalue))
        else:
            content_parts.append(field)

    if content_parts:
        msg["content"] = _unescape(FIELD_SEP.join(content_parts))
    elif "tool_calls" in msg:
        # 只有工具调用的 assistant 消息,content 显式给 None 更符合协议
        msg["content"] = None
    else:
        msg["content"] = ""
    return msg


def _parse_tool_calls(text: str) -> List[Dict[str, Any]]:
    """把 ``name(args)`` 列表还原成 tool_calls 结构。"""
    calls: List[Dict[str, Any]] = []
    for item in text.split(FIELD_SEP):
        if not item:
            continue
        paren = item.find("(")
        if paren < 0 or not item.endswith(")"):
            calls.append({"type": "function",
                          "function": {"name": item, "arguments": "{}"}})
            continue
        name = item[:paren]
        args = item[paren + 1:-1]
        calls.append({"type": "function",
                      "function": {"name": name, "arguments": args}})
    return calls


# ---------------------------------------------------------------------------
# 便捷入口:bytes ⇄ bytes(给 Encoder 用)
# ---------------------------------------------------------------------------

def messages_to_text(raw_json: bytes) -> bytes:
    """请求体 JSON 字节 → 渲染文本字节。"""
    return render_request(json.loads(raw_json))


def text_to_messages(text: bytes) -> bytes:
    """渲染文本字节 → 请求体 JSON 字节。"""
    return json.dumps(unrender_request(text), ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
