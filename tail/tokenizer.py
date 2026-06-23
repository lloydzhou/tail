"""分词器封装 —— 基于 tiktoken。

对应设计文档第 5.2 节与 Phase 1「Tokenization 模块」。

重要约束:网关与 SDK 必须使用与后端一致(或等价)的分词器,否则前缀
哈希无法对齐,会导致缓存无法命中。生产 OpenResty 版本应集成模型对应
的分词器;本参考实现用 tiktoken 的 cl100k_base 作为默认占位。
"""

from __future__ import annotations

import threading
from typing import Iterable, List

import tiktoken

# 模型 -> tiktoken encoding 名称的映射。
# DeepSeek 等模型有自研 BPE,生产环境应按需替换为等价分词器。
_MODEL_ENCODING = {
    "gpt-4": "cl100k_base",
    "gpt-4o": "o200k_base",
    "gpt-3.5-turbo": "cl100k_base",
    "deepseek-chat": "cl100k_base",
}
_DEFAULT_ENCODING = "cl100k_base"

# role / content 之间的不可见分隔符,避免 "ab"+"c" == "a"+"bc" 类哈希歧义。
_FIELD_SEP = "\x00"


class Tokenizer:
    """线程安全的 tiktoken 封装,提供按消息列表编码的能力。"""

    def __init__(self, encoding_name: str = _DEFAULT_ENCODING):
        self._enc = tiktoken.get_encoding(encoding_name)
        self._lock = threading.Lock()

    @classmethod
    def for_model(cls, model: str) -> "Tokenizer":
        return cls(_MODEL_ENCODING.get(model, _DEFAULT_ENCODING))

    def encode(self, text: str) -> List[int]:
        return self._enc.encode(text)

    def encode_message(self, message: dict) -> List[int]:
        """把单条 OpenAI 消息编码为 token id 列表。

        编码方式:把 role 与 content 用分隔符拼成稳定文本再编码。
        生产中应与后端 tokenizer 的 chat 模板保持一致;此处用于哈希计算,
        只要 SDK 与网关采用同一逻辑即可保证对齐。
        """
        role = str(message.get("role", ""))
        content = message.get("content", "")
        if isinstance(content, list):
            # 多模态/工具消息:把结构序列化为稳定字符串后参与编码。
            content = repr(content)
        else:
            content = str(content)
        return self.encode(f"{role}{_FIELD_SEP}{content}")

    def encode_messages(self, messages: Iterable[dict]) -> List[int]:
        """按顺序拼接编码多条消息,得到前缀的 token id 序列。"""
        ids: List[int] = []
        for m in messages:
            ids.extend(self.encode_message(m))
        return ids
