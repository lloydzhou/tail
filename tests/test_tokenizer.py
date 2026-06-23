"""单元测试:分词器封装。

对应设计文档第 5.2 节与 Phase 1。重点验证 SDK 与网关用同一逻辑时,
对相同 messages 会得到相同的 token 序列(缓存对齐的前提)。
"""

from tail.tokenizer import Tokenizer


def test_encode_is_deterministic():
    enc = Tokenizer()
    assert enc.encode("hello world") == enc.encode("hello world")


def test_encode_message_stable():
    enc = Tokenizer()
    m = {"role": "user", "content": "Hello there!"}
    assert enc.encode_message(m) == enc.encode_message(m)


def test_role_and_content_both_contribute():
    """role 不同或 content 不同,token 序列都应不同。"""
    enc = Tokenizer()
    a = enc.encode_message({"role": "user", "content": "hi"})
    b = enc.encode_message({"role": "system", "content": "hi"})
    c = enc.encode_message({"role": "user", "content": "ho"})
    assert a != b
    assert a != c


def test_concat_messages_matches_prefix():
    """前缀 [m0] 的 token 序列 == [m0, m1] 序列的前缀。

    这是协议正确性的核心不变量:前缀哈希可对齐。
    """
    enc = Tokenizer()
    m0 = {"role": "system", "content": "You are helpful."}
    m1 = {"role": "user", "content": "Hi"}
    prefix = enc.encode_messages([m0])
    full = enc.encode_messages([m0, m1])
    assert full[: len(prefix)] == prefix
    assert len(full) > len(prefix)


def test_model_specific_tokenizer():
    enc_default = Tokenizer()
    enc_gpt4o = Tokenizer.for_model("gpt-4o")
    enc_gpt35 = Tokenizer.for_model("gpt-3.5-turbo")
    # gpt-4o 用 o200k_base,可能与其他不同;至少对象独立。
    assert enc_default is not enc_gpt4o
    assert enc_gpt35 is not enc_gpt4o


def test_multimodal_content_does_not_crash():
    """content 为列表(多模态/工具)时应序列化参与编码,不抛异常。"""
    enc = Tokenizer()
    m = {"role": "assistant", "content": [{"type": "text", "text": "ok"}]}
    ids = enc.encode_message(m)
    assert isinstance(ids, list)
    assert len(ids) > 0
