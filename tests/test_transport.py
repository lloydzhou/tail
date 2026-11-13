"""transport 层测试:分块、帧格式、渲染、编解码、存储、网关。

新协议要点(与 v1 的差异,详见 docs/PROTOCOL-v2.md):
  - 渲染器 / 反渲染器 / 编码器 / 解码器 **全部是流式的**
  - Content-Type 改为 application/x-tail-v2
  - 流式切块必须与一次性切分产生完全相同的块边界(缓存正确性的基础)
"""

from __future__ import annotations

import json
import os
import random

import pytest

from tail.transport import chunking as ck
from tail.transport import codec, frames, gateway as gw, render, storage


# ---------------------------------------------------------------------------
# 固定语料:确定性、非周期性。周期性内容切不出块(见 chunking 文档)。
# ---------------------------------------------------------------------------

_WORDS = ("分析 数据 结果 模型 输入 输出 评估 指标 样本 训练 推理 上下文 "
          "缓存 传输 协议 分块 网关 客户端 服务端 请求 响应").split()


def sent(rng: random.Random, n: int) -> str:
    return "".join(rng.choice(_WORDS) + ("，" if rng.random() < 0.3 else "")
                   for _ in range(n))


def make_text(seed: int = 0, turns: int = 30) -> str:
    rng = random.Random(seed)
    parts = [sent(rng, 300)]
    for i in range(turns):
        parts.append(f"问题{i}:" + sent(rng, rng.randint(20, 120)))
        parts.append(f"回答{i}:" + sent(rng, rng.randint(20, 120)))
    return "".join(parts)


def make_payload(turns: int = 10, seed: int = 0) -> dict:
    rng = random.Random(seed)
    msgs = [{"role": "system", "content": sent(rng, 200)}]
    for i in range(turns):
        msgs.append({"role": "user", "content": f"问题{i}:" + sent(rng, 50)})
        msgs.append({"role": "assistant", "content": f"回答{i}:" + sent(rng, 50)})
    return {"model": "deepseek-chat", "messages": msgs, "temperature": 0.7}


# ---------------------------------------------------------------------------
# 分块(vLLM 定长块)
# ---------------------------------------------------------------------------

class TestChunking:
    def test_split_covers_input_exactly(self):
        data = make_text().encode()
        segs = ck.split(data)
        assert b"".join(data[s:e] for s, e in segs) == data

    def test_empty_input(self):
        assert ck.split(b"") == []

    def test_fixed_size(self):
        """定长块:除最后一块外,每块都恰好 BLOCK_SIZE(vLLM 的做法)。"""
        data = os.urandom(100_000)
        segs = ck.split(data)
        for s, e in segs[:-1]:
            assert e - s == ck.BLOCK_SIZE
        assert segs[-1][1] - segs[-1][0] <= ck.BLOCK_SIZE

    def test_deterministic(self):
        data = make_text().encode()
        assert ck.split(data) == ck.split(data)

    def test_block_hash_is_content_addressed(self):
        assert ck.block_hash(b"hello") == ck.block_hash(b"hello")
        assert ck.block_hash(b"hello") != ck.block_hash(b"hellp")
        assert len(ck.block_hash(b"x")) == ck.BLOCK_ID_SIZE

    def test_block_hash_depends_on_parent(self):
        """vLLM 的 hash 链:同样的内容在不同前缀下 id 不同。

        这是"前缀缓存"语义的来源——块 id 标识的是「从开头到这里的整段前缀」,
        而不只是本块内容。
        """
        parent_a = ck.block_hash(b"prefix A")
        parent_b = ck.block_hash(b"prefix B")
        assert ck.block_hash(b"same", parent_a) != ck.block_hash(b"same", parent_b)


class TestPrefixReuse:
    """定长块的核心价值:多轮对话只在**尾部**新增块,前缀块 id 全部不变。

    这条保护的是「块边界只由位置决定」——这正是 vLLM / DeepSeek 的做法。
    定长块在**尾部追加**场景下表现最好(这正是 vLLM 的选择),
    而对话历史恰好就是尾部追加。
    """

    def _ids(self, data: bytes):
        return [bid for bid, _full in ck.blocked_hashes(data)]

    def test_append_only_grows_tail(self):
        base = make_text(seed=1, turns=100).encode()
        grown = base + sent(random.Random(9), 400).encode()
        before, after = self._ids(base), self._ids(grown)
        # 前缀块 id 全部相同
        assert after[:len(before) - 1] == before[:len(before) - 1]
        assert len(after) > len(before)

    def test_chinese_utf8_splits_evenly(self):
        data = make_text(seed=3, turns=100).encode()
        segs = ck.split(data)
        assert len(segs) >= 3
        for s, e in segs[:-1]:
            assert e - s == ck.BLOCK_SIZE


class TestBlockHasher:
    """流式切块必须与一次性切分**完全一致**,否则客户端与网关的块边界会不同。"""

    @pytest.mark.parametrize("seed", [1, 2, 3])
    def test_matches_one_shot(self, seed):
        rng = random.Random(seed)
        text = "".join(sent(rng, 15) for _ in range(1000)).encode()
        once = list(ck.split_blocks(text))
        for step in (1, 7, 256, 4096, len(text)):
            sc = ck.BlockHasher()
            got = []
            for i in range(0, len(text), step):
                got += [block for _bid, block in sc.feed(text[i:i + step])]
            got += [block for _bid, block in sc.flush()]
            assert got == once, f"粒度 {step} 下流式与一次性不一致"

    def test_hashes_match_blocked_hashes(self):
        """流式算出的块哈希必须与一次性 blocked_hashes 完全一致(含哈希链)。"""
        text = make_text(seed=2, turns=50).encode()
        once = ck.blocked_hashes(text)
        sc = ck.BlockHasher()
        got = list(sc.feed(text)) + list(sc.flush())
        assert [bid for bid, _ in got] == [bid for bid, _ in once]

    def test_empty(self):
        sc = ck.BlockHasher()
        assert list(sc.feed(b"")) == []
        assert list(sc.flush()) == []

    def test_small_input(self):
        """小于一块的输入:finish 冲刷出尾块,id 与一次性一致。"""
        sc = ck.BlockHasher()
        out = list(sc.feed(b"abc")) + list(sc.flush())
        assert [block for _bid, block in out] == [b"abc"]
        assert out[0][0] == ck.block_hash(b"abc")


# ---------------------------------------------------------------------------
# 帧格式
# ---------------------------------------------------------------------------

class TestFrames:
    def test_roundtrip_mixed(self):
        text = make_text().encode()
        pairs = ck.blocked_to_blocks(text)[:5]
        enc = [("new", pairs[0])] + [("ref", h) for h, _b in pairs[1:]]
        assert frames.decode_frames(frames.encode_frames(enc)) == enc

    def test_incremental_feed_arbitrary_boundaries(self):
        text = os.urandom(20000)
        enc = [("new", (h, b)) for h, b in ck.blocked_to_blocks(text)]
        wire = frames.encode_frames(enc)
        dec = frames.FrameDecoder()
        got = []
        for i in range(0, len(wire), 3):
            got += list(dec.feed(wire[i:i + 3]))
        assert got == enc and dec.eof_ok

    def test_end_frame_required(self):
        with pytest.raises(ValueError):
            frames.decode_frames(frames.encode_ref(ck.block_hash(b"x")))

    def test_ref_frame_is_17_bytes(self):
        assert len(frames.encode_ref(ck.block_hash(b"x"))) == 17

    def test_invalid_block_hash_length(self):
        with pytest.raises(ValueError):
            frames.encode_ref(b"short")

    def test_bad_frame_type(self):
        with pytest.raises(ValueError):
            frames.decode_frames(b"\x09" + b"\x00" * 20)


# ---------------------------------------------------------------------------
# 渲染 / 反渲染(流式)
# ---------------------------------------------------------------------------

class TestRender:
    def test_deterministic(self):
        payload = make_payload()
        assert render.render_request(payload) == render.render_request(payload)

    def test_deterministic_ignores_key_order(self):
        payload = make_payload()
        shuffled = dict(reversed(list(payload.items())))
        assert render.render_request(payload) == render.render_request(shuffled)

    def test_roundtrip_roles_and_content(self):
        payload = make_payload()
        back = render.unrender_request(render.render_request(payload))
        assert back["messages"] == payload["messages"]
        assert back["model"] == payload["model"]
        assert back["temperature"] == payload["temperature"]

    def test_roundtrip_multimodal(self):
        payload = {"model": "x", "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "看图"},
                {"type": "image_url", "image_url": {"url": "http://a/b.png"}},
            ]}]}
        content = render.unrender_request(render.render_request(payload))["messages"][0]["content"]
        assert "看图" in content and "http://a/b.png" in content

    def test_roundtrip_tool_calls(self):
        payload = {"model": "x", "messages": [
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c1", "type": "function",
                 "function": {"name": "get_time", "arguments": '{"tz":"UTC"}'}}]},
            {"role": "tool", "tool_call_id": "c1", "content": "12:00"}]}
        msgs = render.unrender_request(render.render_request(payload))["messages"]
        assert msgs[0]["tool_calls"][0]["function"]["name"] == "get_time"
        assert msgs[1]["tool_call_id"] == "c1"
        assert msgs[1]["content"] == "12:00"

    def test_roundtrip_tools_definition(self):
        tools = [{"type": "function", "function": {
            "name": "f", "description": "描述",
            "parameters": {"type": "object",
                           "properties": {"a": {"type": "string"}},
                           "required": ["a"]}}}]
        payload = {"model": "x", "messages": [{"role": "user", "content": "hi"}],
                   "tools": tools}
        assert render.unrender_request(render.render_request(payload))["tools"] == tools

    def test_content_with_control_chars(self):
        payload = {"model": "x", "messages": [
            {"role": "user", "content": "带\x1e分隔\x1f符\x00的正文\\反斜杠"}]}
        back = render.unrender_request(render.render_request(payload))
        assert back["messages"][0]["content"] == "带\x1e分隔\x1f符\x00的正文\\反斜杠"

    def test_plain_text_is_treated_as_content(self):
        """正文里没有版本头这类特殊结构,任意文本都应能安全解析。

        (早先版本在正文最前面放了一个 ``<|v2|>`` 版本标记;已移除——
        版本走 HTTP 头,不污染正文,否则升版会让第一个块失效。)
        """
        out = render.unrender_request(b"just some text")
        assert out == {}          # 没有 role 标记 → 没有 message

    def test_unknown_tag_is_kept_as_content(self):
        """正文里出现的未知 ``<|xxx|>`` 不能被当成结构。"""
        payload = {"model": "x", "messages": [
            {"role": "user", "content": "写一个 <|unknown|> 标记"}]}
        back = render.unrender_request(render.render_request(payload))
        assert "<|unknown|>" in back["messages"][0]["content"]

    def test_empty_input_is_valid(self):
        """空文本不是格式错误——所有块缺失时解码器会产出空。"""
        assert list(render.RenderParser().feed(b"")) == []
        assert list(render.RenderParser().finish()) == []


class TestStreamingRender:
    """渲染器与反渲染器都必须能吃任意字节边界的片段。"""

    @pytest.mark.parametrize("seed", range(6))
    def test_roundtrip_with_hostile_content(self, seed):
        """正文里带标记、引号、反斜杠、换行时,任意切块都要往返一致。

        这条是护栏:解析器为了"正文里的 ``<|user|>`` 不当结构"要跟踪 JSON
        字符串状态,一旦状态算错,简单测试用例抓不到(曾用 ``str.find`` 优化
        引号扫描,400 例模糊测试里 235 例流式解析出错,而单元测试全绿)。
        """
        rng = random.Random(seed)
        hostile = ["<|user|>", "<|end|>", "<|tool|>", '"引号"', "\\",
                   "\n", '<|model|>', 'a"b', '中\\"文', "🚀"]
        msgs = []
        for _ in range(rng.randint(1, 5)):
            role = rng.choice(["system", "user", "assistant", "tool"])
            m = {"role": role,
                 "content": "".join(rng.choice(hostile)
                                    for _ in range(rng.randint(0, 4)))}
            if role == "tool":
                m["tool_call_id"] = f"c{rng.randint(1, 9)}"
            msgs.append(m)
        payload = {"model": "m", "messages": msgs}
        text = render.render_request(payload)
        assert render.unrender_request(text) == payload
        for step in (1, 3, 17, 4096):
            parser = render.RenderParser()
            events = []
            for i in range(0, len(text), step):
                events += list(parser.feed(text[i:i + step]))
            events += list(parser.finish())
            got = [e[1] for e in events
                   if e[0] == render.EventKind.MESSAGE_END]
            assert got == msgs, f"切块粒度 {step} 下流式解析结果不一致"

    @pytest.mark.parametrize("step", [1, 2, 3, 7, 64])
    def test_parser_handles_any_byte_boundary(self, step):
        payload = make_payload(turns=5)
        text = render.render_request(payload)
        parser = render.RenderParser()
        events = []
        for i in range(0, len(text), step):
            events += list(parser.feed(text[i:i + step]))
        events += list(parser.finish())
        msgs = [e[1] for e in events if e[0] == render.EventKind.MESSAGE_END]
        assert msgs == payload["messages"]

    def test_parser_handles_split_multibyte_char(self):
        """UTF-8 多字节字符被切开时不能崩(增量解码器负责拼回)。"""
        payload = {"model": "x", "messages": [{"role": "user", "content": "中文测试"}]}
        text = render.render_request(payload)
        for step in (1, 2, 3):
            parser = render.RenderParser()
            events = []
            for i in range(0, len(text), step):
                events += list(parser.feed(text[i:i + step]))
            events += list(parser.finish())
            msgs = [e[1] for e in events if e[0] == render.EventKind.MESSAGE_END]
            assert msgs[0]["content"] == "中文测试"

    def test_iter_render_equals_render_request(self):
        payload = make_payload(turns=5)
        assert b"".join(render.iter_render(payload)) == render.render_request(payload)

    def test_render_is_lazy(self):
        """iter_render 不应该一次性算完——取第一个片段就应该能立刻返回。"""
        payload = make_payload(turns=100)
        gen = render.iter_render(payload)
        first = next(gen)
        assert first.startswith(b"<|")

    def test_large_payload_uses_constant_memory(self):
        """渲染大请求时峰值内存应与请求体大小无关。"""
        import tracemalloc
        payload = make_payload(turns=2000)
        tracemalloc.start()
        total = sum(len(p) for p in render.iter_render(payload))
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert peak < total * 0.2, f"峰值 {peak} 相对总量 {total} 过高(不流式)"


# ---------------------------------------------------------------------------
# 编解码
# ---------------------------------------------------------------------------

class TestCodec:
    def test_encode_decode_roundtrip(self):
        text = make_text().encode()
        enc = codec.Encoder()
        wire = enc.encode(text)
        src = codec.DictBlockSource()
        dec = codec.Decoder(src, on_new_block=lambda b, c: src.put_many({b: c}))
        out = b"".join(dec.feed(wire))
        dec.finish()
        assert out == text

    def test_stream_encode_matches_one_shot(self):
        text = make_text().encode()
        enc = codec.Encoder()
        one = enc.encode(text)
        enc2 = codec.Encoder()
        streamed = b"".join(enc2.iter_encode([text[i:i + 100]
                                              for i in range(0, len(text), 100)]))
        assert streamed == one

    def test_second_request_sends_only_increment(self):
        rng = random.Random(7)
        enc = codec.Encoder()
        text1 = make_text(seed=7, turns=400).encode()
        enc.encode(text1)
        text2 = text1 + sent(rng, 300).encode()
        wire2 = enc.encode(text2)
        assert len(wire2) < len(text2) * 0.1

    def test_known_blocks_become_refs(self):
        text = make_text().encode()
        enc = codec.Encoder()
        enc.encode(text)
        assert all(k == "ref" for k, _ in frames.decode_frames(enc.encode(text)))

    def test_cache_empty_still_works(self):
        text = make_text().encode()
        wire = codec.Encoder().encode(text)
        dec = codec.Decoder(codec.NullBlockSource())
        out = b"".join(dec.feed(wire))
        dec.finish()
        assert out == text and dec.ok

    def test_missing_blocks_degrade_not_error(self):
        text = make_text().encode()
        enc = codec.Encoder()
        enc.encode(text)
        dec = codec.Decoder(codec.NullBlockSource())
        out = b"".join(dec.feed(enc.encode(text)))
        dec.finish()
        assert out == b"" and dec.missing and not dec.ok

    def test_strict_mode_raises(self):
        text = make_text().encode()
        enc = codec.Encoder()
        enc.encode(text)
        dec = codec.Decoder(codec.NullBlockSource(), strict=True)
        with pytest.raises(codec.MissingBlock):
            list(dec.feed(enc.encode(text)))

    def test_encoder_forget_triggers_resend(self):
        text = make_text().encode()
        enc = codec.Encoder()
        wire1 = enc.encode(text)
        enc.forget({bid for bid, _full in ck.blocked_hashes(text)})
        assert enc.encode(text) == wire1


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------

class TestStorage:
    def test_memory_roundtrip(self):
        text = make_text().encode()
        st = storage.MemoryStorage()
        assert st.put_text(text) > 0
        # 存储只收满块,id 按 hash 链计算
        ids = [bid for bid, full in ck.blocked_hashes(text) if full]
        assert len(st.get_many(ids)) == len(ids)

    def test_memory_lru_eviction(self):
        st = storage.MemoryStorage(capacity=2)
        st.put_many({b"a" * 16: b"1", b"b" * 16: b"2", b"c" * 16: b"3"})
        assert len(st) == 2
        assert st.get_many([b"a" * 16]) == {}
        assert st.get_many([b"c" * 16]) == {b"c" * 16: b"3"}

    def test_dbm_roundtrip_and_persist(self, tmp_path):
        text = make_text().encode()
        path = str(tmp_path / "blocks.db")
        ids = [bid for bid, full in ck.blocked_hashes(text) if full]
        with storage.DbmStorage(path) as st:
            st.put_text(text)
            assert len(st.get_many(ids)) == len(ids)
        with storage.DbmStorage(path) as st:
            assert len(st.get_many(ids)) == len(ids)

    def test_make_storage(self, tmp_path):
        assert isinstance(storage.make_storage(""), storage.MemoryStorage)
        st = storage.make_storage(f"dbm:{tmp_path}/b.db")
        assert isinstance(st, storage.DbmStorage)
        st.close()
        with pytest.raises(ValueError):
            storage.make_storage("s3://bucket/key")


# ---------------------------------------------------------------------------
# 网关
# ---------------------------------------------------------------------------

class TestGateway:
    def test_wants_blocks(self):
        assert gw.TailGateway.wants_blocks("application/x-tail-v2")
        assert gw.TailGateway.wants_blocks("application/x-tail-v2; charset=utf-8")
        assert not gw.TailGateway.wants_blocks("application/json")
        assert not gw.TailGateway.wants_blocks(None)

    def test_full_roundtrip(self):
        payload = make_payload(turns=20)
        out = gw.roundtrip(payload, storage.MemoryStorage())
        assert out["messages"] == payload["messages"]
        assert out["model"] == payload["model"]

    def test_session_output_is_valid_json(self):
        payload = make_payload(turns=5)
        st = storage.MemoryStorage()
        wire = gw.TailClient().encode_request(payload)
        pieces = list(gw.TailGateway(st).iter_payload_json([wire]))
        assert json.loads(b"".join(pieces))["messages"] == payload["messages"]

    def test_streaming_output_equals_buffered(self):
        """流式 JSON 片段拼起来,与一次性结果完全一致。"""
        payload = make_payload(turns=8)
        st = storage.MemoryStorage()
        wire = gw.TailClient().encode_request(payload)
        streamed = b"".join(gw.TailGateway(st).iter_payload_json([wire]))
        back, _ = gw.TailGateway(st).collect_payload([wire])
        assert json.loads(streamed) == back

    def test_turn_by_turn_volume_decreases(self):
        rng = random.Random(11)
        msgs = [{"role": "system", "content": sent(rng, 200)}]
        st = storage.MemoryStorage()
        client = gw.TailClient()
        sizes, ctx = [], []
        for turn in range(1, 21):
            msgs.append({"role": "user", "content": f"问题{turn}:" + sent(rng, 40)})
            payload = {"model": "x", "messages": msgs}
            wire = client.encode_request(payload)
            session = gw.TailGateway(st).iter_payload_json([wire])
            assert json.loads(b"".join(session))["messages"] == msgs
            sizes.append(len(wire))
            ctx.append(sum(len(m["content"]) for m in msgs))
            msgs.append({"role": "assistant", "content": f"回答{turn}:" + sent(rng, 40)})
        recent = sum(sizes[10:]) / len(sizes[10:])
        assert recent < ctx[-1] * 0.4, f"后半段 {recent:.0f}B vs 上下文 {ctx[-1]}B"

    def test_storage_unavailable_degrades(self):
        payload = make_payload(turns=5)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)
        wire = client.encode_request(payload)
        st.clear()
        payload_out, session = gw.TailGateway(st).collect_payload([wire])
        assert session.missing
        assert session.status == gw.CACHE_DEGRADED
        assert payload_out == {}

    def test_client_invalidate_after_degrade(self):
        payload = make_payload(turns=5)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)
        client.invalidate()
        assert client.known_blocks == 0
        out, session = gw.TailGateway(st).collect_payload(
            [client.encode_request(payload)])
        assert out["messages"] == payload["messages"]
        assert not session.missing

    def test_hit_ratio(self):
        payload = make_payload(turns=6)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)
        _, session = gw.TailGateway(st).collect_payload(
            [client.encode_request(payload)])
        assert session.hit_ratio == 1.0
        assert session.new_block_count == 0
