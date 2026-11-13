"""transport 层测试:CDC 分块、帧格式、渲染、编解码、存储、网关。

覆盖 issue 里的验收标准(见 docs/ISSUE-text-dedup-transport.md)。
"""

from __future__ import annotations

import json
import os
import random

import pytest

from tail.transport import chunking as ck
from tail.transport import codec, frames, gateway as gw, render, storage


# ---------------------------------------------------------------------------
# 固定语料:确定性、非周期性。周期性内容会让 CDC 切不出块(见 chunking 文档),
# 用这种语料测局部性才有意义。
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
# CDC 分块
# ---------------------------------------------------------------------------

class TestChunking:
    def test_split_covers_input_exactly(self):
        data = make_text().encode()
        segs = ck.split(data)
        assert b"".join(data[s:e] for s, e in segs) == data

    def test_empty_input(self):
        assert ck.split(b"") == []

    def test_respects_min_size(self):
        data = os.urandom(200_000)
        segs = ck.split(data, avg_size=4096, min_size=1024, max_size=16384)
        # 最后一块可能小于 min_size
        for s, e in segs[:-1]:
            assert e - s >= 1024
            assert e - s <= 16384

    def test_average_size_near_target(self):
        data = os.urandom(500_000)
        segs = ck.split(data, avg_size=4096, min_size=1024, max_size=16384)
        avg = sum(e - s for s, e in segs) / len(segs)
        assert 2048 <= avg <= 10240, f"平均块长 {avg:.0f} 偏离 4KB 太多"

    def test_deterministic(self):
        data = make_text().encode()
        assert ck.split(data) == ck.split(data)

    def test_works_on_chinese_utf8(self):
        """回归测试:gear hash 在中文 UTF-8 上会产生 0 个切点,buzhash 不会。

        这条断言直接保护「换成 buzhash」这个决定。
        """
        data = make_text().encode()
        segs = ck.split(data)
        forced = [e - s for s, e in segs if e - s >= ck.DEFAULT_MAX_SIZE]
        # 不应有太多块是被 max_size 硬切出来的
        assert len(forced) <= len(segs) * 0.4, "切分过度依赖 max_size 兜底,哈希失效"

    def test_block_id_is_content_addressed(self):
        a = ck.block_id(b"hello")
        b = ck.block_id(b"hello")
        c = ck.block_id(b"hellp")
        assert a == b and a != c
        assert len(a) == ck.BLOCK_ID_SIZE


class TestLocality:
    """CDC 的核心价值:局部改动的块影响面有限。"""

    def _ids(self, data: bytes):
        return {ck.block_id(b) for b in ck.split_blocks(data)}

    def test_append_affects_at_most_one_block(self):
        """尾部追加的代价是常数:只有最后一块会被改写。

        文本越长,受影响的占比越小,所以断言"绝对损失块数 <= 1"
        而不是比值——短文本里 1 块占比天然很高。
        """
        base = make_text(seed=1, turns=100).encode()
        before = self._ids(base)
        after = self._ids(base + sent(random.Random(9), 200).encode())
        lost = len(before - after)
        assert lost <= 1, f"尾部追加改动了 {lost} 个块(应 <= 1)"

    def test_append_locality_improves_with_length(self):
        """文本越长,追加后的保持率越高(收敛到 100%)。"""
        ratios = []
        for turns in (30, 300, 1000):
            base = make_text(seed=1, turns=turns).encode()
            before = self._ids(base)
            after = self._ids(base + sent(random.Random(9), 200).encode())
            ratios.append(len(before & after) / len(before))
        assert ratios[0] < ratios[1] < ratios[2], ratios
        assert ratios[-1] >= 0.99, f"1MB 文本追加后保持 {ratios[-1]:.1%}"

    def test_middle_insert_keeps_most_blocks(self):
        base = make_text(seed=2).encode()
        before = self._ids(base)
        pos = len(base) // 2
        modified = base[:pos] + sent(random.Random(9), 100).encode() + base[pos:]
        after = self._ids(modified)
        ratio = len(before & after) / len(before)
        assert ratio >= 0.8, f"中间插入后仅 {ratio:.0%} 块保持(固定切分下会接近 0)"

    def test_middle_delete_keeps_most_blocks(self):
        base = make_text(seed=3).encode()
        before = self._ids(base)
        pos = len(base) // 2
        modified = base[:pos] + base[pos + 500:]
        after = self._ids(modified)
        ratio = len(before & after) / len(before)
        assert ratio >= 0.8, f"中间删除后仅 {ratio:.0%} 块保持"


# ---------------------------------------------------------------------------
# 帧格式
# ---------------------------------------------------------------------------

class TestFrames:
    def test_roundtrip_single(self):
        bid = ck.block_id(b"content")
        wire = frames.encode_ref(bid)
        out = frames.decode_frames(wire + frames.encode_end())
        assert out == [("ref", bid)]

    def test_roundtrip_mixed(self):
        text = make_text().encode()
        blocks = list(ck.split_blocks(text))[:5]
        enc = [("new", blocks[0])] + [("ref", ck.block_id(b)) for b in blocks[1:]]
        wire = frames.encode_frames(enc)
        assert frames.decode_frames(wire) == enc

    def test_incremental_feed_arbitrary_boundaries(self):
        text = os.urandom(20000)
        blocks = list(ck.split_blocks(text))
        enc = [("new", b) for b in blocks]
        wire = frames.encode_frames(enc)
        dec = frames.FrameDecoder()
        got = []
        for i in range(0, len(wire), 3):     # 3 字节一段,故意跨帧
            got += list(dec.feed(wire[i:i + 3]))
        assert got == enc
        assert dec.eof_ok

    def test_end_frame_required(self):
        bid = ck.block_id(b"x")
        with pytest.raises(ValueError):
            frames.decode_frames(frames.encode_ref(bid))    # 缺 END

    def test_ref_frame_is_17_bytes(self):
        assert len(frames.encode_ref(ck.block_id(b"x"))) == 17

    def test_invalid_block_id_length(self):
        with pytest.raises(ValueError):
            frames.encode_ref(b"short")

    def test_bad_frame_type(self):
        with pytest.raises(ValueError):
            frames.decode_frames(b"\x09" + b"\x00" * 20)


# ---------------------------------------------------------------------------
# 渲染 / 反渲染
# ---------------------------------------------------------------------------

class TestRender:
    def test_deterministic_same_input(self):
        payload = make_payload()
        assert render.render_request(payload) == render.render_request(payload)

    def test_deterministic_ignores_key_order(self):
        """键顺序不同必须渲染成相同字节,否则 block_id 会飘。"""
        payload = make_payload()
        shuffled = dict(reversed(list(payload.items())))
        assert render.render_request(payload) == render.render_request(shuffled)

    def test_deterministic_ignores_message_field_order(self):
        payload = make_payload()
        reordered = json.loads(json.dumps(payload))
        for m in reordered["messages"]:
            items = list(m.items())
            m.clear()
            m.update(reversed(items))
        assert render.render_request(payload) == render.render_request(reordered)

    def test_roundtrip_roles_and_content(self):
        payload = make_payload()
        text = render.render_request(payload)
        back = render.unrender_request(text)
        assert back["messages"] == payload["messages"]
        assert back["model"] == payload["model"]
        assert back["temperature"] == payload["temperature"]

    def test_roundtrip_multimodal_content(self):
        payload = {"model": "x", "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "看图"},
                {"type": "image_url", "image_url": {"url": "http://a/b.png"}},
            ]},
        ]}
        back = render.unrender_request(render.render_request(payload))
        content = back["messages"][0]["content"]
        assert "看图" in content
        assert "http://a/b.png" in content

    def test_roundtrip_tool_calls(self):
        payload = {"model": "x", "messages": [
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c1", "type": "function",
                 "function": {"name": "get_time", "arguments": '{"tz":"UTC"}'}},
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": "12:00"},
        ]}
        back = render.unrender_request(render.render_request(payload))
        msgs = back["messages"]
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
        back = render.unrender_request(render.render_request(payload))
        assert back["tools"] == tools

    def test_content_with_control_chars(self):
        payload = {"model": "x", "messages": [
            {"role": "user", "content": "带\x1e分隔\x1f符的正文"},
        ]}
        back = render.unrender_request(render.render_request(payload))
        assert back["messages"][0]["content"] == "带\x1e分隔\x1f符的正文"

    def test_text_is_smaller_for_many_short_messages(self):
        """短消息多时渲染应显著瘦身(结构开销占比高)。"""
        payload = {"model": "x",
                   "messages": [{"role": "user", "content": "短消息"}] * 500}
        raw = json.dumps(payload, ensure_ascii=False).encode()
        text = render.render_request(payload)
        assert len(text) < len(raw) * 0.6, f"渲染 {len(text)} vs JSON {len(raw)}"

    def test_missing_version_header_rejected(self):
        with pytest.raises(ValueError):
            render.unrender_request(b"not a rendered text")


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

    def test_second_request_sends_only_increment(self):
        """长对话的第二轮只发增量:线上体积应远小于全文。"""
        rng = random.Random(7)
        enc = codec.Encoder()
        text1 = make_text(seed=7, turns=300).encode()      # ~320KB
        enc.encode(text1)
        text2 = text1 + sent(rng, 300).encode()            # 追加约 2KB
        wire2 = enc.encode(text2)
        assert len(wire2) < len(text2) * 0.1, f"增量 {len(wire2)} vs 全文 {len(text2)}"

    def test_known_blocks_become_refs(self):
        text = make_text().encode()
        enc = codec.Encoder()
        enc.encode(text)
        wire = enc.encode(text)
        # 第二次全部应是引用帧
        frames_list = frames.decode_frames(wire)
        assert all(k == "ref" for k, _ in frames_list)

    def test_cache_empty_still_works(self):
        text = make_text().encode()
        enc = codec.Encoder()
        wire = enc.encode(text)
        dec = codec.Decoder(codec.NullBlockSource())
        out = b"".join(dec.feed(wire))
        dec.finish()
        assert out == text and dec.ok

    def test_missing_blocks_degrade_not_error(self):
        """已发出的引用在存储丢失后取不到:记录 missing,不抛异常。"""
        text = make_text().encode()
        enc = codec.Encoder()
        enc.encode(text)
        wire = enc.encode(text)          # 全是引用帧
        dec = codec.Decoder(codec.NullBlockSource())
        out = b"".join(dec.feed(wire))
        dec.finish()
        assert out == b""
        assert dec.missing and not dec.ok

    def test_strict_mode_raises(self):
        text = make_text().encode()
        enc = codec.Encoder()
        enc.encode(text)
        wire = enc.encode(text)
        dec = codec.Decoder(codec.NullBlockSource(), strict=True)
        with pytest.raises(codec.MissingBlock):
            list(dec.feed(wire))

    def test_encoder_forget_triggers_resend(self):
        text = make_text().encode()
        enc = codec.Encoder()
        wire1 = enc.encode(text)
        all_ids = {ck.block_id(b) for b in ck.split_blocks(text)}
        enc.forget(all_ids)
        wire2 = enc.encode(text)
        assert wire2 == wire1, "forget 之后应与首次编码一致"

    def test_streaming_memory_independent_of_size(self):
        """按小块喂入,解码器不会积攒整个 body。"""
        text = make_text(turns=100).encode()
        enc = codec.Encoder()
        wire = enc.encode(text)
        src = codec.DictBlockSource()
        dec = codec.Decoder(src, on_new_block=lambda b, c: src.put_many({b: c}))
        pieces = []
        for i in range(0, len(wire), 1024):
            pieces.extend(dec.feed(wire[i:i + 1024]))
        dec.finish()
        assert b"".join(pieces) == text


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------

class TestStorage:
    def test_memory_roundtrip(self):
        text = make_text().encode()
        st = storage.MemoryStorage()
        n = st.put_text(text)
        assert n > 0 and len(st) == n
        blocks = list(ck.split_blocks(text))
        got = st.get_many([ck.block_id(b) for b in blocks])
        assert len(got) == len(blocks)

    def test_memory_lru_eviction(self):
        st = storage.MemoryStorage(capacity=2)
        st.put_many({b"a" * 16: b"1", b"b" * 16: b"2", b"c" * 16: b"3"})
        assert len(st) == 2
        assert st.get_many([b"a" * 16]) == {}      # 最旧的被淘汰
        assert st.get_many([b"c" * 16]) == {b"c" * 16: b"3"}

    def test_dbm_roundtrip(self, tmp_path):
        text = make_text().encode()
        path = str(tmp_path / "blocks.db")
        with storage.DbmStorage(path) as st:
            st.put_text(text)
            blocks = list(ck.split_blocks(text))
            got = st.get_many([ck.block_id(b) for b in blocks])
            assert len(got) == len(blocks)
            assert got[ck.block_id(blocks[0])] == blocks[0]

    def test_dbm_persists_across_instances(self, tmp_path):
        path = str(tmp_path / "blocks.db")
        text = b"persist me " * 100
        with storage.DbmStorage(path) as st:
            st.put_text(text)
        with storage.DbmStorage(path) as st:
            blocks = list(ck.split_blocks(text))
            got = st.get_many([ck.block_id(b) for b in blocks])
            assert len(got) == len(blocks)

    def test_null_source_returns_nothing(self):
        assert storage.make_storage("").ping() is True   # 无 URL → Memory
        assert codec.NullBlockSource().get_many([b"x" * 16]) == {}

    def test_make_storage_dbm(self, tmp_path):
        st = storage.make_storage(f"dbm:{tmp_path}/b.db")
        assert isinstance(st, storage.DbmStorage)
        st.close()

    def test_make_storage_rejects_unknown(self):
        with pytest.raises(ValueError):
            storage.make_storage("s3://bucket/key")


# ---------------------------------------------------------------------------
# 网关
# ---------------------------------------------------------------------------

class TestGateway:
    def test_wants_blocks(self):
        assert gw.TailGateway.wants_blocks("application/x-tail-blocks")
        assert gw.TailGateway.wants_blocks("application/x-tail-blocks; charset=utf-8")
        assert not gw.TailGateway.wants_blocks("application/json")
        assert not gw.TailGateway.wants_blocks(None)

    def test_full_roundtrip_preserves_messages(self):
        payload = make_payload(turns=20)
        st = storage.MemoryStorage()
        out = gw.roundtrip(payload, st)
        assert out["messages"] == payload["messages"]
        assert out["model"] == payload["model"]

    def test_turn_by_turn_volume_decreases(self):
        """多轮对话里,线上体积相对全文应持续下降。"""
        rng = random.Random(11)
        msgs = [{"role": "system", "content": sent(rng, 200)}]
        st = storage.MemoryStorage()
        client = gw.TailClient()
        ratios = []
        for turn in range(1, 21):
            msgs.append({"role": "user", "content": f"问题{turn}:" + sent(rng, 40)})
            msgs.append({"role": "assistant", "content": f"回答{turn}:" + sent(rng, 40)})
            payload = {"model": "x", "messages": msgs}
            wire = client.encode_request(payload)
            res = gw.TailGateway(st).decode_stream([wire])
            text = b"".join(res)
            assert render.unrender_request(text)["messages"] == msgs
            ratios.append(len(wire) / len(text))
        assert ratios[-1] < ratios[0], f"体积比例未下降: {ratios[0]:.0%} → {ratios[-1]:.0%}"
        assert ratios[-1] < 0.5, f"第 20 轮仍占 {ratios[-1]:.0%}"

    def test_streaming_decode_in_small_chunks(self):
        """网关按任意字节边界喂入,结果一致。"""
        payload = make_payload(turns=10)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        wire = client.encode_request(payload)
        res = gw.TailGateway(st).decode_stream([wire[i:i + 97]
                                                for i in range(0, len(wire), 97)])
        text = b"".join(res)
        assert render.unrender_request(text)["messages"] == payload["messages"]

    def test_storage_unavailable_degrades(self):
        """存储清空后,客户端仍发引用帧 → 网关记录 degraded,不崩溃。"""
        payload = make_payload(turns=5)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)          # 第一轮建立缓存
        wire = client.encode_request(payload)   # 第二轮全是引用
        st.clear()
        res = gw.TailGateway(st).decode_stream([wire])
        text = b"".join(res)
        assert res.status == gw.CACHE_DEGRADED
        assert text == b""
        assert res.missing

    def test_client_invalidate_after_degrade(self):
        payload = make_payload(turns=5)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)
        client.invalidate()
        assert client.known_blocks == 0
        wire = client.encode_request(payload)
        res = gw.TailGateway(st).decode_stream([wire])
        text = b"".join(res)
        assert render.unrender_request(text)["messages"] == payload["messages"]

    def test_hit_ratio_reported(self):
        payload = make_payload(turns=6)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)
        res = gw.TailGateway(st).decode_stream([client.encode_request(payload)])
        list(res)
        assert res.hit_ratio == 1.0          # 第二轮全命中
        assert res.new_block_count == 0

    def test_renderer_change_causes_miss_not_error(self):
        """模板变更后旧缓存自然 miss,请求仍成功(验收标准之一)。"""
        payload = make_payload(turns=3)
        st = storage.MemoryStorage()
        client = gw.TailClient()
        client.encode_request(payload)
        # 模拟客户端换模板:清空已知集合,全部当新块发
        client.invalidate()
        res = gw.TailGateway(st).decode_stream([client.encode_request(payload)])
        text = b"".join(res)
        assert render.unrender_request(text)["messages"] == payload["messages"]
        assert not res.missing
