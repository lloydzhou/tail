"""transport 层测试:分块、渲染、前缀凭据、链式存储、网关。

协议要点(详见 docs/TRANSPORT-PIPELINE.md):
  - 渲染 / 反渲染 / 切块 / 哈希写入 **全部是流式的**
  - Content-Type 为 application/x-tail-v3
  - 分块用 vLLM 式定长块 + 哈希链;**哈希的权威在网关**
  - 客户端只做只读校验:确认「前 L 字节没变」,不发任何哈希
"""

from __future__ import annotations

import json
import os
import random

import pytest

from tail.transport import chunking as ck
from tail.transport import codec, gateway as gw, render, storage


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
# 渲染 / 反渲染
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

    def test_split_right_after_backslash(self):
        """定向边界:正文以反斜杠结尾,且切块点**恰好落在反斜杠之后**。

        这时下一块的第一个引号可能是"被转义的",不是字符串边界。随机测试不保证
        命中这个位置,所以这里把每个切点都试一遍。

        回归:曾用 ``str.find`` 跳跃找引号,跨块时反斜杠状态丢失,
        400 例模糊测试里 235 例流式解析出错。
        """
        # 正文以反斜杠结尾(JSON 里是 "a\\"),再加一条含引号的消息
        payload = {"model": "m", "messages": [
            {"role": "user", "content": "a\\"},
            {"role": "assistant", "content": '他说"你好"然后走了'},
            {"role": "user", "content": '再次 "引号" 与 \\ 反斜杠'},
        ]}
        text = render.render_request(payload)
        assert render.unrender_request(text) == payload

        # 在每个位置切一刀,两段分别喂入,结果必须一致
        for cut in range(len(text) + 1):
            parser = render.RenderParser()
            events = list(parser.feed(text[:cut]))
            events += list(parser.feed(text[cut:]))
            events += list(parser.finish())
            got = [e[1] for e in events
                   if e[0] == render.EventKind.MESSAGE_END]
            assert got == payload["messages"], f"切点 {cut} 下解析结果不一致"

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

# ---------------------------------------------------------------------------
# 前缀凭据:客户端自检 + 网关续链
# ---------------------------------------------------------------------------

def _roundtrip(store, client, payload):
    """走一遍完整的 v3 往返,并把服务端确认的前缀写回客户端。"""
    prepared = client.prepare(payload)
    body = list(prepared.body)
    gateway = gw.TailGateway(store)
    out, session = gateway.collect_payload(iter(body), prepared.have_len,
                                           prepared.have_tip)
    client.set_credential(prepared.session, session.prefix_len, session.tip)
    return prepared, out, session, sum(map(len, body))


class TestPrefixClient:

    def test_cold_then_hit(self):
        """同一份 payload 第二次只发不满一块的尾巴。"""
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=8)

        p1, out1, s1, wire1 = _roundtrip(store, client, payload)
        assert p1.have_len == 0
        assert out1["messages"] == payload["messages"]
        assert s1.prefix_len > 0

        p2, out2, s2, wire2 = _roundtrip(store, client, payload)
        assert p2.have_len == s1.prefix_len
        assert out2["messages"] == payload["messages"]
        assert wire2 < wire1 // 10, (wire1, wire2)
        assert s2.hit_ratio > 0.9

    def test_append_only_extends_prefix(self):
        """纯追加:前缀只增不减,线上只走新增部分。"""
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=6)
        _, _, s1, _ = _roundtrip(store, client, payload)
        payload["messages"].append({"role": "user", "content": "追加的提问" * 40})
        p2, out2, s2, _ = _roundtrip(store, client, payload)
        assert p2.have_len == s1.prefix_len
        assert out2["messages"] == payload["messages"]
        assert s2.prefix_len > s1.prefix_len

    def test_edit_middle_falls_back_to_full(self):
        """改中间消息 → 前缀变了,客户端必须从 0 重发(否则静默拼错)。"""
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=6)
        _roundtrip(store, client, payload)
        payload["messages"][3]["content"] = "被改掉的回答" * 50
        prepared, out, session, wire = _roundtrip(store, client, payload)
        assert prepared.have_len == 0          # 凭据作废
        assert out["messages"] == payload["messages"]

    def test_changed_param_breaks_prefix(self):
        """顶层参数在正文最前面:改了它同样让前缀失效。"""
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=6)
        _roundtrip(store, client, payload)
        payload["temperature"] = 0.1
        prepared, out, session, _ = _roundtrip(store, client, payload)
        assert prepared.have_len == 0

    def test_session_key_is_content_addressed(self):
        """会话键 = 首块哈希:同样的开头 → 同样的键,不依赖客户端发明 id。"""
        a = gw.TailClient().prepare(make_payload(turns=3, seed=1))
        b = gw.TailClient().prepare(make_payload(turns=3, seed=1))
        c = gw.TailClient().prepare(make_payload(turns=3, seed=2))
        assert a.session == b.session and a.session != c.session
        assert len(a.session) == ck.BLOCK_ID_SIZE

    def test_credentials_are_bounded(self):
        """每会话只留有限个凭据(容忍并发响应乱序),不会无限涨。"""
        client = gw.TailClient(max_credentials=2)
        session = b"x" * 16
        for i in range(10):
            client.set_credential(session, 512 * (i + 1), bytes([i]) * 16)
        assert len(client._encoder.credentials(session)) == 2

    def test_invalidate_drops_credentials(self):
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=4)
        _roundtrip(store, client, payload)
        assert client.sessions == 1
        client.invalidate()
        assert client.sessions == 0
        prepared, _, _, _ = _roundtrip(store, client, payload)
        assert prepared.have_len == 0

    def test_stale_credential_raises_missing_prefix(self):
        """存储被清空:网关必须抛错过,而不是拼出错前缀。"""
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=6)
        _, _, s1, _ = _roundtrip(store, client, payload)
        store.clear()
        prepared = client.prepare(payload)
        assert prepared.have_len == s1.prefix_len
        with pytest.raises(codec.MissingPrefix):
            gw.TailGateway(store).collect_payload(
                iter(prepared.body), prepared.have_len, prepared.have_tip)


# ---------------------------------------------------------------------------
# 链式存储
# ---------------------------------------------------------------------------

class TestChainStore:

    def test_put_and_resolve(self):
        store = storage.MemoryChainStore()
        blocks = storage.text_blocks(b"a" * 512 + b"b" * 512, 512)
        tip = store.put_chain(b"", blocks)
        assert tip == blocks[-1][0]
        ids = store.resolve(tip, 2)
        assert ids == [blocks[0][0], blocks[1][0]]
        assert store.get_many(ids)[blocks[1][0]] == b"b" * 512

    def test_resolve_returns_none_when_middle_missing(self):
        """前缀必须完整可走:中间缺一块就是 miss,不能拼出洞。"""
        store = storage.MemoryChainStore()
        blocks = storage.text_blocks(b"a" * 512 + b"b" * 512 + b"c" * 512, 512)
        tip = store.put_chain(b"", blocks)
        store._data.pop(blocks[1][0], None)
        assert store.resolve(tip, 3) is None

    def test_resolve_partial_length(self):
        store = storage.MemoryChainStore()
        blocks = storage.text_blocks(b"a" * 512 * 3, 512)
        tip = store.put_chain(b"", blocks)
        assert store.resolve(tip, 1) == [blocks[-1][0]]

    def test_capacity_eviction(self):
        store = storage.MemoryChainStore(capacity=2)
        blocks = storage.text_blocks(b"a" * 512 * 3, 512)
        tip = store.put_chain(b"", blocks)
        assert store.resolve(tip, 3) is None      # 头部被淘汰
        assert store.resolve(tip, 2) is not None

    def test_make_store_default(self):
        assert isinstance(storage.make_store(""), storage.MemoryChainStore)

    def test_cached_store_serves_from_l1(self):
        """L1 命中时不该再问 L2(走链 1,600 跳的成本必须被吃掉)。"""
        calls = {"resolve": 0, "get_many": 0}

        class Spy(storage.MemoryChainStore):
            def resolve(self, tip, n_blocks):
                calls["resolve"] += 1
                return super().resolve(tip, n_blocks)

            def get_many(self, ids):
                calls["get_many"] += 1
                return super().get_many(ids)

        inner = Spy()
        store = storage.CachedChainStore(inner, capacity=1000)
        blocks = storage.text_blocks(b"a" * 512 * 4, 512)
        tip = store.put_chain(b"", blocks)
        ids = store.resolve(tip, 4)
        assert ids is not None
        before = dict(calls)
        assert store.resolve(tip, 4) == ids
        assert calls == before, "L1 应当直接命中,不碰 inner"
        assert store.get_many(ids)[ids[0]] is not None


# ---------------------------------------------------------------------------
# 网关协议层
# ---------------------------------------------------------------------------

class TestGateway:

    def test_wants_blocks(self):
        assert gw.TailGateway.wants_blocks(gw.CONTENT_TYPE_BLOCKS)
        assert not gw.TailGateway.wants_blocks("application/json")

    def test_have_header_roundtrip(self):
        tip = bytes(range(16))
        assert gw.decode_have(gw.encode_have(1536, tip)) == (1536, tip)
        assert gw.decode_have("") == (0, b"")
        assert gw.decode_have("垃圾") == (0, b"")
        assert gw.decode_have("1536;short") == (0, b"")

    def test_session_header_roundtrip(self):
        raw = bytes(range(16))
        assert gw.decode_session(raw.hex()) == raw
        assert gw.decode_session("zz") == b""
        assert gw.decode_session(None) == b""

    def test_full_roundtrip_keeps_content_and_params(self):
        store = storage.MemoryChainStore()
        payload = make_payload(turns=5)
        out = gw.roundtrip(payload, store)
        assert out["messages"] == payload["messages"]
        assert out["model"] == payload["model"]
        assert out["temperature"] == pytest.approx(payload["temperature"])

    def test_output_is_valid_json(self):
        store = storage.MemoryChainStore()
        client = gw.TailClient()
        prepared = client.prepare(make_payload(turns=3))
        raw = b"".join(gw.TailGateway(store).iter_payload_json(
            prepared.body, prepared.have_len, prepared.have_tip))
        assert json.loads(raw)["messages"]

    def test_streaming_equals_buffered(self):
        """逐片喂与一次性喂必须产出相同的 JSON。"""
        store = storage.MemoryChainStore()
        payload = make_payload(turns=4)
        client = gw.TailClient()
        prepared = client.prepare(payload)
        body = list(prepared.body)
        gateway = gw.TailGateway(store)
        a = b"".join(gateway.iter_payload_json(iter(body), 0, b""))
        pieces = [bytes([b]) for chunk in body for b in chunk]
        b = b"".join(gateway.iter_payload_json(iter(pieces), 0, b""))
        assert json.loads(a) == json.loads(b)

    def test_turn_by_turn_volume_decreases(self):
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=4)
        volumes = []
        for i in range(6):
            if i:
                payload["messages"].append(
                    {"role": "user", "content": f"第{i}轮问题" * 40})
                payload["messages"].append(
                    {"role": "assistant", "content": f"第{i}轮回答" * 40})
            _, _, _, wire = _roundtrip(store, client, payload)
            volumes.append(wire)
        # 每轮新增两条消息(约 1.2 KB),线上体积应稳定在新增量附近
        assert volumes[-1] < volumes[0] / 3, volumes
        assert volumes[-1] < 2000, volumes

    def test_hit_ratio(self):
        store, client = storage.MemoryChainStore(), gw.TailClient()
        payload = make_payload(turns=5)
        _, _, s1, _ = _roundtrip(store, client, payload)
        _, _, s2, _ = _roundtrip(store, client, payload)
        assert s1.hit_ratio < 0.01
        assert s2.hit_ratio > 0.9
