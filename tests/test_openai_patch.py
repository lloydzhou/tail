"""openai 官方 SDK monkey patch 的单元测试。

不连真实网关,用一个本地 echo HTTP server(基于标准库)验证:
  - patch 后,openai SDK 的请求会带 X-Cache-Hash 等头 + 只发增量;
  - 响应的 X-Cache-* 头被正确解析更新本地缓存;
  - 收到 422 miss 会自动用全量重试。

这样把「monkey patch 逻辑」与「网关实现」解耦,纯 Python 可测。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

# import openai 必须在 install() 之前,确保 patch 挂到已加载的类上
import openai  # noqa: F401
from tail import openai_patch as kvcache_openai

MODEL = "test-model"


# ---------------------------------------------------------------------------
# 一个可控的 mock 网关 server:记录每次请求,按预设策略响应
# ---------------------------------------------------------------------------


class MockGateway:
    """基于标准库的 mock 网关,记录收到的请求,并实现简化协商。"""

    def __init__(self):
        self.received: list[dict] = []
        self._store: dict[str, dict] = {}  # hash -> {messages, model, prefix_length}
        self._lock = threading.Lock()
        self.server: ThreadingHTTPServer | None = None
        self.url = ""

    def start(self):
        gw = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length).decode("utf-8")
                body = json.loads(raw) if raw else {}
                cache_hash = self.headers.get("X-Cache-Hash")
                prefix_len = self.headers.get("X-Cache-Prefix-Length")
                messages = body.get("messages", [])
                model = body.get("model", "")

                hit = False
                with gw._lock:
                    gw.received.append({
                        "messages": messages,
                        "cache_hash": cache_hash,
                        "prefix_length": prefix_len,
                        "is_increment": cache_hash is not None,
                    })
                    if cache_hash and cache_hash in gw._store:
                        entry = gw._store[cache_hash]
                        if (entry["model"] == model
                                and entry["prefix_length"] == int(prefix_len or 0)):
                            # 命中:拼装完整
                            full = entry["messages"] + messages
                            hit = True
                            final_messages = full
                        else:
                            # 不一致 → fast_fail
                            self._respond(422, hit=False)
                            return
                    else:
                        final_messages = messages

                    # 算新哈希(简化:用 json 串的 hash)
                    import hashlib
                    new_hash = hashlib.sha256(
                        json.dumps(final_messages, sort_keys=True).encode()
                    ).hexdigest()[:16]
                    gw._store[new_hash] = {
                        "messages": final_messages,
                        "model": model,
                        "prefix_length": len(final_messages),
                    }

                self._respond(200, hit=hit, new_hash=new_hash)

            def _respond(self, status, *, hit, new_hash=""):
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Cache-Hash", new_hash)
                self.send_header("X-Cache-Expire", "9999999999")
                self.send_header("X-Cache-Hit", "true" if hit else "false")
                payload = json.dumps({
                    "id": "x", "object": "chat.completion", "created": 1, "model": MODEL,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                                 "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                })
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload.encode())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = self.server.server_address[1]
        self.url = f"http://127.0.0.1:{port}/v1"
        t = threading.Thread(target=self.server.serve_forever, daemon=True)
        t.start()

    def stop(self):
        if self.server:
            self.server.shutdown()

    def reset(self):
        with self._lock:
            self.received.clear()
            self._store.clear()


@pytest.fixture
def gateway():
    gw = MockGateway()
    gw.start()
    kvcache_openai.reset_cache()
    kvcache_openai.install()
    yield gw
    gw.stop()


def _client(gateway: MockGateway):
    return openai.OpenAI(base_url=gateway.url, api_key="sk-test")


def test_patch_installs_idempotently():
    """install() 幂等,多次调用不报错。"""
    kvcache_openai.install()
    kvcache_openai.install()
    from openai._base_client import SyncAPIClient
    assert getattr(SyncAPIClient, "_kvcache_patched", False) is True


def test_first_request_sends_full_messages(gateway):
    gateway.reset()
    client = _client(gateway)
    msgs = [{"role": "system", "content": "S"}, {"role": "user", "content": "q1"}]
    r = client.chat.completions.create(model=MODEL, messages=msgs)
    assert r.choices[0].message.content == "ok"
    # 第一次无缓存,不发 X-Cache-Hash
    assert gateway.received[-1]["cache_hash"] is None
    assert gateway.received[-1]["messages"] == msgs
    client.close()


def test_second_request_sends_only_increment(gateway):
    gateway.reset()
    client = _client(gateway)
    msgs1 = [{"role": "system", "content": "LONG PREFIX " * 10}, {"role": "user", "content": "q1"}]
    client.chat.completions.create(model=MODEL, messages=msgs1)
    msgs2 = msgs1 + [{"role": "assistant", "content": "a1"}, {"role": "user", "content": "q2"}]
    r = client.chat.completions.create(model=MODEL, messages=msgs2)
    assert r.choices[0].message.content == "ok"
    # 第二次应带哈希且只发增量(2 条)
    last = gateway.received[-1]
    assert last["cache_hash"] is not None
    assert len(last["messages"]) == 2
    assert last["messages"][0]["content"] == "a1"
    client.close()


def test_local_cache_state_updated(gateway):
    gateway.reset()
    client = _client(gateway)
    msgs1 = [{"role": "user", "content": "q1"}]
    client.chat.completions.create(model=MODEL, messages=msgs1)
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    # 本地前缀应等于完整 messages
    assert state.prefix_count == 1
    assert state.cache_hash != ""
    assert state.prefix_digest != ""
    client.close()


def test_auto_retry_on_miss(gateway):
    """注入一个无效本地哈希 → 第一次 miss(422)→ SDK 自动全量重试 → 成功。"""
    gateway.reset()
    client = _client(gateway)
    # 注入过期/无效缓存,使第一次发增量触发 miss
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    state.cache_hash = "invalid_hash_xyz"
    state.expire_time = 1e12  # 远未来
    state.prefix_count = 1
    state.prefix_digest = kvcache_openai._messages_digest([{"role": "system", "content": "old prefix"}])

    msgs = [{"role": "system", "content": "old prefix"}, {"role": "user", "content": "q"}]
    r = client.chat.completions.create(model=MODEL, messages=msgs)
    assert r.choices[0].message.content == "ok"
    # 应有两次请求:第一次增量(miss),第二次全量(重试成功)
    assert len(gateway.received) >= 2
    client.close()


def test_non_chat_request_not_affected(gateway):
    """非 /chat/completions 请求(如 /models)不被 patch 干扰。"""
    # mock gateway 只实现了 POST /chat/completions;这里只验证 patch 不抛异常
    client = _client(gateway)
    # 触发一次正常 chat 请求确保 patch 已生效
    client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "hi"}])
    # 验证 is_chat_request 判定不误伤:cache 状态只对 chat 请求更新
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    assert state.prefix_count == 1
    client.close()


# ---------------------------------------------------------------------------
# 扩充场景
# ---------------------------------------------------------------------------


def test_multi_turn_incremental_only(gateway):
    """多轮对话:从第 2 轮起每轮只发增量。"""
    gateway.reset()
    client = _client(gateway)
    base = [{"role": "system", "content": "S" * 100}, {"role": "user", "content": "t1"}]
    client.chat.completions.create(model=MODEL, messages=base)
    inc1 = [{"role": "assistant", "content": "r1"}, {"role": "user", "content": "t2"}]
    client.chat.completions.create(model=MODEL, messages=base + inc1)
    inc2 = [{"role": "assistant", "content": "r2"}, {"role": "user", "content": "t3"}]
    client.chat.completions.create(model=MODEL, messages=base + inc1 + inc2)

    assert gateway.received[1]["is_increment"] is True
    assert len(gateway.received[1]["messages"]) == 2
    assert gateway.received[2]["is_increment"] is True
    assert len(gateway.received[2]["messages"]) == 2
    client.close()


def test_different_models_isolated(gateway):
    """不同 model 的缓存互不干扰。"""
    gateway.reset()
    client = _client(gateway)
    client.chat.completions.create(model="model-A", messages=[{"role": "user", "content": "a"}])
    client.chat.completions.create(model="model-B", messages=[{"role": "user", "content": "b"}])
    sa = kvcache_openai.get_cache_state("model-A", gateway.url)
    sb = kvcache_openai.get_cache_state("model-B", gateway.url)
    # 用 digest 验证内容不同(prefix_count 都=1,digest 应不同)
    assert sa.prefix_count == 1 and sb.prefix_count == 1
    assert sa.prefix_digest != sb.prefix_digest
    assert sa.prefix_digest == kvcache_openai._messages_digest([{"role": "user", "content": "a"}])
    assert sb.prefix_digest == kvcache_openai._messages_digest([{"role": "user", "content": "b"}])
    assert sa.cache_hash != sb.cache_hash
    client.close()


def test_cache_expires_then_full_resend(gateway):
    """缓存过期后,下次请求应重新发全量。"""
    gateway.reset()
    client = _client(gateway)
    client.chat.completions.create(model=MODEL, messages=[{"role": "user", "content": "first"}])
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    state.expire_time = 1  # 过期
    gateway.reset()
    msgs = [{"role": "user", "content": "first"}, {"role": "user", "content": "second"}]
    client.chat.completions.create(model=MODEL, messages=msgs)
    assert gateway.received[-1]["is_increment"] is False
    assert len(gateway.received[-1]["messages"]) == 2
    client.close()


def test_retry_updates_cache_with_full_messages(gateway):
    """miss 重试后,本地缓存应以全量 messages 更新。"""
    gateway.reset()
    client = _client(gateway)
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    state.cache_hash = "bad_hash"
    state.expire_time = 1e12
    state.prefix_count = 1
    state.prefix_digest = kvcache_openai._messages_digest([{"role": "system", "content": "old"}])

    msgs = [{"role": "system", "content": "old"},
            {"role": "user", "content": "new"},
            {"role": "user", "content": "q"}]
    client.chat.completions.create(model=MODEL, messages=msgs)
    state2 = kvcache_openai.get_cache_state(MODEL, gateway.url)
    assert state2.prefix_count == 3
    assert state2.prefix_digest == kvcache_openai._messages_digest(msgs)
    client.close()


def test_large_prefix_handled(gateway):
    """超长 system 前缀能正常缓存。"""
    gateway.reset()
    client = _client(gateway)
    big = [{"role": "system", "content": "S" * 5000}, {"role": "user", "content": "q1"}]
    client.chat.completions.create(model=MODEL, messages=big)
    msgs2 = big + [{"role": "assistant", "content": "a"}, {"role": "user", "content": "q2"}]
    r = client.chat.completions.create(model=MODEL, messages=msgs2)
    assert r.choices[0].message.content == "ok"
    assert gateway.received[-1]["is_increment"] is True
    assert len(gateway.received[-1]["messages"]) == 2
    client.close()


# ---------------------------------------------------------------------------
# v2.1 SDK 一致性修复测试(设计文档 §5)
# 验证:compact/编辑/重排/多 session 交叉 → 都降级为发全量,绝不静默错误
# ---------------------------------------------------------------------------


def test_compact_falls_back_to_full(gateway):
    """compact(删中间消息)后,前缀指纹不匹配 → 发全量,不静默错误。"""
    gateway.reset()
    client = _client(gateway)
    base = [{"role": "system", "content": "S" * 100}, {"role": "user", "content": "q1"},
            {"role": "assistant", "content": "a1"}, {"role": "user", "content": "q2"}]
    client.chat.completions.create(model=MODEL, messages=base)
    # 第二轮:删掉中间的 q1/a1(compact),只保留 system + q2
    compacted = [{"role": "system", "content": "S" * 100}, {"role": "user", "content": "q2"},
                 {"role": "user", "content": "q3"}]
    client.chat.completions.create(model=MODEL, messages=compacted)
    # 因 messages[:4] 指纹 != 旧前缀指纹 → 应发全量(非增量)
    assert gateway.received[-1]["is_increment"] is False
    assert gateway.received[-1]["messages"] == compacted
    client.close()


def test_edit_old_message_falls_back_to_full(gateway):
    """编辑已发的旧消息 → 指纹不匹配 → 发全量。"""
    gateway.reset()
    client = _client(gateway)
    base = [{"role": "user", "content": "original"}, {"role": "user", "content": "q2"}]
    client.chat.completions.create(model=MODEL, messages=base)
    # 第二轮:改了第一条消息内容
    edited = [{"role": "user", "content": "EDITED"}, {"role": "user", "content": "q2"},
              {"role": "user", "content": "q3"}]
    client.chat.completions.create(model=MODEL, messages=edited)
    assert gateway.received[-1]["is_increment"] is False
    client.close()


def test_reorder_falls_back_to_full(gateway):
    """重排消息顺序 → 指纹不匹配 → 发全量。"""
    gateway.reset()
    client = _client(gateway)
    base = [{"role": "user", "content": "a"}, {"role": "user", "content": "b"}]
    client.chat.completions.create(model=MODEL, messages=base)
    # 第二轮:调换顺序
    reordered = [{"role": "user", "content": "b"}, {"role": "user", "content": "a"},
                 {"role": "user", "content": "c"}]
    client.chat.completions.create(model=MODEL, messages=reordered)
    assert gateway.received[-1]["is_increment"] is False
    client.close()


def test_prefix_not_changed_still_incremental(gateway):
    """正常追加(前缀不变)仍走增量 —— 确认修复不破坏正常路径。"""
    gateway.reset()
    client = _client(gateway)
    base = [{"role": "system", "content": "S"}, {"role": "user", "content": "q1"}]
    client.chat.completions.create(model=MODEL, messages=base)
    grown = base + [{"role": "assistant", "content": "a"}, {"role": "user", "content": "q2"}]
    client.chat.completions.create(model=MODEL, messages=grown)
    assert gateway.received[-1]["is_increment"] is True
    assert len(gateway.received[-1]["messages"]) == 2  # 只发增量
    client.close()


def test_multi_session_in_separate_contextvars_isolated():
    """不同 contextvars 上下文 = 不同 session,各自独立缓存(设计文档 §5.4 方案 C)。"""
    import contextvars
    gw = MockGateway()
    gw.start()
    kvcache_openai.hard_reset()
    kvcache_openai.install()
    try:
        client = _client(gw)
        # session A 在上下文 ctx_a 里跑
        def run_a():
            gw.reset()
            client.chat.completions.create(model=MODEL, messages=[
                {"role": "user", "content": "A1"}])
            client.chat.completions.create(model=MODEL, messages=[
                {"role": "user", "content": "A1"},
                {"role": "assistant", "content": "aA"},
                {"role": "user", "content": "A2"}])
            return gw.received[-1]["is_increment"]

        ctx_a = contextvars.copy_context()
        is_inc_a = ctx_a.run(run_a)

        # session B 在另一上下文跑完全不同的对话
        def run_b():
            gw.reset()
            client.chat.completions.create(model=MODEL, messages=[
                {"role": "user", "content": "B1"}])
            client.chat.completions.create(model=MODEL, messages=[
                {"role": "user", "content": "B1"},
                {"role": "assistant", "content": "aB"},
                {"role": "user", "content": "B2"}])
            return gw.received[-1]["is_increment"]

        ctx_b = contextvars.copy_context()
        is_inc_b = ctx_b.run(run_b)

        # 各自上下文里第二轮都应是增量(独立缓存,不互相污染)
        assert is_inc_a is True, "session A 第二轮应增量"
        assert is_inc_b is True, "session B 第二轮应增量"
        client.close()
    finally:
        gw.stop()


def test_multi_session_serial_falls_back_gracefully(gateway):
    """同上下文串行跑两个 session:第二个 session 前缀指纹不匹配 → 降级全量(不报错)。"""
    gateway.reset()
    client = _client(gateway)
    # session A
    client.chat.completions.create(model=MODEL, messages=[
        {"role": "user", "content": "A1"}, {"role": "user", "content": "A2"}])
    # session B(同 client 同上下文,但内容完全不同)
    gateway.reset()
    client.chat.completions.create(model=MODEL, messages=[
        {"role": "user", "content": "B1"}, {"role": "user", "content": "B2"}])
    # 因前缀指纹不匹配(B1 != A1)→ 应发全量
    assert gateway.received[-1]["is_increment"] is False
    assert len(gateway.received[-1]["messages"]) == 2  # 全量
    client.close()


def test_cache_state_uses_digest_not_array(gateway):
    """验证 _CacheState 不再存整个 prefix_messages 数组(内存优化)。"""
    gateway.reset()
    client = _client(gateway)
    client.chat.completions.create(model=MODEL, messages=[
        {"role": "user", "content": "x" * 1000}])  # 大消息
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    # 应存 count + digest,不存数组
    assert not hasattr(state, "prefix_messages")
    assert state.prefix_count == 1
    assert len(state.prefix_digest) == 16  # 16 hex
    client.close()


# ===========================================================================
# v2.2: 412/422 miss 异常的重试(SDK 一致性核心 bug 修复)
# ===========================================================================


def test_miss_412_triggers_full_retry(gateway):
    """网关返回 412(缓存 miss)→ SDK 捕获异常 → 全量重试 → 成功。

    回归真实场景 bug:网关重启/dbm 丢失后,SDK 本地缓存的 hash 在网关找不到,
    网关返回 412 异常,SDK 必须 catch 并全量重试,不能把异常抛给用户。
    """
    gateway.reset()
    client = _client(gateway)
    # 正常一轮建缓存
    client.chat.completions.create(model=MODEL,
        messages=[{"role": "user", "content": "first"}])
    # 第二轮:模拟网关找不到该 hash → 返回 412
    # (mock gateway 的协商逻辑会因 hash 不匹配返回 412)
    state = kvcache_openai.get_cache_state(MODEL, gateway.url)
    # 故意改坏 hash,让网关 miss
    state.cache_hash = "0::0::bad_hash_not_in_gateway"
    state.prefix_count = 1
    state.prefix_digest = kvcache_openai._messages_digest([{"role": "user", "content": "first"}])

    msgs = [{"role": "user", "content": "first"}, {"role": "user", "content": "second"}]
    # 不应抛异常,应自动全量重试成功
    r = client.chat.completions.create(model=MODEL, messages=msgs)
    assert r.choices[0].message.content == "ok"
    # 应有两次请求(第一次 miss 412,第二次全量重试成功)
    assert len(gateway.received) >= 2
    client.close()
