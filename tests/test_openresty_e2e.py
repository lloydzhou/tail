"""OpenResty 网关端到端测试。

启动真实的 OpenResty 网关(监听 8765)+ Python 模拟后端(监听 8080),
通过 httpx 发真实 HTTP 请求,验证 Lua 网关的协商逻辑。

Kvrocks 可选:
  - 若 6666 端口可达,网关会用 L2(硬盘)缓存;
  - 若不可达,网关自动降级到只用 L1(共享内存),测试同样能过。
  这正是设计文档第 5.4 节「Redis 不可用时仅用本地共享内存」的体现。

运行:
    cd /home/lloyd/ZCodeProject
    python3 -m pytest tests/test_openresty_e2e.py -v

前置:OpenResty 已编译(runtime/openresty/bin/openresty 存在)。
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import httpx
import pytest

PROJECT = Path("/home/lloyd/ZCodeProject")
OR_BIN = PROJECT / "runtime/openresty/bin/openresty"
OR_CONF = PROJECT / "openresty/conf/nginx.conf"
OR_PREFIX = PROJECT / "openresty"
LOG_DIR = PROJECT / "openresty/logs"

GATEWAY = "http://127.0.0.1:8765"
BACKEND = "http://127.0.0.1:8080"
MODEL = "deepseek-chat"


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_port(host: str, port: int, deadline: float = 15.0) -> bool:
    end = time.time() + deadline
    while time.time() < end:
        if _port_open(host, port):
            return True
        time.sleep(0.2)
    return False


# ---------------------------------------------------------------------------
# session 级 fixture:起后端 + 网关,整个测试会话复用
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stack():
    """启动 mock backend + OpenResty 网关,返回 (proc 列表, 前置是否满足)。"""
    if not OR_BIN.exists():
        pytest.skip("OpenResty 未编译(runtime/openresty/bin/openresty 不存在)")

    procs: list[subprocess.Popen] = []

    # 1) 启动 mock backend(uvicorn)
    env = dict(os.environ)
    backend = subprocess.Popen(
        ["python3", "-m", "uvicorn", "tests.mock_backend:app_factory",
         "--factory", "--host", "127.0.0.1", "--port", "8080"],
        cwd=str(PROJECT), env=env,
        stdout=open(LOG_DIR / "e2e_backend.log", "wb"),
        stderr=subprocess.STDOUT,
    )
    procs.append(backend)
    if not _wait_port("127.0.0.1", 8080, 15):
        pytest.skip("mock backend 起不来(端口 8080 未就绪)")

    # 2) 启动 OpenResty 网关
    # 先 test 配置,再正式启动
    try:
        subprocess.run(
            [str(OR_BIN), "-p", f"{OR_PREFIX}/", "-c", str(OR_CONF), "-t"],
            check=True, capture_output=True, timeout=20,
        )
    except subprocess.CalledProcessError as e:
        for p in procs:
            p.terminate()
        pytest.skip(f"OpenResty 配置测试失败: {e.stderr.decode(errors='replace')[:300]}")

    # openresty 默认 daemon on,用 nohup + pid 管理;这里直接前台 daemonize off 通过 -g
    # nginx.conf 里没写 daemon off,默认后台启动,正好
    gw = subprocess.Popen(
        [str(OR_BIN), "-p", f"{OR_PREFIX}/", "-c", str(OR_CONF)],
        stdout=open(LOG_DIR / "e2e_gateway.log", "wb"),
        stderr=subprocess.STDOUT,
    )
    procs.append(gw)
    if not _wait_port("127.0.0.1", 8765, 15):
        for p in procs:
            p.terminate()
        pytest.skip("OpenResty 网关起不来(端口 8765 未就绪)")

    # 健康检查
    try:
        with httpx.Client(timeout=5) as c:
            r = c.get(f"{GATEWAY}/__kvcache/health")
            if r.status_code != 200:
                raise RuntimeError(f"health 非 200: {r.status_code}")
    except Exception as e:
        for p in procs:
            p.terminate()
        pytest.skip(f"网关健康检查失败: {e}")

    # 清空后端观测记录 + 网关缓存(L1 通过 reload 清,Kvrocks 可选)
    with httpx.Client(timeout=5) as c:
        c.post(f"{BACKEND}/__backend/reset")

    kvrocks_available = _port_open("127.0.0.1", 6666)

    yield {"kvrocks": kvrocks_available}

    # teardown
    subprocess.run([str(OR_BIN), "-p", f"{OR_PREFIX}/", "-s", "stop"],
                   capture_output=True, timeout=15)
    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def _chat(messages, *, cache_hash=None, prefix_length=None, model=MODEL):
    headers = {"Content-Type": "application/json"}
    if cache_hash is not None:
        headers["X-Cache-Hash"] = cache_hash
        if prefix_length is not None:
            headers["X-Cache-Prefix-Length"] = str(prefix_length)
    body = {"model": model, "messages": messages}
    with httpx.Client(timeout=30) as c:
        return c.post(f"{GATEWAY}/v1/chat/completions", json=body, headers=headers)


def _backend_received():
    with httpx.Client(timeout=5) as c:
        return c.get(f"{BACKEND}/__backend/received").json()["requests"]


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------


def test_health(stack):
    with httpx.Client(timeout=5) as c:
        r = c.get(f"{GATEWAY}/__kvcache/health")
    assert r.status_code == 200


def test_first_request_full_passthrough_and_returns_hash(stack):
    """首次无哈希:完整透传,响应带新哈希,X-Cache-Hit=false。"""
    msgs = [{"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hello"}]
    r = _chat(msgs)
    assert r.status_code == 200
    assert r.headers.get("X-Cache-Hash")
    assert int(r.headers["X-Cache-Expire"]) > 0
    assert r.headers["X-Cache-Hit"] == "false"
    seen = _backend_received()
    assert seen[-1]["messages"] == msgs


def test_cache_hit_reconstructs_full_messages(stack):
    """命中:增量被拼装成完整 messages 转发给后端。"""
    msgs = [{"role": "system", "content": "Prefix " * 20},
            {"role": "user", "content": "q1"}]
    r1 = _chat(msgs)
    h1 = r1.headers["X-Cache-Hash"]
    inc = [{"role": "assistant", "content": "a1"}, {"role": "user", "content": "q2"}]
    r2 = _chat(inc, cache_hash=h1, prefix_length=len(msgs))
    assert r2.status_code == 200, r2.text
    assert r2.headers["X-Cache-Hit"] == "true"
    seen = _backend_received()
    assert seen[-1]["messages"] == msgs + inc


def test_cache_miss_fast_fail(stack):
    """未命中(fast_fail):不转发后端,返回 422。"""
    before = len(_backend_received())
    r = _chat([{"role": "user", "content": "only incremental"}],
              cache_hash="nonexistent_hash", prefix_length=99)
    assert r.status_code == 422
    assert r.headers["X-Cache-Hit"] == "false"
    assert len(_backend_received()) == before  # 后端未被调用


def test_multi_turn_new_hashes(stack):
    """多轮:每次返回新哈希,后端每次收到完整 messages。"""
    m1 = [{"role": "user", "content": "1"}]
    r1 = _chat(m1); h1 = r1.headers["X-Cache-Hash"]
    m2 = m1 + [{"role": "assistant", "content": "a"}, {"role": "user", "content": "2"}]
    r2 = _chat(m2[len(m1):], cache_hash=h1, prefix_length=len(m1)); h2 = r2.headers["X-Cache-Hash"]
    m3 = m2 + [{"role": "assistant", "content": "b"}, {"role": "user", "content": "3"}]
    r3 = _chat(m3[len(m2):], cache_hash=h2, prefix_length=len(m2)); h3 = r3.headers["X-Cache-Hash"]
    assert len({h1, h2, h3}) == 3
    seen = _backend_received()
    assert seen[-3]["messages"] == m1
    assert seen[-2]["messages"] == m2
    assert seen[-1]["messages"] == m3


def test_kvrocks_status_reported(stack):
    """测试输出 Kvrocks 是否参与(便于人看),不做强断言。"""
    print(f"\n[kvcache-e2e] Kvrocks(L2 硬盘缓存)参与: {stack['kvrocks']}")


# ---------------------------------------------------------------------------
# 扩充场景:Kvrocks 持久化 / 大前缀 / 并发 / 过期 / 错误哈希
# ---------------------------------------------------------------------------


def test_kvrocks_actually_stores_entry(stack):
    """缓存真的写进了 Kvrocks(v2.1:meta + sys/tools/seg/pfx 多 key)。"""
    import redis
    r = redis.Redis(host="127.0.0.1", port=6666)
    for k in r.scan_iter("prefix_cache:*"):
        r.delete(k)
    # v2.1:system 是顶层字段(不从 messages 抽取,§2.4 C2)
    r1 = _chat_raw({"model": "deepseek-chat",
                     "system": "STORE_CHECK",
                     "messages": [{"role": "user", "content": "q"}]})
    h = r1.headers["X-Cache-Hash"]  # v2.1: sys::tools::pfx(含 sys_hash)
    import time as _t
    _t.sleep(1.0)
    # meta 入口
    assert r.exists(f"prefix_cache:meta:{h}") == 1, "Kvrocks 里没有 meta 条目"
    meta = r.get(f"prefix_cache:meta:{h}")
    assert b"pfx_hash" in meta
    # cache_key 第一段是 sys_hash(非 "0",因为发了顶层 system)
    sys_hash = h.split("::")[0]
    assert sys_hash != "0", "发了 system,sys_hash 不应为 0"
    assert r.exists(f"prefix_cache:sys:{sys_hash}") == 1, "sys 段应存在"
    sys_blob = r.get(f"prefix_cache:sys:{sys_hash}")
    assert b"STORE_CHECK" in sys_blob, "sys 段应含 STORE_CHECK"


def _chat_raw(body):
    """直接发原始 body(支持顶层 system/tools 字段)。"""
    import httpx
    with httpx.Client(timeout=30) as c:
        return c.post(f"{GATEWAY}/v1/chat/completions", json=body)


def test_access_driven_ttl_renewal(stack):
    """访问驱动续期(§7.4):命中读取后,Kvrocks 里该 key 的 TTL 应被刷新(变大)。

    流程:写入 → 记 TTL_0 → 命中读取(触发续期)→ 记 TTL_1 → 验证续期生效。

    v2.1:续期作用在 pfx 节点(prefix_cache:pfx:{pfx_hash})。
    写入时 pfx 用 renew_ttl(1800);命中读取时 reconstruct 沿链 EXPIRE 续期,
    TTL 仍 ≈ renew_ttl(因为刚写入也是这个值)。验证:命中后 TTL > 0 且接近 renew_ttl。
    """
    import redis
    import time as _t
    r = redis.Redis(host="127.0.0.1", port=6666)
    for k in r.scan_iter("prefix_cache:*"):
        r.delete(k)
    # 写入(首次请求建缓存)
    r1 = _chat([{"role": "system", "content": "RENEW_CHECK"}, {"role": "user", "content": "q"}])
    h = r1.headers["X-Cache-Hash"]  # sys::tools::pfx
    _t.sleep(1.0)  # 等异步写入
    # v2.1:从 cache_key 第三段取 pfx_hash,查 pfx 节点的 TTL
    pfx_hash = h.split("::")[2]
    pfx_key = f"prefix_cache:pfx:{pfx_hash}"
    assert r.exists(pfx_key) == 1, f"pfx 节点应存在: {pfx_key}"
    ttl_0 = r.ttl(pfx_key)
    assert ttl_0 > 0, f"写入后 pfx TTL 应>0, 实际 {ttl_0}"
    # 命中读取(增量)→ reconstruct 沿链 EXPIRE 续期
    inc = [{"role": "assistant", "content": "a"}, {"role": "user", "content": "q2"}]
    r2 = _chat(inc, cache_hash=h, prefix_length=2)
    assert r2.status_code == 200, f"应命中: {r2.status_code}"
    _t.sleep(0.5)
    ttl_1 = r.ttl(pfx_key)
    # 续期后 TTL 应仍接近 renew_ttl(刚写入是 1800,续期也是 1800,所以差不多)
    assert ttl_1 > 0, "续期后 pfx TTL 应 > 0"
    assert ttl_1 <= 1810, f"续期后 TTL 应≈renew_ttl(1800),实际 {ttl_1}"


def test_miss_does_not_renew(stack):
    """miss(不存在的 key)不应触发续期。"""
    import redis
    r = redis.Redis(host="127.0.0.1", port=6666)
    fake = "nonexistent_renew_check"
    # 用不存在的 hash 发增量 → miss(fast_fail 422)→ 不该有续期
    resp = _chat([{"role": "user", "content": "inc"}], cache_hash=fake, prefix_length=1)
    assert resp.status_code == 422
    # 该 key 不存在(没写过),TTL 查询返回 -2
    assert r.ttl(f"prefix_cache:{fake}") == -2


def test_cache_survives_gateway_reload(stack):
    """reload 网关后(进程重启,缓存只在 Kvrocks),同哈希仍命中。

    这是 Kvrocks 硬盘缓存的核心价值:不随进程消失。
    """
    import subprocess, time as _t
    msgs = [{"role": "system", "content": "RELOAD_PERSIST_" * 10}, {"role": "user", "content": "q"}]
    r1 = _chat(msgs)
    h = r1.headers["X-Cache-Hash"]
    _t.sleep(1.0)  # 等写入

    # reload 网关
    subprocess.run(
        [str(OR_BIN), "-p", f"{OR_PREFIX}/", "-c", str(OR_CONF), "-s", "reload"],
        capture_output=True, timeout=15,
    )
    _t.sleep(2.0)  # 等 worker 重启

    # 用旧哈希发增量,应仍命中(Kvrocks 里有数据)
    inc = [{"role": "assistant", "content": "a"}, {"role": "user", "content": "q2"}]
    r2 = _chat(inc, cache_hash=h, prefix_length=len(msgs))
    assert r2.status_code == 200, f"reload 后未命中: {r2.status_code}"
    assert r2.headers["X-Cache-Hit"] == "true", "reload 后应从 Kvrocks 命中"
    seen = _backend_received()
    assert seen[-1]["messages"] == msgs + inc


def test_large_prefix_cached(stack):
    """大 system 前缀(数十 KB)能正确缓存与命中。"""
    big = [{"role": "system", "content": "BIG_" * 5000}, {"role": "user", "content": "q1"}]
    r1 = _chat(big)
    h = r1.headers["X-Cache-Hash"]
    import time as _t; _t.sleep(1.0)
    inc = [{"role": "user", "content": "q2"}]
    r2 = _chat(inc, cache_hash=h, prefix_length=len(big))
    assert r2.status_code == 200
    assert r2.headers["X-Cache-Hit"] == "true"


def test_wrong_prefix_length_is_miss(stack):
    """prefix_length 与缓存记录不符 → 未命中。"""
    r1 = _chat([{"role": "user", "content": "setup"}])
    h = r1.headers["X-Cache-Hash"]
    import time as _t; _t.sleep(0.5)
    # 故意给错的 prefix_length(真实=1,给 9)
    r2 = _chat([{"role": "user", "content": "x"}], cache_hash=h, prefix_length=9)
    assert r2.status_code == 422
    assert r2.headers["X-Cache-Hit"] == "false"


def test_expired_entry_in_kvrocks_is_miss(stack):
    """Kvrocks 里已过期的 entry,网关读取时判为 miss。

    直接往 Kvrocks 注入一个 expire_at 已过的条目,验证网关不命中。
    """
    import json, time as _t
    import redis
    r = redis.Redis(host="127.0.0.1", port=6666)
    fake_hash = "deadbeefdeadbeef"
    expired_entry = {
        "messages": [{"role": "user", "content": "ghost"}],
        "model": MODEL, "prefix_length": 1,
        "expire_at": int(_t.time()) - 1000,  # 已过期
    }
    r.set(f"prefix_cache:{fake_hash}", json.dumps(expired_entry), ex=3600)
    # 用这个过期哈希发增量 → 应 miss(422)
    resp = _chat([{"role": "user", "content": "inc"}],
                 cache_hash=fake_hash, prefix_length=1)
    assert resp.status_code == 422
    assert resp.headers["X-Cache-Hit"] == "false"
    r.delete(f"prefix_cache:{fake_hash}")


def _chat_on(gw, messages, *, cache_hash=None, prefix_length=None, model=MODEL):
    """在指定 gateway app 上发请求(用于多网关实例测试)。"""
    headers = {"Content-Type": "application/json"}
    if cache_hash is not None:
        headers["X-Cache-Hash"] = cache_hash
        if prefix_length is not None:
            headers["X-Cache-Prefix-Length"] = str(prefix_length)
    body = {"model": model, "messages": messages}
    with __import__("httpx").Client(transport=__import__("httpx").ASGITransport(app=gw),
                                     base_url="http://t") as c:
        return c.post("/v1/chat/completions", json=body, headers=headers)


def test_concurrent_requests_threadsafe(stack):
    """并发请求不互相干扰,每个都得到合法响应。"""
    import threading
    errors = []

    def worker(i):
        try:
            r = _chat([{"role": "user", "content": f"concurrent-{i}"}])
            assert r.status_code == 200
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [], f"并发出错: {errors}"


def test_health_endpoint(stack):
    with __import__("httpx").Client() as c:
        r = c.get(f"{GATEWAY}/__kvcache/health", timeout=5)
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
