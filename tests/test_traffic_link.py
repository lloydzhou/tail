"""真实轨迹与限速链路的测试。

两件事会被以后的改动悄悄弄坏,所以钉住:

1. **轨迹生成器真的会产生它声称的事件**(compact / branch / new_tools / 并行会话),
   而不是永远只产生 append——否则「真实场景」的数据其实是理想情况;
2. **限速中继真的按带宽计时**,且按请求切窗口。这条尤其重要:第一版实现里
   中继不限速、窗口按连接算,结果量出来全是 0 ms,数字看着正常但其实什么也没测。
"""

from __future__ import annotations

import socket
import threading
import time

import pytest

from tools.link import ThrottledRelay, upload_seconds
from tools.traffic import build_trace, payload_bytes, tool_defs


class TestTrace:
    def test_append_only_when_no_events(self):
        tr = build_trace(6)
        assert tr.kinds() == {"append": 6}
        sizes = [len(payload_bytes(t.payload("m"))) for t in tr.turns]
        assert sizes == sorted(sizes), "纯追加时请求体应该单调变大"
        assert sizes[-1] > sizes[0]

    def test_compact_shrinks_the_context(self):
        tr = build_trace(16, compact_every=8)
        assert tr.kinds().get("compact") == 1
        sizes = [len(payload_bytes(t.payload("m"))) for t in tr.turns]
        i = next(i for i, t in enumerate(tr.turns) if t.kind == "compact")
        assert sizes[i] < sizes[i - 1], "compact 之后上下文应该变小"

    def test_branch_marks_a_turn(self):
        tr = build_trace(12, branch_every=5)
        assert tr.kinds().get("branch", 0) >= 1
        for t in tr.turns:
            if t.kind == "branch":
                assert any("换个方向" in str(m.get("content"))
                           for m in t.messages if m["role"] == "user")

    def test_new_tools_changes_the_tool_defs(self):
        tr = build_trace(10, tools_at=4)
        tools = [len(t.tools) for t in tr.turns]
        i = next(i for i, t in enumerate(tr.turns) if t.kind == "new_tools")
        assert tools[i] > tools[i - 1], "new_tools 之后工具定义应该变多"
        assert tools[0] == len(tool_defs())

    def test_parallel_sessions_interleave(self):
        tr = build_trace(10, sessions=3)
        assert tr.sessions == 3
        assert len({t.session for t in tr.turns}) == 3
        # 交错发出:相邻两轮属于不同会话
        assert tr.turns[0].session != tr.turns[1].session

    def test_every_turn_is_valid_chat_payload(self):
        tr = build_trace(8, compact_every=4, branch_every=3, tools_at=3)
        for t in tr.turns:
            p = t.payload("m")
            assert p["messages"][0]["role"] == "system"
            assert all(m["role"] in ("system", "user", "assistant", "tool")
                       for m in p["messages"])
            assert p["tools"]


@pytest.fixture
def sink():
    """一个只收不看的 TCP 服务端,给中继当上游。"""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    stop = threading.Event()

    def accept() -> None:
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            threading.Thread(target=lambda c=conn: _drain(c), daemon=True).start()

    def _drain(c: socket.socket) -> None:
        try:
            while True:
                if not c.recv(65536):
                    return
        except OSError:
            return

    threading.Thread(target=accept, daemon=True).start()
    yield port
    stop.set()
    srv.close()


def _send_and_time(relay: ThrottledRelay, nbytes: int, wait: float) -> tuple:
    """往中继写 nbytes,返回 (收到的总字节, 上传窗口秒数)。

    限速时**不能**只等第一个窗口:一段几百毫秒的传输会切成好几个窗口,
    只看第一个就把字节数算少了(第一版测试就栽在这里)。
    """
    conn = socket.create_connection(("127.0.0.1", relay.port))
    conn.sendall(b"x" * nbytes)
    deadline = time.time() + wait
    total = 0
    start = end = 0.0
    while time.time() < deadline:
        for w in relay.take_windows(settle=0.0):
            total += w.up_bytes
            start = w.start if not start else min(start, w.start)
            end = max(end, w.end)
        if total >= nbytes:
            break
        time.sleep(0.02)
    conn.close()
    return total, (end - start if start else 0.0)


class TestThrottledLink:
    def test_upload_time_follows_bandwidth(self, sink):
        """同一批字节,带宽减半 → 上传时间应该差不多加倍。"""
        size = 128 * 1024
        times = {}
        for mbps in (50.0, 12.5):
            relay = ThrottledRelay("127.0.0.1", sink, mbps=mbps)
            relay.start()
            try:
                _got, secs = _send_and_time(relay, size, wait=6.0)
            finally:
                relay.stop()
            assert secs > 0, f"{mbps} Mbps 没量到上传窗口"
            times[mbps] = secs
        assert times[12.5] > times[50.0] * 1.8, times

    def test_measured_time_is_near_the_arithmetic(self, sink):
        """实测要和「字节 / 带宽」同量级,否则限速没有真的生效。

        留宽余量:每段读写的调度开销都会让实测偏高,但偏高一个量级就说明
        请求体其实是瞬间写进内核缓冲了(第一版就是这个 bug)。
        """
        size = 128 * 1024
        mbps = 10.0
        relay = ThrottledRelay("127.0.0.1", sink, mbps=mbps)
        relay.start()
        try:
            got, secs = _send_and_time(relay, size, wait=8.0)
        finally:
            relay.stop()
        want = upload_seconds(size, mbps)
        assert got == size, (got, size)
        assert want * 0.9 <= secs <= want * 3.0, (secs, want)

    def test_unlimited_link_is_fast(self, sink):
        """mbps=0:不限速,同样的字节应该快得多(纯回环只测 CPU)。"""
        size = 128 * 1024
        relay = ThrottledRelay("127.0.0.1", sink, mbps=0)
        relay.start()
        try:
            _got, secs = _send_and_time(relay, size, wait=3.0)
        finally:
            relay.stop()
        assert secs < upload_seconds(size, 10.0) / 4

    def test_two_requests_are_two_windows(self, sink):
        """串行两次上传要切成两个窗口,不能并成一个。

        这条钉的是第一版那个 bug:按连接计时,长连接上若干个请求会挤进一条记录。
        """
        relay = ThrottledRelay("127.0.0.1", sink, mbps=10.0)
        relay.start()
        try:
            first = socket.create_connection(("127.0.0.1", relay.port))
            first.sendall(b"a" * (48 * 1024))
            time.sleep(0.4)
            w1 = relay.take_windows(settle=0.0)
            first.close()
            time.sleep(0.1)

            second = socket.create_connection(("127.0.0.1", relay.port))
            second.sendall(b"b" * (48 * 1024))
            time.sleep(0.4)
            w2 = relay.take_windows(settle=0.0)
            second.close()
        finally:
            relay.stop()
        assert len(w1) == 1 and len(w2) == 1, (w1, w2)
        assert w1[0].up_bytes == 48 * 1024
        assert w2[0].up_bytes == 48 * 1024

    def test_partial_window_is_held_not_reported(self, sink):
        """还在传的窗口不能当成完整窗口交出去,否则字节数会偏小。"""
        relay = ThrottledRelay("127.0.0.1", sink, mbps=5.0)
        relay.start()
        try:
            conn = socket.create_connection(("127.0.0.1", relay.port))
            conn.sendall(b"c" * (256 * 1024))
            time.sleep(0.25)              # 5 Mbps 下 256 KB 要 0.4 s,还没传完
            early = relay.take_windows(settle=0.3)
            time.sleep(0.5)
            rest = relay.take_windows(settle=0.0)
            conn.close()
        finally:
            relay.stop()
        total = sum(w.up_bytes for w in early + rest)
        assert total == 256 * 1024, (early, rest)
