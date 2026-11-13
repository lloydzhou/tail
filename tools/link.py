"""一条**限速的**上游链路,按真实带宽计时两个 magpie 之间的上行。

字节数乘以带宽才是时间。这里把带宽变成实测的一部分:上行方向按漏桶限速,于是
「少传 90% 字节」直接体现成「上传快多少」。

**为什么要按时间窗口切分请求**,而不是一条连接算一次:

magpie 到中继这一段是**长连接**(它自己的连接池),一个连接上会连着发好几个请求。
所以中继不能按连接记,只能记「一串字节到达的时刻」,再按**空隙**切出每个请求的
上传窗口:一次上传是连续的,上传完到下一个请求开始之间必然有服务端往返的空隙。
`take_windows()` 就做这件事,返回每个请求的上传字节数与起止时刻。

限速的两件事缺一不可:

  1. **压小接收窗口**(`SO_RCVBUF`,要在 listen 之前设):否则请求体一股脑进内核
     缓冲,一秒就写完,再怎么在中继里 sleep 都只是「晚点读到」;
  2. **按小段读并按段睡**(`chunk`,默认 8 KB):一次 read 读完整包就量不出
     「传完用了多久」。

模拟范围:带宽与固定时延。不模拟丢包、拥塞控制、并发流竞争——要回答的是
「同样的链路条件下,两种传法各要多久」,不是链路仿真。
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class Window:
    """一个请求的上传窗口:从这一轮的第一个字节到最后一个字节。"""

    start: float
    end: float
    up_bytes: int

    @property
    def seconds(self) -> float:
        return self.end - self.start

    def as_dict(self) -> Dict[str, float]:
        return {"up_bytes": self.up_bytes,
                "upload_ms": round(self.seconds * 1000, 1)}


class ThrottledRelay:
    """限速中继:上行按 `mbps` 限速,并记录每个请求的上传窗口。"""

    def __init__(self, host: str, port: int, mbps: float = 0.0,
                 ts: float = 0.0, rcvbuf: int = 16 * 1024,
                 chunk: int = 8 * 1024) -> None:
        """
        Args:
          mbps: 上行带宽(Mbit/s);0 表示不限速(纯回环)
          ts: 上行每段之后额外加的固定时延(秒)
          rcvbuf: 接收窗口字节数(见模块说明:压小它限速才生效)
          chunk: 上行每段读多少字节(见模块说明:这是测量粒度)
        """
        self.target = (host, port)
        self.mbps = mbps
        self.ts = ts
        self.rcvbuf = rcvbuf
        self.chunk = chunk
        self.down_bytes = 0
        #: 上行字节到达的时刻与体积,(t, n)
        self._arrivals: List[Tuple[float, int]] = []
        self._mu = threading.Lock()
        self._port = _free_port()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._srv = None
        self._sock: Optional[socket.socket] = None

    @property
    def port(self) -> int:
        return self._port

    # -- 服务 -------------------------------------------------------------

    def _serve(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._listen())
        self._loop.run_forever()

    async def _listen(self) -> None:
        self._srv = None
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 窗口在握手时协商,accept 之后再设没用,所以要在 listen 之前
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.rcvbuf)
        sock.bind(("127.0.0.1", self._port))
        sock.listen(64)
        sock.setblocking(False)
        self._srv = await asyncio.start_server(self._handle, sock=sock)
        self._sock = sock

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        try:
            up_r, up_w = await asyncio.open_connection(*self.target)
        except OSError:
            writer.close()
            return

        async def pump(src, dst, upstream: bool) -> None:
            try:
                while True:
                    want = self.chunk if upstream else 65536
                    data = await src.read(want)
                    if not data:
                        break
                    if upstream:
                        with self._mu:
                            self._arrivals.append((time.perf_counter(), len(data)))
                    else:
                        with self._mu:
                            self.down_bytes += len(data)
                    dst.write(data)
                    await dst.drain()
                    if upstream and self.mbps > 0:
                        # 漏桶:这一段在这个带宽下该传多久就等多久,再读下一段
                        await asyncio.sleep(len(data) * 8 / (self.mbps * 1e6))
                    elif upstream and self.ts:
                        await asyncio.sleep(self.ts)
            except (ConnectionResetError, BrokenPipeError,
                    ConnectionAbortedError):
                pass
            finally:
                for s in (dst,):
                    try:
                        s.close()
                    except Exception:           # noqa: BLE001 — 关掉就好
                        pass

        await asyncio.gather(pump(reader, up_w, True), pump(up_r, writer, False))
        try:
            writer.close()
        except Exception:                       # noqa: BLE001
            pass

    # -- 观测 -------------------------------------------------------------

    def take_windows(self, gap: float = 0.08,
                     settle: float = 0.35) -> List[Window]:
        """取走已经完成的请求上传窗口。

        Args:
          gap: 两次到达相隔超过这么久,就算两个不同的请求
          settle: 最后一段之后要静默这么久,才认为这个窗口结束了(不再等)
        """
        now = time.perf_counter()
        with self._mu:
            arrivals, self._arrivals = self._arrivals, []
        if not arrivals:
            return []
        arrivals.sort(key=lambda a: a[0])
        windows: List[Window] = []
        start, end, total = arrivals[0][0], arrivals[0][0], 0
        pending: List[Tuple[float, int]] = []
        for t, n in arrivals:
            if t - end > gap:
                windows.append(Window(start, end, total))
                start, end, total = t, t, 0
            end, total = t, total + n
        # 最后一段还在等静默期:没静默够就留到下次
        if now - end < settle:
            pending = [(t, n) for t, n in arrivals if t >= start]
            windows_built = windows
        else:
            windows.append(Window(start, end, total))
            windows_built = windows
        if pending:
            with self._mu:
                self._arrivals = pending + self._arrivals
        return windows_built

    def start(self) -> None:
        self._thread.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self._port),
                                              timeout=1):
                    return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("限速中继没起来")

    def reset(self) -> None:
        with self._mu:
            self._arrivals = []
            self.down_bytes = 0

    def stop(self) -> None:
        """停掉中继:关 listener、断连接、让事件循环收尾。

        不这么做的话,收尾时 asyncio 会把还没跑完的 `_handle` 任务打成
        "Task was destroyed but it is pending",测试里满屏噪声。
        """
        async def shutdown() -> None:
            for t in asyncio.all_tasks():
                if t is not asyncio.current_task():
                    t.cancel()
            await asyncio.sleep(0)
        try:
            fut = asyncio.run_coroutine_threadsafe(shutdown(), self._loop)
            fut.result(timeout=3)
        except Exception:                   # noqa: BLE001 — 关得掉就行
            pass
        if self._srv is not None:
            self._loop.call_soon_threadsafe(self._srv.close)
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._loop.call_soon_threadsafe(self._loop.stop)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def upload_seconds(nbytes: int, mbps: float) -> float:
    """纯算术参照:这么多字节在这么多带宽下要传多久。"""
    if mbps <= 0:
        return 0.0
    return nbytes * 8 / (mbps * 1e6)
