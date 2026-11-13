# Tail —— 在 SDK 与上游之间插入一层「文本去重传输层」

> **Tail** — *Send the tail, the head is cached.*
>
> 名字取自 `tail -f`：只看新增的那几行。把同一个心智模型用到 LLM 请求上——
> 前缀已经在网关里了，客户端只发增量。

## 为什么值得做

DeepSeek 已经把上下文缓存的命中率做到 **99%+** —— 上游其实不需要我把前面的内容再发一遍。

可是缓存省掉的是**上游的算力**，省不掉我的**上行带宽**和**等待时间**：SDK 每一轮
仍然把完整 `messages` 序列化、打包、上传一次。重复的东西，公网上再走一遍。

所以网关这一跳值得优化。注意收益落在**两拨人**身上，不要混为一谈：
**用户省时间，服务商省钱。**

### 对用户：省的是时间，不是钱

要说清楚：**那点上行流量对用户根本不是成本**。800 KB × 20 轮 = 16 MB，按公网价
也就一分多钱，没人会为这个换方案。

用户真正有感的只有一件事：**每次请求要等多久**。而请求「发完」是 TTFT 的前置段——
链路上行越窄，这段等待越明显。限速模拟公网实测：

| 上行带宽 | 原生 | Tail | 每轮省下 |
|---|---|---|---|
| 10 Mbps | 733 ms | **45 ms** | 0.69 s |
| 5 Mbps | 1,461 ms | **63 ms** | 1.4 s |
| 2 Mbps | 3,643 ms | **135 ms** | 3.5 s |

一个每天 200 轮的开发者：10 Mbps 下一天少等 **2.3 分钟**，一年 **约 14 小时**；
2 Mbps（移动网络、跨境链路）下一年 **约 70 小时**。

**上行少一个数量级，等待就少一个数量级。** 这是 Tail 对用户的全部价值。

### 对服务提供商：同样的带宽，能多撑 26 倍的人

带宽成本落在**服务提供方**这一侧——入口流量、跨区链路、带宽峰值容量，
都是按 GB 和带宽峰值计费的。这是基础设施层面的账，所以按服务商量级算：
**DAU 十万 / 百万 / 千万，人均 20 轮/天，单轮按 800 KB 上限**。

**算法一：用户数不变，上行流量降 96%**

| 规模 | 原生上行/天 | 用 Tail | 每月省下（¥0.10/GB · ¥0.80/GB） |
|---|---|---|---|
| 10 万 DAU · 200 万请求/天 | 1.6 TB | 60 GB | 46 TB（¥4,600 · ¥3.7 万） |
| 100 万 DAU · 2,000 万请求/天 | 16 TB | 600 GB | 462 TB（¥4.6 万 · ¥37 万） |
| 1000 万 DAU · 2 亿请求/天 | 160 TB | 6 TB | 4.6 PB（**¥46 万 · ¥370 万**） |

两档单价都列出来是有意的：按量价 ¥0.80/GB 乘 PB 级流量会得出荒谬数字——
4.6 PB/月 不可能按 0.8 元/GB 结算。量越大单价越低，但**省下的绝对值仍落在几十万到数百万/月这一档**。

**算法二：同样的带宽预算，能承载 26 倍的业务量**

真正掏钱的不是「一天多少 TB」，是**常驻带宽**（按 Gbps 包月/包年买，
峰值决定买多大）。同一个量级换算过来：

| DAU | 原生 常驻 / 峰值（峰均比 3×） | 用 Tail 常驻 / 峰值 |
|---|---|---|
| 10 万 | 148 Mbps / 444 Mbps | 5.6 Mbps / 17 Mbps |
| 100 万 | 1.5 Gbps / 4.4 Gbps | 56 Mbps / 167 Mbps |
| 1000 万 | **14.8 Gbps / 44 Gbps** | **0.56 Gbps / 1.7 Gbps** |

千万 DAU 这一行是整件事的份量所在：原生要 **44 Gbps** 入口上行——那是几十个
100G 交换口、跨区专线和运营商带宽包的账，而且要按峰值签；Tail 只要 **1.7 Gbps**，
一根 10G 口都吃不满。**同样的业务量，入口带宽差了 26 倍。**

**存储不是对价**：新增块按用户当天真正打的字计——乐观 40 KB/人/天（多轮增量），
千万 DAU 约 400 GB/天；悲观按整天全新上下文 800 KB/人/天，约 8 TB/天。
配 3 天 TTL，占用 **1.2 TB ~ 24 TB**，ESSD 约 **¥600 ~ ¥1.2 万/月**。
对比每月省下的 ¥46 万以上，是个零头；而且存储有 TTL 会自动回收，上行流量不会。

**一个诚实的推论**：瓶颈会**从带宽转到网关 CPU**。2 亿请求/天均值 2,315 req/s、
峰值约 7,000 req/s，按 55 req/s/核算约 **127 核**——几台机器的事，和推理集群比可以忽略。
这就是供应商真正拿到的杠杆：以前扩容是「买带宽」（贵、受运营商和机房约束、要按峰值签），
现在是「加核」（便宜、随时、线性）。即使全按最低的 ¥0.10/GB 结算、把网关和存储都算进去，
省下的钱仍是自建成本的 **20 倍以上**。

**所以这是个双边收益的东西**：用户拿走时间，服务商拿走容量（或成本）。
任何一方单独看，都不足以支撑这个复杂度；两边加起来才成立。

### 代价（诚实版）

| 项 | 量 |
|---|---|
| 客户端额外 CPU | 3.5 ms / 800 KB 请求 |
| 网关额外 CPU | 18 ms / 800 KB 请求（线性，约 55 req/s/核） |
| 固定引用开销 | 每 512 B 块付 17 B 引用帧 ≈ **上下文的 3.3%**，这是本设计的理论下限 |
| 回环上反而更慢 | 45 ms vs 7.6 ms —— 回环没有带宽瓶颈，收益只在真实链路上出现 |

## 它解决什么问题

一个 100 轮、每轮 1K token 的对话，到第 100 轮时请求体约 400KB，其中 99% 是前 99 轮
已经传过的内容。Tail 把这些重复内容换成了 16 字节的引用。

实测（20 轮中文对话，每轮追加一轮问答）：

| 轮次 | 上下文文本 | 线上体积 | 占比 |
|---|---|---|---|
| 1（冷启动） | 2.5 KB | 2.5 KB | 100% |
| 5 | 5.5 KB | 1.7 KB | 30% |
| 10 | 9.2 KB | 1.8 KB | 20% |
| 20 | 16.7 KB | 2.1 KB | 13% |

关键性质：**上下文线性增长，线上体积基本持平。**

## 怎么做的

```
客户端                                        网关
─────────────────────────────────────────────────────────────
messages JSON                                 帧流
  │ ① 渲染器（懂 LLM 语义）                      │ ② 解码器（只认字节）
  ▼                                             ▼
一维文本流                                     一维文本流
  │ ② 编码器（只认字节）                          │ ① 反渲染器（懂 LLM 语义）
  ▼                                             ▼
帧流 ─────────── 公网（体积最小的一段）────────→ messages JSON
                                                   │
                                                   ▼
                                                上游推理 API
```

四层，职责单一，可独立替换：

| 层 | 模块 | 输入 → 输出 | 懂 LLM 语义？ |
|---|---|---|---|
| 渲染器 | `render.py` | messages JSON → 一维文本 | ✅ |
| 编码器 | `codec.py` | 一维文本 → 帧流 | ❌ 只认字节 |
| 解码器 | `codec.py` | 帧流 → 一维文本 | ❌ 只认字节 |
| 反渲染器 | `render.py` | 一维文本 → messages JSON | ✅ |
| 分块 | `chunking.py` | 字节 → 块（定长） | ❌ |
| 存储 | `storage.py` | 块集合 | ❌ |

**中间三层完全不知道聊天协议。** 要支持 Anthropic Messages 或 Responses API，
只需写一个新的 `render` 模块，其余一行都不用改。

## 三条不变式

1. **渲染必须确定性** —— 同一份 messages 必须渲染成逐字节相同的文本，否则
   块内容变化、`block_hash` 变化、缓存整体失效。实现上：字段顺序固定、
   `json.dumps(sort_keys=True)`。格式版本走 HTTP 头，不写进正文。
2. **中间层无语义** —— 编码器/解码器不知道哪个字节是 role。
3. **缓存缺失只退化，不报错** —— 取不到块时响应带 `X-Tail-Retransmit: 1`（状态码不变），
   客户端清空本地已知块集合后重发全文。存储全空、服务重启时系统仍能正常工作。

## 为什么不用原 JSON 直接传

**完整协议、管线与设计动机见 [`docs/TRANSPORT-PIPELINE.md`](docs/TRANSPORT-PIPELINE.md)。**

要点：

- **JSON 是树，块边界没有自然落点。** 渲染成固定格式文本后是一维字节流，
  块边界按固定 `block_size` 对齐即可（vLLM 的做法）。
- **前缀缓存消不掉新增消息里的结构字节。** 每条新消息都带 `{"role":"`, `","content":"`
  这些零信息开销。不过要诚实：正文很长的对话里这只占 3~4%，渲染成文本本身几乎不省
  （长对话实测只省 3.5%，而 1000 条短消息能省 55%）。**渲染的真正价值是把树摊平成
  一维字节流，让块能按固定间隔切、跨轮复用。**
- **尾部增量只能处理追加。** compact、编辑旧消息、重排、中间插入都会导致整条前缀
  失效；分块去重下只重传受影响的块。

## 快速开始

```bash
pip install -e ".[test]"

# 起网关（内存存储；生产用 --dbm 或 --redis）
python -m tail.serve --backend https://api.deepseek.com --port 8765 --dbm ./blocks.db
```

客户端：**现有的 `openai` 代码不用改请求逻辑**，只多传一个 `http_client`。

```python
import httpx2
from openai import OpenAI
from tail.transport.client import TailTransport

client = OpenAI(
    base_url="http://127.0.0.1:8765/v1",
    api_key="sk-xxx",
    http_client=httpx2.Client(transport=TailTransport()),   # ← 唯一新增
)

# 下面和原生用法一模一样，流式也一样
for event in client.chat.completions.create(
        model="deepseek-chat", messages=messages, stream=True):
    print(event.choices[0].delta.content or "", end="")
```

异步用 `AsyncOpenAI` + `httpx2.AsyncClient(transport=TailAsyncTransport())`。

### 客户端只记哈希，不缓存内容

本地状态只有一个「网关已有块」的哈希集合（16 字节/块，约 70 B/块），**不留正文副本**。
编码时命中就只发 17 字节引用帧。实测约 79 KB 上下文（174 个满块）：

| | |
| --- | --- |
| 本地集合占内存 | 1,664 B 纯哈希（104 块，set 实际约 7 KB） |
| 冷启动上行 | 39,838 B |
| 全命中后上行 | 1,276 B（省 97%） |

三点要注意：

1. **必须复用同一个 client 实例**——集合挂在 client 上，每轮新建 `OpenAI(...)`
   等于永远冷启动（6 轮合计 14,745 B vs 5,973 B，差 59%）。
2. **复用是前缀形状的**——块哈希沿用 vLLM 的父链 `hash(parent, block)`，纯追加几乎
   免费，往中间插一句则其后全部失效。
3. **没有逐块 ACK**——客户端乐观假设「发出去的对端就存下了」，对端存储被清空时用
   `X-Tail-Retransmit` 通知，客户端全量重发一次（多一次往返，不会错内容）。

`TailTransport` 只拦截 `/chat/completions` 的 JSON 请求，其余请求（`/models`、
embeddings、文件上传……）原样透传；网关回 `X-Tail-Retransmit` 时它自动清空本地
已知块集合并重发一次全量，对上层透明。

完整可运行示例见 [examples/chat.py](examples/chat.py)（原生）与
[examples/chat_tail.py](examples/chat_tail.py)（接入 Tail），两者 diff 只有 3 行。

网关也接受普通 `application/json` 请求并原样转发，便于灰度对比与排障。

### 依赖说明：为什么是 `httpx2`

`httpx` 稳定版停在 0.28.1（后续只有 `1.0.dev*` 预发布），继任包是
[`pydantic/httpx2`](https://github.com/pydantic/httpx2)（同一作者 Tom Christie）。
`openai` 3.x 已依赖 `httpx2`，Starlette 1.10 的 `TestClient` 也优先用 `httpx2`
并对 `httpx` 报弃用警告。所以本项目统一用 `httpx2`，客户端与网关只有一套 HTTP 栈。

`TailTransport` 本身与库无关：它按请求对象所属的库自动选择内层 transport，
因此仍在用 `httpx` 的 `openai` 1.x 也能直接接。

### 直接用底层 API（不用 SDK）

```python
from tail.transport.gateway import TailClient

client = TailClient()
body = client.encode_request({"model": "deepseek-chat", "messages": messages})
resp = httpx2.post(
    "http://127.0.0.1:8765/v1/chat/completions",
    content=body,
    headers={"content-type": "application/x-tail-v2",
             "authorization": f"Bearer {api_key}"},
)
if resp.headers.get("x-tail-retransmit"):
    client.invalidate()          # 缓存失效，下一轮重发全文
```

## 线上格式

`Content-Type: application/x-tail-v2`，body 为二进制帧流：

```
0x01 <block_hash>                      引用：这段内容网关已有
0x02 <block_hash> <4 字节长度> <块字节>   新块：直传内容
0x03                                   结束
```

引用帧固定 17 字节。新块帧带 `block_hash`：哈希是 vLLM 式的
`hash(父哈希 + 本块)`，解码端不知道父哈希，算不出正确值，必须由发送方随帧传来。

用二进制而非 JSON 包一层的理由：无二次转义、可流式解析、引用帧极其紧凑。

## 分块与块哈希

**完全照搬 vLLM Automatic Prefix Caching**
（[设计文档](https://docs.vllm.ai/en/v0.8.3/_sources/design/v1/prefix_caching.md)），
没有自创算法：

```
block_hash = hash(父块哈希, 本块字节)
```

- **定长块**，块边界只由位置决定，与内容无关。
- **哈希链**：父哈希让相同内容挂在不同前缀下得到不同哈希，这是"前缀"语义的来源。
- **只缓存满块**（vLLM: "We only cache full blocks"），尾块不入存储。

DeepSeek 侧对应的是"按固定 token 间隔落盘"
（[上下文硬盘缓存](https://api-docs.deepseek.com/zh-cn/guides/kv_cache/)），
同样是固定间隔。

参数：`BLOCK_SIZE = 512` 字节 / `BLOCK_ID_SIZE = 16` 字节 / blake2b。

**`BLOCK_SIZE` 是实测调出来的，不能照抄 vLLM 的 16 token。** 本层每块要付
`1+16` 字节帧头，48 字节（16 token 的中文换算）的块光帧头就占 35%，实测反而更差：

| block_size | 48B | 64B | 128B | 256B | **512B** | 1024B | 4096B |
|---|---|---|---|---|---|---|---|
| 20 轮对话省 | -2% | 19% | 52% | 66% | **69%** | 68% | 26% |

哈希用 blake2b 而非 `hash()`：Borg 明确要求块 id 必须是密码学强哈希，
且内置 `hash()` 带进程随机盐，不能跨进程复用缓存。

## 存储

扁平块集合，**没有链、没有 prev 指针、没有引用图**。顺序由请求里的帧序列直接表达，
所以不需要引用计数、不需要访问驱动续期、不需要缺块补发协议、不存在「部分命中」概念。

```
blk:{block_hash} → 块内容
```

- `MemoryStorage` —— 进程内，重启即丢（默认）
- `DbmStorage` —— 零依赖，单机部署
- `RedisStorage` —— MGET 批量取块，生产用（`pip install redis`）

## 测试

```bash
python -m pytest tests/ -v
```

85 个测试，覆盖分块与前缀复用、帧格式往返、渲染确定性、编解码、存储后端、
「客户端 → ASGI 网关 → mock 上游」的端到端链路（含存储失效降级与恢复），
以及用真实 `openai` SDK 经 `TailTransport` 接入的回归测试。
