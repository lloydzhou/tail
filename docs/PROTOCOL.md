# Tail 线缆协议规范

> Tail 是一个位于 LLM 客户端 SDK 与推理后端之间的**前缀缓存协商层**。
> 本文档是它对外暴露的 HTTP 线缆协议(Wire Protocol)规范:它在标准
> OpenAI Chat Completions 协议之上,增加了一组 `X-Cache-*` 头与一种
> 增量发送语义,从而让客户端只发"尾部增量",由网关侧透明还原完整请求。
>
> - **基础协议**:OpenAI Chat Completions API(Tail 不发明新端点、不改 body)
> - **扩展方式**:仅在请求/响应两侧各加 2~3 个 HTTP 头,加一个 `412` 快速失败响应
> - **目标读者**:网关实现者(Lua / Python / Rust)、SDK 实现者、接入调试者
> - **配套文档**:[设计文档](./DESIGN-chunked-cache.md)(v2.1 Segment-Merkle)

## 1. 与官方协议的关系

Tail **不重新发明** chat completions。客户端发出的请求、推理后端收到的请求,
都是 100% 标准的 OpenAI Chat Completions 协议。Tail 只做两件事:

1. 在请求体到达后端**之前**,若缓存命中,用缓存里的前缀 messages 把
   客户端发的"增量 messages"补全成完整 messages;
2. 通过 HTTP 头告诉客户端"上一次的前缀我已经存下了,下次只发增量即可"。

### 1.1 参考的官方规范

Tail 的基础协议等价于下列官方文档所定义的 `POST /v1/chat/completions`:

| 来源 | 端点 | 用途 |
|------|------|------|
| [OpenAI — Chat Completions Overview](https://developers.openai.com/api/reference/chat-completions/overview/) | `POST /v1/chat/completions` | 权威端点定义(请求/响应 schema) |
| [OpenAI — Create chat completion](https://developers.openai.com/api/reference/python/resources/chat/subresources/completions/methods/create/) | 同上 | 字段级参数参考(model/messages/tools/stream/...) |
| [DeepSeek — Create Chat Completion](https://api-docs.deepseek.com/api/create-chat-completion) | `POST /chat/completions` | OpenAI 兼容实现,字段一致,无 `/v1` 前缀 |
| [DeepSeek — Multi-round Conversation](https://api-docs.deepseek.com/guides/multi_round_chat) | 同上 | 说明多轮对话本身是无状态 API |

> **注意**:DeepSeek 与大多数第三方推理服务遵循 "OpenAI 兼容" 线缆格式,
> 区别只在 URL 前缀(`/v1/chat/completions` vs `/chat/completions`)和模型名。
> Tail 网关对两者都透明:网关只看 `X-Cache-*` 头,不关心 body 的厂商字段。

### 1.2 不变量(Tail 永不破坏的契约)

无论缓存命中与否,以下三点保持不变:

1. **后端收到的请求体 = 完整的标准 Chat Completions 请求体**
   (含完整 `messages` / `tools` / `system` / `stream`)。
2. **客户端拿到的响应体 = 后端原样字节流**
   (含 SSE `text/event-stream` 流式逐块透传)。
3. **不发明任何新路径**。唯一的端点仍是 `POST /v1/chat/completions`。

---

## 2. 基础协议(无缓存时)

缓存未命中、或客户端首次请求时,Tail 完全退化为一个透明代理。此时报文与
官方协议逐字节一致(除了 Tail 注入的 `X-Cache-*` 响应头)。

### 2.1 请求(首次,无 `X-Cache-*` 头)

```http
POST /v1/chat/completions HTTP/1.1
Host: gateway.example.com
Content-Type: application/json
Authorization: Bearer sk-...

{
  "model": "deepseek-chat",
  "messages": [
    {"role": "system", "content": "你是一个有用的助手。"},
    {"role": "user", "content": "你好"}
  ],
  "stream": true
}
```

### 2.2 响应(网关写入缓存后回送)

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
X-Cache-Hash: cbe06d5835a21fd3::49da429ee74cb70f::e2bf5a4c82c9b178
X-Cache-Expire: 1750000000
X-Cache-Hit: false

data: {"choices":[{"delta":{"content":"你好"}}]}
...
```

> 客户端**必须**保存这三个响应头;它们是下一次请求只发增量的凭据。

---

## 3. Tail 扩展:HTTP 头

### 3.1 请求方向(Client → Gateway)

| Header | 必填 | 含义 |
|--------|------|------|
| `X-Cache-Hash` | 否 | 上次响应里 `X-Cache-Hash` 回送的值。**不带 = 首次请求**,网关视为全量并建缓存。 |
| `X-Cache-Prefix-Length` | 否 | 该哈希对应的前缀 **messages 条数**(不是 segment 数)。与 `X-Cache-Hash` 必须同时出现或同时缺失。 |

语义约束:

- **若带 `X-Cache-Hash`**,则请求体里的 `messages` **可以只含增量**
  (即第 `X-Cache-Prefix-Length + 1` 条之后的消息);网关命中后会还原完整数组。
  body 里的 `system` / `tools` **可以省略**,网关命中后从缓存补回。
- **若不带 `X-Cache-Hash`**(首次请求),请求体必须是完整的标准请求,网关不还原、直接建缓存。
- 客户端**永远不需要理解 `X-Cache-Hash` 的内部结构**(三段拼接)。对客户端而言它只是一个不透明 token。

### 3.2 响应方向(Gateway → Client)

| Header | 总会出现 | 含义 |
|--------|----------|------|
| `X-Cache-Hash` | 是 | **本次完整前缀**的新哈希。无论命中、未命中、还是 412 重试后的全量转发,网关都会基于"本次它发给后端的完整请求"重新计算并回送。客户端应**用新值覆盖**本地保存的旧值。 |
| `X-Cache-Expire` | 是 | 缓存过期 Unix 时间戳(秒),由网关加 ±jitter 后算出(防雪崩)。客户端据此判断本地缓存是否还有效。 |
| `X-Cache-Hit` | 是 | `true` / `false`。`true` = 网关命中并已还原完整前缀;`false` = 未命中(可能是首次、meta 过期、链断裂等)。 |

> 这三个头**在所有响应路径上都出现**:2xx 命中、2xx 未命中(passthrough 模式)、
> 412 快速失败。客户端只需统一读取,无需针对状态码分支处理。

---

## 4. Tail 扩展:`412 Precondition Failed` 快速失败

当缓存**未命中**(或链断裂、`prefix_length` 不一致),且网关运行在 `fast_fail`
模式(默认)时,网关**不会**把残缺的增量请求转发给后端,而是直接返回:

```http
HTTP/1.1 412 Precondition Failed
Content-Type: application/json
X-Cache-Hash: 
X-Cache-Expire: 1750000000
X-Cache-Hit: false

{
  "error": {
    "message": "prefix cache miss; retry with full messages",
    "type": "prefix_cache_miss",
    "code": "cache_miss"
  }
}
```

### 4.1 为什么是 `412` 而不是 `422` / `200`

`X-Cache-Hash` 在语义上是一个**前置条件**(precondition):客户端在声明
"我假设你那边有这个前缀"。MDN 对 `412 Precondition Failed` 的定义正是
"服务器在评估请求头中给出的前置条件时失败"——与本场景精确吻合。
(`422 Unprocessable Entity` 表示 body 语法错误,与"前置条件失败"不同。)

### 4.2 客户端处理契约(必须实现)

收到 `412` 的客户端**必须**:

1. **失效本地缓存**:丢弃当前保存的 `X-Cache-Hash` / `X-Cache-Prefix-Length`。
2. **用完整 messages 重发一次**:不带任何 `X-Cache-*` 头,请求体恢复成
   完整的标准 Chat Completions 请求。
3. 重发成功后,从新响应里重新读取 `X-Cache-*` 三件套,作为后续增量请求的凭据。

> 详见 `tail/openai_patch.py` 中 `_patched_request` 的 `try/except` + `_is_miss_exception`
> 实现:它通过 `X-Cache-Hit: false` 或状态码 `412` 识别 miss,失效缓存,
> 用完整 messages 重试一次。

### 4.3 passthrough 模式(可选,不推荐)

网关也可配置为 `miss_mode=passthrough`:未命中时把当前 messages 当完整请求转发,
返回 2xx + `X-Cache-Hit: false`。该模式语义不精确(网关无法区分"客户端发了全量"
和"客户端发了增量但 miss"),且无法保证后端不被残缺请求打挂。**默认禁用**。

---

## 5. `cache_key` 的内部格式(三段哈希)

虽然客户端把 `X-Cache-Hash` 当作不透明 token,但网关实现者必须遵守它的格式,
以保证不同语言实现的网关(Lua / Python)对同一请求产出**相同的 key**
(跨实现哈希一致性是硬约束,见设计文档 §3.5)。

### 5.1 格式

```
cache_key = sys_hash "::" tools_hash "::" pfx_hash
```

每段固定 16 个十六进制字符(SHA-256 截断前 64 bit)。任一段缺失时用
字符串 `"0"`(单字符零)占位。

| 段 | 来源 | 缺失时的占位 |
|----|------|--------------|
| `sys_hash` | 请求体 `system` 字段:`sha256_hex16(str(system))` | `"0"`(无 `system`) |
| `tools_hash` | 请求体 `tools` 字段:`sha256_hex16(json(tools, sort_keys=True, separators=(",",":"), ensure_ascii=False))` | `"0"`(无 `tools`) |
| `pfx_hash` | `messages` 的 Segment-Merkle 链哈希(见 §6) | 链到末段;空 messages 为 `"0"` |

示例:`cbe06d5835a21fd3::49da429ee74cb70f::e2bf5a4c82c9b178`
(有 system、有 tools、有 messages 的情况)。

### 5.2 为什么三段独立哈希

跨对话/跨用户复用:同一段 system prompt 被多个对话使用时,`sys_hash` 相同,
存储里只需存一份 `sys:<hash>` → 全文。同理 tools。只有 `pfx_hash` 因对话而异。
详见设计文档 §2.2、§3.5。

---

## 6. `pfx_hash`:Segment-Merkle 链(实现者必读)

`pfx_hash` 不是对整个 messages 数组的整体哈希,而是一条**增量可追加**的 Merkle 链。
这样新增一个对话回合时,`O(1)` 就能算出新哈希、写新链节,无需重新哈希整条历史。

### 6.1 Segment 定义

把 messages 数组切成若干 segment,每个 segment 满足约束:

```
segment = (1 × assistant)? + (m × tool) + (n × user)
约束:m + n ≥ 1  且  m · n = 0    // tool 与 user 互斥,且至少一个非 assistant
```

即每个 segment 至多含一条 assistant、其后跟若干 tool 结果**或**若干 user 消息
(两者不能同段)。完整切分规则见设计文档 §3.1,代码见 `tail/gateway/segment.py`。

### 6.2 段哈希 `segment_hash`

把段内每条 message 用稳定字节布局编码后空分隔拼接,再 SHA-256 截断:

```
encode_message(msg) = f"{rl}:{role}\x00{cl}:{content}\x01"
其中:
  rl = role 的 UTF-8 字节长度
  cl = content 的 UTF-8 字节长度(content 为 list/dict 时用
       json.dumps(separators=(",",":"), sort_keys=True, ensure_ascii=False) 规范化)
  \x00 / \x01 为单字节分隔符,防边界碰撞

segment_hash(seg) = sha256_hex16( "".join( encode_message(m) for m in seg ) )
```

> **关键**:字节布局必须 Python / Lua 逐字节一致,否则跨实现 cache_key 对不上。
> 代码见 `tail/gateway/hashing.py`(`encode_message`)与 `openresty/lua/kvcache/hashing.lua`。

### 6.3 链式前缀哈希 `pfx_hash`

```
H(prefix_0) = "0"                                   // 空前缀固定为字符 "0"
H(prefix_k) = sha256_hex16( H(prefix_{k-1}) + "|" + segment_hash(seg_k) )
```

即每个链节 = 前一节哈希 + `"|"` + 本段哈希,再 SHA-256 截断。
代码见 `tail/gateway/merkle.py`(`chain_step` / `chain_hash` / `build_nodes`)。

**追加性质**:新增第 k 个 segment 时,只需 `chain_step(H(prefix_{k-1}), segment_hash(seg_k))`,
即 O(1) 算出新 `pfx_hash`,无需重算前 k-1 段。这是 Tail 存储省流量能力的核心。

---

## 7. 增量发送的完整时序

下图展示一次典型的两轮对话(第二轮命中缓存):

```
客户端                                  Tail 网关                              后端
  │  POST /v1/chat/completions            │                                     │
  │  messages=[sys, u1]                   │                                     │
  │  (无 X-Cache-*)                       │                                     │
  │ ─────────────────────────────────────▶│  miss(首次)                         │
  │                                       │  POST /v1/chat/completions          │
  │                                       │  messages=[sys, u1]                 │
  │                                       │ ───────────────────────────────────▶│
  │                                       │  200 SSE (a1)                       │
  │                                       │ ◀───────────────────────────────────│
  │                                       │  异步写缓存: sys/tools/pfx/seg/meta │
  │  200 SSE (a1)                         │  + 回送 X-Cache-Hash=K1             │
  │   X-Cache-Hash: K1                    │     X-Cache-Hit: false              │
  │ ◀─────────────────────────────────────│                                     │
  │                                       │                                     │
  │  客户端保存 K1, prefix_length=2       │                                     │
  │                                       │                                     │
  │  POST /v1/chat/completions            │                                     │
  │  messages=[u2]            ← 仅增量!  │                                     │
  │  X-Cache-Hash: K1                     │                                     │
  │  X-Cache-Prefix-Length: 2             │                                     │
  │ ─────────────────────────────────────▶│  命中 K1,还原 [sys, u1]            │
  │                                       │  POST /v1/chat/completions          │
  │                                       │  messages=[sys, u1, a1, u2]         │
  │                                       │ ───────────────────────────────────▶│
  │                                       │  200 SSE (a2)                       │
  │                                       │ ◀───────────────────────────────────│
  │  200 SSE (a2)                         │  异步写新缓存链节 pfx_hash=K2       │
  │   X-Cache-Hash: K2                    │  + 回送 X-Cache-Hash=K2             │
  │   X-Cache-Hit: true                   │     X-Cache-Hit: true               │
  │ ◀─────────────────────────────────────│                                     │
```

注意第二轮:

- 客户端上行只发了 1 条 message(`u2`),而不是 4 条;
- 后端收到的仍是完整的 4 条(`sys, u1, a1, u2`)——后端无感知;
- `X-Cache-Hash` 从 `K1` 变成 `K2`(因为前缀变长了)。

---

## 8. 一致性约束(客户端侧)

"按 `X-Cache-Prefix-Length` 条数切增量"本身有正确性陷阱。客户端在切分增量前,
**必须**校验本地前缀指纹与实际 messages 前缀一致,否则会发生静默数据错误
(compact、编辑旧消息、重排、多 session 交叉等)。具体场景与修复见
设计文档 [§5 SDK 前缀一致性保证](./DESIGN-chunked-cache.md#5-sdk-前缀一致性保证)。

本协议**不强制**客户端实现指纹校验,但任何声称兼容 Tail 的 SDK 若不实现 §5.3,
将在 compact / 多 session 场景下产生错误结果(而非报错)。参考实现
`tail/openai_patch.py` 已实现完整校验(`_messages_digest`)。

---

## 9. 流式(SSE)透传契约

当请求体 `"stream": true` 时:

- 后端响应 `Content-Type: text/event-stream`,逐块发 `data: {...}\n\n`。
- Tail 网关**必须**逐块透传字节流,不得缓冲整个响应(否则首字延迟暴涨)。
  - Python 网关:`httpx.AsyncClient.send(stream=True)` + `StreamingResponse(aiter_raw())`
  - OpenResty 网关:`proxy_buffering off; proxy_http_version 1.1;`
- `X-Cache-*` 响应头在**第一个字节之前**随响应头发出(它们在 header 阶段就已知)。
- 客户端必须在读取 SSE 流的**同时**捕获响应头(参考实现通过 patch `httpx.send` 实现)。

---

## 10. 头部清理(网关实现者注意)

网关在把请求转发给后端前,**必须剥离**以下请求头:

- `X-Cache-Hash` / `X-Cache-Prefix-Length`(Tail 内部协商头,后端不认)
- hop-by-hop 头:`connection` / `keep-alive` / `te` / `trailers` / `upgrade` /
  `proxy-authenticate` / `proxy-authorization` / `transfer-encoding`
- `host`(后端按自己域名解析)
- `content-length` / `transfer-encoding`(body 已被网关重写,长度变了)
- **空的 `Authorization`**(客户端用占位 key 时是 `"Bearer "` 后端会报非法头值)

响应方向同理剥离 `content-encoding` / `transfer-encoding` / `content-length` /
`connection`,再追加 `X-Cache-*`。参考实现:`tail/gateway/app.py`。

---

## 11. 健康检查与运维端点

网关额外提供两个非协议端点(运维用,不影响协议本身):

| 端点 | 方法 | 用途 |
|------|------|------|
| `/__tail/health` | GET | 返回 `{"status":"ok","storage": <bool>}`,存储可达性探针 |
| `/__tail/stats` | GET | 占位,未来可返回命中率/存储量等指标 |

---

## 12. 协议版本与演进

当前协议版本对应设计文档 v2.1。版本体现在 `cache_key` 的三段格式上,而非单独
的版本头。未来若 `encode_message` / segment 约束 / 链式哈希发生不兼容改动,
`cache_key` 会自然变化(旧缓存自动 miss 并重建),客户端无需感知——这也是
"客户端把 `X-Cache-Hash` 当不透明 token"这一约束带来的好处。

### 变更记录

| 版本 | 变更 |
|------|------|
| v2.1 | 三段 `cache_key`(`sys::tools::pfx`);Segment-Merkle 链;访问驱动续期 |
| v1.0 | 单段 `cache_key`;整段 messages 缓存(已被 v2.1 取代) |

---

## 参考资料

- [OpenAI — Chat Completions Overview](https://developers.openai.com/api/reference/chat-completions/overview/)
- [OpenAI — Create chat completion](https://developers.openai.com/api/reference/python/resources/chat/subresources/completions/methods/create/)
- [DeepSeek — Create Chat Completion](https://api-docs.deepseek.com/api/create-chat-completion)
- [DeepSeek — Multi-round Conversation](https://api-docs.deepseek.com/guides/multi_round_chat)
- [MDN — 412 Precondition Failed](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/412)
- 本项目 [设计文档(v2.1 Segment-Merkle)](./DESIGN-chunked-cache.md)
