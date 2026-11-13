# Tail 传输层:协议与数据管线

> 本文描述**当前实现**的完整协议与管线,所有常量、字节布局、行为均与代码逐项核对。
> 代码位置:`tail/transport/`。
>
> 审核要点集中在每节的「设计依据」——每一个决策都应能指向一个行业参考实现
> (vLLM / DeepSeek / OpenAI / Borg)或一组实测数据,没有自创机制。

---

## 1. 它解决什么问题

客户端 SDK 与推理服务之间的请求体是 OpenAI Chat Completions 的 JSON。多轮对话里,
每一轮都要把**整份历史**重新上传一次,其中绝大部分与上一轮完全相同。

目标:

1. **减少公网传输字节**——重复的前缀不再传。
2. **不改变上游协议**——推理服务收到的是**标准** OpenAI JSON,它完全不需要知道 Tail 存在。

Tail 插在中间:

```
客户端 SDK
   │  ① 渲染:JSON → 一维文本(固定顺序,确定性)
   │  ② 编码:文本 → 定长块 → 帧流(已知块只发 17 字节引用)
   ▼
 ══════ 公网 ══════   ③ 只传「新增的块」
   ▼
 网关
   │  ④ 解码:帧流 → 文本(边收边查存储补全引用)
   │  ⑤ 反渲染:文本 → 标准 OpenAI JSON
   ▼
推理服务(DeepSeek / vLLM / ...)   ⑥ 收到的就是普通 JSON
```

**为什么先把 JSON 变成文本**:JSON 是树,块边界没有自然落点;摊平成一维字节流后
才能按固定间隔切块并跨轮复用。这一层还顺手省掉每条消息的 `{"role":"`,
`","content":"` 这类零信息结构字节。

---

## 2. 分层与不变式

| 层 | 模块 | 输入 → 输出 | 懂 LLM 语义 |
|---|---|---|---|
| 渲染器 | `render.py` | messages JSON → 一维文本 | ✅ |
| 编码器 | `codec.py` | 文本 → 帧流 | ❌ 只认字节 |
| 解码器 | `codec.py` | 帧流 → 文本 | ❌ 只认字节 |
| 反渲染器 | `render.py` | 一维文本 → messages JSON | ✅ |
| 分块/哈希 | `chunking.py` | 字节 → 块 + 块哈希 | ❌ 只认字节 |
| 存储 | `storage.py` | `block_hash → 块字节` | ❌ 只认字节 |

三条不变式:

1. **渲染必须确定性**——同一份 messages 必须渲染成**逐字节相同**的文本,否则块内容
   变化、`block_hash` 变化、缓存整体失效。实现手段:`sort_keys=True` 的 JSON 序列化、
   固定的顶层参数顺序。
2. **中间层无语义**——编码器/解码器不知道哪个字节是 role。要支持 Anthropic Messages
   或 Responses API,只需写新的 `render` 模块,中间两层一行不改。
3. **缓存缺失只退化,不报错**——取不到块时**立刻中止**这次上游请求(HTTP 409 +
   `X-Tail-Retransmit: 1`),客户端清空本地已知块集合后重发全文。存储全空、服务重启时
   系统仍能正常工作。**不允许「跳过缺块继续解」**:见 §8。

---

## 3. 渲染格式

### 3.1 标记集合

包装语法 `<|name|>` 取自 **OpenAI Harmony** 的 formatting token
(`<|start|>` / `<|message|>` / `<|end|>`)。名字本身**没有一个是我发明的**:

- **role 标记**:`<|system|>` `<|user|>` `<|assistant|>` `<|tool|>` `<|developer|>`
  `<|function|>` —— 就是 OpenAI 的 `role` 取值原文。
- **字段标记**:`<|tool_calls|>` `<|tool_call_id|>` `<|name|>` `<|extra|>` ——
  `tool_calls` / `tool_call_id` / `name` 是 OpenAI 字段名原文;`extra` 见 §3.5。
- **顶层参数标记**:`<|model|>` `<|temperature|>` 等 21 个,全部是 OpenAI
  请求体字段名原文(顺序固定,见 `render.PARAM_ORDER`)。

### 3.2 布局

```
<|model|>"deepseek-chat"<|temperature|>0.7<|system|>"你是助手。"<|user|>"你好"
```

规则:

1. 顶层参数在前,顺序由 `PARAM_ORDER` 固定(确定性要求)。
2. 参数值用 `json.dumps(sort_keys=True)` 序列化。
3. 每个 message 以 role 标记开头,正文是 **JSON 字符串**(不是裸文本)。
4. message 内部字段以 `<|字段名|>` 引出,值同样是 JSON 字符串。
5. **格式版本不进正文**,走 HTTP 头 `X-Tail-Version`。写进正文会让版本号落在第一个块上,
   升版即全量失效。

### 3.3 正文为什么必须是 JSON 字符串

这是为了消除歧义,不是风格选择。若正文裸输出,用户在 prompt 里写一句
"请把 `<|user|>` 当作分隔符"就会**把整条流解析错位**(实测直接抛
`JSONDecodeError`)。

改用 JSON 字符串承载后,正文里的标记由 JSON 的字符串/转义语义保护,解析器只需
跟踪"当前是否在 JSON 字符串内部",在字符串内部出现的 `<|` 不算结构。
**没有引入任何新的转义符号**——用的就是 JSON 自己的引号与反斜杠。

代价实测:纯中文约 **0.6%**,含大量引号/换行的代码约 **13~19%**。

### 3.4 tool_calls

按 OpenAI 原结构做确定性 JSON,**不发明** `name(args)` 之类的语法:

```
<|assistant|><|tool_calls|>[{"function":{"arguments":"{\"city\": \"北京\"}","name":"get_weather"},"id":"c1","type":"function"}]
```

`arguments` 按**原字符串**保留(不重新序列化)——重新序列化会改变空白
(`{"k": 1}` → `{"k":1}`),字节就不一致了。

> 曾经用 `name(args)` 这种"函数调用"写法,有两个真实缺陷:`id`/`type` 字段无处安放
> 直接丢失;assistant 正文里出现 `f({"a":1})` 时会被误判成 tool_call。

### 3.5 `content` 的三种形态

OpenAI 里 `content: null`、`content` 缺失、`content: ""` 是三种不同语义,但它们
都不产生正文,纯看字节流无法区分。用 `<|extra|>` 携带存在性:

```
<|assistant|><|tool_calls|>[...]<|extra|>{"__content__":null}
<|user|><|extra|>{"__content__":""}
<|user|><|extra|>{"__content__":{"__missing__":true}}     ← 原文没有 content 键
```

`extra` 同时承载无法映射到上述字段的原始键,做到**不静默丢数据**。

---

## 4. 分块与块哈希

**完全照搬 vLLM Automatic Prefix Caching**,没有自创算法。

参考:<https://docs.vllm.ai/en/v0.8.3/_sources/design/v1/prefix_caching.md>

### 4.1 vLLM 做什么

文档原文的图示与规则:

```
       Block 1: |<--- block tokens ---->|
       Block 2: |<------- prefix ------>| |<--- block tokens --->|
       Block 3: |<------------------ prefix -------------------->| |<--- block tokens ---->|
```

1. **定长块**,块边界只由位置决定,与内容无关。
2. **块哈希 = hash(父块哈希 + 本块 tokens + 额外哈希)**:

   ```
   block_hash = hash(parent_hash, block_tokens, extra_hashes)
   ```

   父哈希让相同内容挂在不同前缀下得到不同哈希——这就是"前缀"语义。

   本层目前**不用** `extra_hashes`:vLLM 用它区分 LoRA ID 与多模态输入,
   而这层工作在字节上、不感知 LoRA/多模态。形式上保留位置但不引入空参数,
   等真有第二种 renderer 需要隔离块空间时再加。
3. **只缓存满块**("We only cache full blocks")。

DeepSeek 侧对应的是**按固定 token 间隔落盘**(见
[上下文硬盘缓存](https://api-docs.deepseek.com/zh-cn/guides/kv_cache/)),
同样是固定间隔。

### 4.2 本层的实现

```python
BLOCK_SIZE    = 512                    # 字节
BLOCK_ID_SIZE = 16                     # 128 bit
_HASH_NAME    = "blake2b"

block_hash(block, parent_hash=b"") = blake2b(parent_hash + block)
```

本层工作在**字节**上,因为网关拿不到 tokenizer。

哈希用 blake2b 而非内置 `hash()`:Borg 官方文档明确要求块 id 必须是**密码学强哈希**,
且内置 `hash()` 带进程随机盐,不能跨进程复用缓存。

### 4.3 `BLOCK_SIZE` 为什么是 512 而不是照抄 16 token

vLLM 的 16 token 换算成中文约 48 字节。但本层每块要付 `1+16+4 = 21` 字节帧头,
48 字节的块光帧头就占 35%,**实测反而比不发引用帧更差**:

| block_size | 48B | 64B | 128B | 256B | **512B** | 1024B | 4096B |
|---|---|---|---|---|---|---|---|
| 20 轮对话省 | **-2%** | 19% | 52% | 66% | **69%** | 68% | 26% |

512 取在平台区:再小帧头占比上升,再大复用粒度变粗。

### 4.4 已否决的方案:CDC(内容定义分块)

早期实现过 buzhash 的 CDC(滚动哈希找切点),**已整体移除**。否决理由:

- DeepSeek 与 vLLM 都用固定间隔,不用内容定义切点。
- CDC 必须逐字节计算滚动哈希才能找切点,实测只有 **0.66 MB/s**(2.5MB 请求要 4 秒);
  定长切块是 **486 MB/s**。
- 对话场景天然是**尾部追加**,前缀不动——恰是定长块最有利的形态。
- 那版还错把 `max_size` 设成等于 `avg_size`,35% 的块退化成硬切,
  等于付了 CDC 的全部代价却只拿到部分收益。

---

## 5. 线缆格式

`Content-Type: application/x-tail-v2`,body 为二进制帧流。

```
类型 1 字节 + 定长头部 + 可变内容
```

### 5.1 三种帧

| 类型 | 布局 | 说明 |
|---|---|---|
| `0x01` 引用帧 | `01` + 16B `block_hash` | 固定 **17B**。这个块网关已有 |
| `0x02` 新块帧 | `02` + 16B `block_hash` + 4B 长度(大端) + 块字节 | 固定头 **21B** |
| `0x03` 结束帧 | `03` | 固定 **1B** |

实际字节(块 `b"x"*512`):

```
引用帧:  017e44ea981aeaab1e7e9b7fe4b8b3a65a                       (17B)
新块帧:  027e44ea981aeaab1e7e9b7fe4b8b3a65a 00000003 616263        (24B,内容 "abc")
结束帧:  03                                                       (1B)
```

### 5.2 新块帧为什么也带 `block_hash`

块哈希是**哈希链** `hash(父哈希 + 本块)`。解码端收到新块时并不知道它的父哈希
(前一个块可能是引用帧,也可能被跳过),**算不出正确值**。所以哈希由发送方算好
随帧传来——这与 vLLM 里 `block_hash` 由调度侧计算、随 `KVCacheBlock` 流转是同一个道理。

### 5.3 为什么用二进制而不是 JSON

- **无二次转义**:JSON 里块内容要再转义引号和反斜杠,体积与 CPU 都翻倍。
- **可流式解析**:读 1 字节类型 + 定长头部,不必等整个 body。
- **引用帧极紧凑**:17 字节,1000 块仅 17KB。

### 5.4 HTTP 头

| 头 | 方向 | 含义 |
|---|---|---|
| `X-Tail-Version` | 响应 | 协议版本,当前 `"2"` |
| `X-Tail-Status` | 响应 | `hit` / `miss` / `degraded`,仅供日志监控 |
| `X-Tail-Hit` | 响应 | 命中率,三位小数,如 `0.973` |
| `X-Tail-Retransmit` | 响应 | 存在缺块时置 `1`,客户端应重发全文 |

---

## 6. 传输管线

**整条链路全程流式**。没有任何一步会先把整个请求体收进内存。

### 6.0 全景图:hash 在哪算、缓存何时读写、字节怎么走

阶段编号 ①~⑪ 与下面各小节的对应关系在括号里标出。

```
【客户端进程：TailTransport 随 http_client 活，状态都在这里】

  OpenAI SDK
    │  已序列化的 JSON body —— 客户端唯一一份完整副本，而且是 bytes
    ▼
  ① TailTransport.handle_request                    （§6.1）
    │    json.loads → payload dict
    ▼
  ② render.iter_render(payload)                     （§3）
    │    messages JSON → 一维文本片段流；确定性渲染是全部缓存的前提
    ▼
  ③ chunking.BlockHasher.feed(piece)          ◀◀◀  hash 在这里算（客户端）
    │    攒满 512 B 出一个满块
    │    block_hash = blake2b(parent_hash ‖ block)[:16]
    │    父链保证「同内容不同前缀 → 不同 id」，即前缀语义（与 vLLM 一致）
    │    只把满块交出去；尾块每轮重发（内容还会变，缓存它没意义）
    ▼
  ④ Encoder._encode_block(bid, block)         ◀◀◀  缓存「读」= 查本地 known 集合
    │    bid ∈ known ?
    │      ├─ 是 → 引用帧   1 + 16      = 17 B   （不发内容）
    │      └─ 否 → 新块帧   1 + 16 + 512 = 529 B  （内容全发）→ known.add(bid)
    ▼
  ⑤ codec._coalesce(16 KB)                          （§6.5）
    │    把上千个 17 B 小帧并成 ~16 KB 一片
    ▼
  ⑥ httpx2 内层 transport → HTTP/1.1 chunked        （§6.2）
         Content-Type: application/x-tail-v2，删除 Content-Length
         800 KB 上下文（1,600 块）：冷启动 1,600×529 = 846,400 B（≈827 KB），
         全命中 1,600×17 = 27,200 B（≈27 KB）—— 帧头就是全命中时的全部线上内容

【公网】只走需要的字节。上行是这里唯一稀缺的东西。

【网关进程】

  ⑦ request.stream() → _AsyncToSyncBody              （§6.2 / §6.4）
    │    有界队列 maxsize=8 提供背压：解码慢则后台协程阻塞，不会整包落内存
    ▼
  ⑧ frames.FrameDecoder.feed → (kind, payload)
    │    new → (block_hash, content)   ← hash 是发送方算好随帧传来的，网关不算
    │    ref → block_hash              ← 网关只有 id，没有内容
    ▼
  ⑨ 缓存「读」：storage.get_many(refs)        ◀◀◀  唯一的外部存储读
    │    ref → 按 id 取块（Redis MGET，一次往返取回全部引用块）
    │    new → 内容直传，同时 put_many 入库（唯一的回写点）
    │    取不到 → MissingBlock → 立刻断上游 + 409（§8），绝不跳过继续解
    ▼
  ⑩ render.RenderParser.feed(piece)                  （§3）
    │    <|标记|> 切分 + json.loads 还原 → (PARAM | MESSAGE_START | MESSAGE_END)
    ▼
  ⑪ iter_payload_json → _AsyncJsonStream             （§6.2）
    │    首片立刻发（不凑批），之后每 256 KB 一批，每条 message 完成即序列化
    ▼
  上游 LLM —— 它不知道 Tail 存在

【响应方向】

  上游 SSE ──── 原样逐块透传 ────> 客户端 SDK
    网关附加 X-Tail-Version / X-Tail-Status / X-Tail-Hit
    缺块时改为 409 + X-Tail-Retransmit: 1
    客户端 TailTransport 看到该头 → invalidate() → 重发一次（对 SDK 透明）
```

**状态归属**——「谁记得什么」是这套设计的核心,所有坑都出在这张表上:

| 状态 | 谁持有 | 生命周期 | 800 KB 上下文下的大小 |
| --- | --- | --- | --- |
| 渲染文本 | 没人落地 | 随流通过 | 0（只有片段） |
| 块顺序 | **请求的帧序列本身** | 一次请求 | 由帧顺序表达，不额外存 |
| 块哈希 | 客户端算，随帧传给网关 | 一次请求 | 引用帧里 16 B/块 |
| 「对端有哪些块」 | 客户端 `Encoder._known` | 随 client 实例，**无淘汰** | **210 KB**（131 B/块） |
| 块内容 | 网关存储（Redis / dbm） | TTL 或 LRU | 822 KB 原始内容 |
| 缺块判定 | 网关 `Decoder.missing` | 一次请求 | — |

三个直接后果:

1. **hash 必须在客户端算**——因为「发引用还是发内容」是发送方在发出那一刻就要做的
   决策；服务端要把字节收下来才能算 hash,而那正是客户端想省掉的那件事（循环依赖）。
2. **网关不校验客户端声称的 hash**——它没有父链,只能信。这是当前唯一的正确性
   缺口（§11），修法是让解码器自己维护父链重算，不改线缆。
3. **客户端 known 集合无界**——服务端嵌 SDK 的形态下随会话数线性涨。

与已定方向的差距（未实现，设计未定稿）:把 ④ 的逐块引用换成「前缀凭据 (长度, 摘要)」、
把 ③ 的 hash 挪到网关、把 ⑨ 的「按 id 取块」换成「按前缀凭据走存储里的链」——
那时 ④ 会发 17 B/块 × 1,600 = 27 KB 的东西只剩下一个请求头。

### 6.1 客户端接入点:TailTransport

第 ① 步的入口是 `TailClient.iter_encode_request(payload)`，但上层（`openai` SDK）
只会交出**已经序列化好的 JSON body**。要把它换成帧流，需要一个「body 改写」的
接入点。

选 `httpx2` 的 **transport**，不选 openai SDK 的内部方法：

| 接入点 | 可行性 |
| --- | --- |
| monkey-patch `openai` 私有方法 | 旧实现的做法。旧协议只需改 `options.json_data` 加个头，可行；新协议要把整个 body 换成**二进制流**，还得维护本地已知块集合，SDK 内部改不动 |
| 应用层手写 HTTP | 可行但要求用户重写全部调用代码，违背「现有代码不变」 |
| **`httpx2.Client(transport=...)`** | ✅ 公开扩展点，SDK 内部结构一行不碰，只替换「字节怎么发出去」 |

`TailTransport.handle_request` 做的事：

```
request.read()                  → 已序列化的 JSON body
json.loads                      → payload dict
iter_encode_request(payload)    → 帧流生成器(不落地、不拼大 bytes)
Content-Type → application/x-tail-v2,删掉 Content-Length(改 chunked)
内层 transport.handle_request   → 真正发出
响应带 X-Tail-Retransmit        → client.invalidate() 后全量重发一次
```

只拦 `/chat/completions` 且 `Content-Type` 含 `json` 的请求；其余（`/models`、
embeddings、文件上传）原样透传。body 不是合法 JSON 时也不猜，原样发出。

#### 本地状态:只记哈希集合,不缓存内容

客户端**不留正文副本**。`codec.Encoder` 只维护一个「对端已有块」的哈希集合：

```
block_hash in known  →  17 字节引用帧
否则                  →  新块帧(带内容)，并把哈希加入 known
```

键是 16 字节块 id，值是「有」这一事实。实测（约 79 KB 上下文、174 个满块）：

| 项 | 量 |
| --- | --- |
| 已知集合净占内存 | **105 ~ 131 B/块**（CPython `set[bytes]`：49 B 的 bytes 对象 + 哈希表槽位） |
| 79 KB 上下文（158 块） | 16,582 B |
| 800 KB 上下文（1,600 块） | **210,104 B** |
| 冷启动线上体积 | 82,422 B |
| 全命中后线上体积 | 6,054 B（省 92.7%） |

即：用本地约 12 KB 的哈希集合，换掉每轮约 76 KB 的上行内容。**但注意放大倍数**：
纯哈希理论值是 16 B/块，实际开销是它的 **8 倍**；而且集合无淘汰，内存随
「会话数 × 上下文长度」线性增长（1,000 个 800 KB 会话 ≈ 205 MB）。

三个必须知道的边界：

1. **必须复用同一个 client 实例**。集合挂在 `TailClient` 上——每轮
   `OpenAI(...)` 新建一个就等于永远冷启动（实测 6 轮合计 283 KB vs 61 KB，
   差 78%）。`TailTransport` 因此随 `http_client` 活，要和 SDK client 同寿命。
2. **复用是前缀形状的，不是任意块去重**。块哈希沿用 vLLM 的父链
   （`bid = hash(parent, block)`），所以一句「插入到中间」会让其后所有块哈希改变，
   全部失效；纯追加才几乎免费。这与 vLLM 前缀缓存、DeepSeek 上下文缓存的语义一致。
3. **没有逐块 ACK**。客户端是乐观假设「发出去的对端就存下了」。服务端存储被清空
   或淘汰时，用 `X-Tail-Retransmit` 一次性告知，客户端 `invalidate()` 后全量重发一次。
   代价是一次多余的往返，不会出现内容错误（新块帧自带完整内容）。

集合本身不做淘汰（无 LRU）。CLI 形态（1~3 个会话）下这无所谓；**服务端嵌 SDK
的形态下这是真问题**：内存随会话数线性涨且永不回收。修法是 HPACK 式「容量可协商」——
编码器与解码器对「表能有多大」达成一致，客户端按容量淘汰。但容量必须 ≤ 服务端
实际持有的块，否则客户端会引用不存在的块、白付一次重发。

对端块存储有 TTL 时客户端并不知道，只会在下次 miss 时经 `X-Tail-Retransmit` 自愈。

### 6.2 启动 HTTP chunked 到上游

网关用 `httpx2` 的 `AsyncByteStream` 适配:每次 `next()` 得到一个 JSON 片段就
立刻 `yield`,不等全部生成完。解码是 CPU 密集的同步代码,放进**单线程**
`ThreadPoolExecutor`(惰性迭代器非线程安全,用默认线程池会丢消息)。

### 6.3 普通 JSON 透传路径

`Content-Type` 不是 `application/x-tail-v2` 时,网关**原样转发**,同样直接透传
`request.stream()`——不先收齐。用于灰度对比与排障。

### 6.4 流式性的实测证据

body 34610B / 34 片,每片间隔 20ms:

```
客户端全部发完耗时:         674ms
上游收到首片时,客户端已发出:  2/34 片      ← 才发出 6% 就开始转发
上游 首片 → 收完:            673ms
```

若先 `await request.body()` 收齐再转发,上游首片会在 **674ms** 之后才到。
这条已固化为测试 `test_upstream_receives_before_client_finishes`,
并验证过:把实现改回收齐再转发,该测试立即失败。

---

### 6.5 分片大小:16 KB 的依据

帧可以很碎:一个 512B 块编成引用帧只有 17 字节。如果每帧单独交给 HTTP,
800 KB 上下文就是 1500+ 个 chunk,每一片都要过一遍「写 socket / 读 socket /
跨线程」的开销——实测多付约 170 ms,而编码本身只要 3.7 ms。所以要合并。

取 16 KB 有两个依据:

1. **和 nginx 的默认缓冲一致**。`client_body_buffer_size` 默认 `8k|16k`,
   [文档](https://nginx.org/en/docs/http/ngx_http_core_module.html)原文是
   「等于两个内存页,x86 与 x86-64 是 8K,其它 64 位平台通常是 16K」。
   取这一档,主流反向代理不需要为 Tail 调参。
2. **本地实测 ≥8 KB 后无差别**。888 KB 热请求按 4K/8K/16K/32K/64K 合并:

   | 合并大小 | 片段数 | 端到端中位数 |
   |---|---|---|
   | 4 KB | 7 | 38.4 ms |
   | 8 KB | 4 | 39.1 ms |
   | 16 KB | 2 | 37.3 ms |
   | 32 KB | 1 | 37.7 ms |
   | 64 KB | 1 | 37.7 ms |

   取 16 KB 而不是更大,是为了让首个片段尽早发出——对端收到第一个块就能开始
   解码,没必要为了少几个 chunk 去推迟首字节。

真正决定网络分段的是 TCP MSS(~1460 B)与 TSO/GSO,不是这里的合并大小;
这一层只影响每 chunk 的固定开销。

## 7. 存储

扁平块集合,**没有链、没有 prev 指针、没有引用图**。顺序由请求里的帧序列直接表达,
因此不需要引用计数、不需要访问驱动续期、不需要缺块补发协议、不存在"部分命中"概念。

```
blk:{block_hash}  →  块字节
```

| 后端 | 说明 |
|---|---|
| `MemoryStorage` | 进程内字典 + 可选 LRU,重启即丢(默认) |
| `DbmStorage` | 零依赖,单机部署 |
| `RedisStorage` | MGET 批量取块,生产用 |

**只存满块**(vLLM: "only cache full blocks")。不满的尾块内容还会被后续追加改变,
缓存它没有意义。

---

## 8. 降级与恢复

任何一块取不到(存储被清、服务重启、TTL 过期、LRU 淘汰),解码器**立刻抛**
`MissingBlock`,不走「跳过继续解」:

1. 抛异常时请求体上传被中断,httpx 直接断掉上游连接——**上游拿不到完整请求,
   不会开始推理**。
2. 网关回 HTTP 409 + `X-Tail-Retransmit: 1` + `X-Tail-Status: degraded`
   + `X-Tail-Hit`(命中率快照)。
3. 客户端 `invalidate()` 清空本地已知块集合,把全文当新一轮发。新块帧自带完整内容,
   所以**不需要任何补发协议**。

为什么不「跳过缺块、拼出能拼的部分照常转发」——两种洞都不能接受:

| 洞的位置 | 跳过后的后果 |
| --- | --- |
| 落在 UTF-8 多字节字符中间(512 B 块边界必然切开汉字) | 后续片段首字节非法 → `UnicodeDecodeError` → 客户端看到传输层异常,**不是**重发信号 |
| 落在 message 边界 | 拼出的 prompt 语义已变,但仍是合法 JSON → **静默转发给上游**,白跑一次完整推理再把结果丢掉 |

另外,`missing` 是在生成器**结束时**才赋值的;旧代码在异常路径上根本走不到赋值,
所以「响应头带 X-Tail-Retransmit」那条路在抛异常时永远不生效。现在统计在抛之前
先落快照(`DecodeSession._snapshot`)。

---

## 9. 实测数据

**全部数字由脚本生成、夹具固定、可复现**:

| 脚本 | 产出 |
|---|---|
| `python tools/bench.py` | 体积、各环节吞吐、对话周期、客户端本地状态 |
| `python tools/e2e_bench.py` | 端到端 wall clock(`Tail` vs 原生透传) |

全部使用 **wall clock**(用户实际感受到的时间)。跑之前需确认机器空闲,理由见本节末尾。

### 体积(81 条消息,75 个满块)

| | 字节 | 相对原始 JSON |
|---|---|---|
| 原始 JSON | 40,141 | — |
| 渲染文本 | 38,262 | 省 5% |
| 冷启动帧流 | 39,838 | 省 1% |
| **全命中帧流** | **1,276** | **省 97%** |

冷启动只省 1%:渲染省下的 5% 又被帧头吃掉(每 512B 块付 21B ≈ 4.1%)。
真正的收益全在第二轮以后。

### 真实对话周期(200 KB → 800 KB → compact → 200 KB)

长会话的实际形态。夹具含真实规模的 `tools`(38 KB)与 `system`(8 KB)固定前缀:

| 轮次 | 原始 JSON | 公网发送 | 占比 | 命中率 |
|---|---|---|---|---|
| 起点 ~200 KB(冷) | 286,073 | 297,628 | 104.0% | 0.0% |
| 增长中 ~455 KB | 466,262 | 197,167 | 42.3% | 61.3% |
| 增长中 ~690 KB | 706,514 | 265,484 | 37.6% | 65.9% |
| 涨到 ~807 KB | 826,640 | 148,444 | 18.0% | 85.4% |
| **compact 之后** | 280,008 | 245,482 | 87.7% | 16.3% |
| compact 后 +1 轮 | 340,071 | 72,152 | 21.2% | 82.2% |
| compact 后 +2 轮 | 400,134 | 74,283 | 18.6% | 84.8% |

两个要点:

1. **冷启动略大于原生**(104%):纯帧头开销。第二轮回本,之后一路降到 18%。
2. **compact 不是全失效**。命中率恰好等于未变前缀占比——`tools` + `system` 没动,
   这部分照常命中;失效的只有被摘要替换掉的 messages。之后几轮立刻回到 82~85%。

### 速度(38 KB 请求,wall clock)

| 环节 | 吞吐 | 单次耗时 |
|---|---|---|
| 渲染 JSON → 文本 | 287 MB/s | 0.13 ms |
| 切块 + 哈希 | 534 MB/s | 0.07 ms(约 1.0 µs/块) |
| 编码 文本 → 帧流(冷,全为新块) | 417 MB/s | 0.09 ms |
| 编码 文本 → 帧流(全命中) | 479 MB/s | 0.08 ms |
| **网关整条:帧流 → JSON** | **56 MB/s** | **0.72 ms** |

客户端相对原生 SDK 的额外开销:render + 切块 + 编码 **0.26 ms**;原生
`json.dumps` 0.05 ms。

### 速度(真实体积,线性)

两端口径(不含网络),随体积线性增长:

| 请求体 | 客户端编码 | 网关解码并重建 JSON | 合计 |
|---|---|---|---|
| 66 KB | 0.27 ms | 1.32 ms | 1.59 ms |
| 242 KB | 0.93 ms | 4.53 ms | 5.46 ms |
| 418 KB | 1.63 ms | 8.25 ms | 9.88 ms |
| 888 KB | 3.46 ms | 18.01 ms | 21.47 ms |

### 端到端 wall clock(Tail vs 原生透传)

同一请求体、同一网关,客户端唯一差别是挂不挂 `TailTransport`:

| 请求体 | 公网上行(实测) | 原生透传 | Tail 帧流(热) | 倍数 |
|---|---|---|---|---|
| 66 KB | 2,279 B(省 97%) | 2.9 ms | 7.5 ms | 2.6× |
| 242 KB | 8,263 B(省 97%) | 3.5 ms | 15.1 ms | 4.3× |
| 418 KB | 14,230 B(省 97%) | 4.7 ms | 22.8 ms | 4.9× |
| 888 KB | 30,176 B(省 97%) | 7.5 ms | 45.4 ms | 6.1× |

透传只是搬字节;帧流要解码 + 反渲染 + 重建 JSON,所以**在回环上更慢**。
收益在上行字节,不在网关 CPU:888 KB 的请求,公网只发 30 KB。

流式性仍然保持:首个 JSON 片段 **0.6 ms** 就交给上游,不等全部解码完。

#### 慢上行链路:这才是 TTFT 的前置段

回环没有带宽瓶颈,所以上面的对比看不出收益。按公网上行带宽限速后
(`tools/e2e_bench.py` 里的 `ThrottledTransport`),同一个 770 KB 请求:

| 上行带宽 | 原生 JSON 透传 | Tail 帧流(热) | 提速 |
|---|---|---|---|
| 10 Mbps | 733 ms | **45 ms** | **16.3×** |
| 5 Mbps | 1,461 ms | **63 ms** | **23.3×** |
| 2 Mbps | 3,643 ms | **135 ms** | **26.9×** |

上行从 770 KB 降到约 30 KB,请求提交时间少了一个数量级。**这是本项目存在的理由**:
在慢链路上,TTFT 的前置段就是这段时间。

### 客户端本地状态

- 已知块 104 个,只存 16 字节块 id(**不留正文副本**),纯哈希 1,664 B
- 复用同一个 client,6 轮合计 5,973 B;每轮新建则 14,745 B(永远冷启动,差 59%)

### 优化过程中修掉的两个真实瓶颈

都在 800 KB 场景下暴露,且都不是「算法慢」而是「切得太碎」:

| 问题 | 现象 | 修法 |
|---|---|---|
| 编码器每个帧单独产出 | 800 KB 热请求产生 **1,542 个 17 B 片段**,每个都成一个 HTTP chunk,网关逐个跨线程搬运 | `codec._coalesce` 合并到 16 KB/片 → **2 片**(取值依据见 6.5) |
| 网关每片一次线程池往返 | 57 片 × ~0.5 ms 往返 → 取流 45.7 ms | `_AsyncJsonStream` 批量取(256 KB/批,首片立即发) → **4 片 / 21 ms** |

合计把 789 KB 热请求的端到端从 **284 ms 降到 32 ms**。

### 测量方法上的两个坑(都踩过)

1. **不要用批量 `timeit` 的均值**:GC 暂停会污染批次均值。同一调用批量测得 1.1 ms,
   单次测得 0.78 ms。现改为每次计时前 `gc.collect()` 取最小值。
2. **机器状态会彻底污染 wall clock**:swap 用满(实测 4.6 GB / 5.1 GB)且 load 53 时,
   同一个 800 KB 请求能测出 **wall 150~300 ms 而 CPU 只有 17 ms**,进程是被换出而非在算。
   据此曾错误得出「网关比透传慢 50 倍」。测量前先看 `uptime` 与 `vm.swapusage`。

## 10. 显式不采用的做法

为便于 review,列出被明确否决、且代码中**已无残留**的方案:

| 方案 | 否决理由 |
|---|---|
| CDC / buzhash 分块 | 参考实现都用固定间隔;逐字节哈希仅 0.66 MB/s;已整体移除 |
| `\x1e`/`\x1f` 控制字符分隔 message | 正文里完全可能出现这两个字节;已删 |
| 正文里的 `<|v2|>` 版本头 | 版本号落在第一个块上,升版即全量失效;且参考实现无此物;已删 |
| `name(args)` 的 tool_calls 语法 | `id`/`type` 丢失;正文里的括号会被误判;已删 |
| 分段(segment)后段内独立切块 | 依赖 `\x1f`;定长块下无收益;`segment.py` 已删 |
| 先 `await request.body()` 再解码 | 整包落内存,违背流式目的;已改为 `request.stream()` |
| 链式存储(`pfx:{hash} → [seg hashes]`) | 读取要顺链走,且顺序信息与帧序列重复;已改扁平集合 |

---

## 11. 审核清单

> ⚠️ **未通过项**:网关不校验客户端声称的块哈希(解码器无父链)。可被用来污染
> 全局块存储,导致跨用户静默内容损坏。修法见 5.2 与新块帧格式,不需要改线缆。


建议按这个顺序核对:

1. **渲染确定性**:同一 payload 连续渲染 1000 次是否字节完全相同?
   (`TestRender::test_deterministic` / `test_deterministic_ignores_key_order`)
2. **往返保真**:随机 payload 渲染→反渲染是否完全相等?
   (`test_roundtrip_with_hostile_content` 覆盖正文含 `<|user|>`、引号、反斜杠、换行)
3. **流式切块一致性**:任意字节边界喂入,结果是否与一次性切分完全一致?
   (`test_matches_one_shot` / `test_hashes_match_blocked_hashes`)
4. **哈希链**:相同内容在不同前缀下 `block_hash` 是否不同?(`test_block_hash_depends_on_parent`)
5. **前缀复用**:尾部追加时,前面满块的 id 是否全部不变?(`test_append_only_grows_tail`)
6. **全程流式**:上游开始收包时客户端是否远未发完?(`test_upstream_receives_before_client_finishes`)
7. **降级恢复**:存储清空后是否正常降级并要求重发,恢复后是否继续命中?
8. **体积**:冷启动 / 全命中 / 多轮增量三项是否符合 §9。
