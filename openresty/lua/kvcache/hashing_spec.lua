-- Lua 单元测试:哈希工具(扩充版,覆盖更多边界场景)。
local hashing = require "kvcache.hashing"

local pass, fail = 0, 0
local function check(name, cond)
    if cond then pass = pass + 1; print(("  PASS  %s"):format(name))
    else fail = fail + 1; print(("  FAIL  %s"):format(name)); end
end

-- ========== 基础属性 ==========
local h1 = hashing.prefix_hash({{role="user", content="hi"}})
check("稳定:同输入同哈希", h1 == hashing.prefix_hash({{role="user", content="hi"}}))
check("长度=16(64bit)", #h1 == 16)
check("是合法十六进制", h1:match("^[0-9a-f]+$") ~= nil)

-- ========== 顺序敏感性 ==========
local ha = hashing.prefix_hash({{role="user",content="a"},{role="user",content="b"}})
local hb = hashing.prefix_hash({{role="user",content="b"},{role="user",content="a"}})
check("顺序敏感:ab != ba", ha ~= hb)

-- ========== 边界抗碰撞(核心不变量)==========
check("边界抗碰撞:[abc] != [ab]+[c]",
    hashing.prefix_hash({{role="user",content="abc"}}) ~=
    hashing.prefix_hash({{role="user",content="ab"},{role="user",content="c"}}))
check("边界抗碰撞:role content 不混",
    hashing.prefix_hash({{role="user",content="x"}}) ~=
    hashing.prefix_hash({{role="use",content="rx"}}))

-- ========== role 与 content 都影响 ==========
local r1 = hashing.prefix_hash({{role="user", content="hi"}})
check("role 影响哈希", r1 ~= hashing.prefix_hash({{role="system", content="hi"}}))
check("content 影响哈希", r1 ~= hashing.prefix_hash({{role="user", content="ho"}}))

-- ========== 前缀扩展 ==========
local base = hashing.prefix_hash({{role="system",content="S"},{role="user",content="1"}})
local ext  = hashing.prefix_hash({{role="system",content="S"},{role="user",content="1"},{role="assistant",content="a"}})
check("前缀扩展哈希变化", base ~= ext)

-- ========== prefix_hash_n 等价性 ==========
local msgs = {{role="system",content="S"},{role="user",content="1"},{role="assistant",content="a"}}
check("prefix_hash_n(2) == 前2条哈希",
    hashing.prefix_hash_n(msgs, 2) == hashing.prefix_hash({{role="system",content="S"},{role="user",content="1"}}))
check("prefix_hash_n(3) == 全部哈希",
    hashing.prefix_hash_n(msgs, 3) == hashing.prefix_hash(msgs))
check("prefix_hash_n(1) == 单条哈希",
    hashing.prefix_hash_n(msgs, 1) == hashing.prefix_hash({{role="system",content="S"}}))

-- ========== 空与边界 ==========
check("空列表有哈希", #hashing.prefix_hash({}) == 16)
check("prefix_hash_n(0) 有哈希", #hashing.prefix_hash_n(msgs, 0) == 16)

-- ========== 多模态/工具 content(table)==========
local ht = hashing.prefix_hash({{role="assistant", content={{type="text",text="ok"}}}})
check("多模态 content 有哈希", #ht == 16)
check("多模态 vs 字符串不同",
    ht ~= hashing.prefix_hash({{role="assistant", content="ok"}}))

-- ========== 特殊字符 / 长内容 ==========
check("含分隔符的内容不冲突",
    hashing.prefix_hash({{role="user", content="a\x00b"}}) ~=
    hashing.prefix_hash({{role="user", content="a"},{role="user", content="b"}}))
check("含中文稳定", #hashing.prefix_hash({{role="user", content="你好世界"}}) == 16)
check("含 emoji 稳定", #hashing.prefix_hash({{role="user", content="😀🎉"}}) == 16)
local long = string.rep("x", 10000)
check("超长内容(10k)有哈希", #hashing.prefix_hash({{role="user", content=long}}) == 16)

-- ========== content 缺失/空 ==========
check("content 缺省(只有 role)有哈希",
    #hashing.prefix_hash({{role="user"}}) == 16)
check("content 空字符串稳定",
    hashing.prefix_hash({{role="user", content=""}}) ==
    hashing.prefix_hash({{role="user", content=""}}))

-- ========== 单条与多条不会意外碰撞 ==========
check("单条 vs 双条不同",
    hashing.prefix_hash({{role="user", content="ab"}}) ~=
    hashing.prefix_hash({{role="user", content="a"},{role="user", content="b"}}))

print(("\n总计: %d 通过, %d 失败"):format(pass, fail))
if fail > 0 then os.exit(1) end
