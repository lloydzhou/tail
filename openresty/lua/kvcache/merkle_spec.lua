-- Lua 单元测试:Merkle 前缀链(v2.1 §3.4)。
-- 纯算法,不依赖 Kvrocks。
local merkle = require "kvcache.merkle"

local pass, fail = 0, 0
local function check(name, cond)
    if cond then pass = pass + 1; print(("  PASS  %s"):format(name))
    else fail = fail + 1; print(("  FAIL  %s"):format(name)); end
end

-- segment_hash
local sh1 = merkle.segment_hash({{role="user", content="hi"}})
check("segment_hash 长度=16", #sh1 == 16)
check("segment_hash 稳定", sh1 == merkle.segment_hash({{role="user", content="hi"}}))
check("不同 segment 不同 hash",
    sh1 ~= merkle.segment_hash({{role="assistant", content="hi"}}))

-- chain_hash
check("空链 = EMPTY_HASH", merkle.chain_hash({}) == merkle.EMPTY_HASH)
check("空链 = '0'", merkle.EMPTY_HASH == "0")

-- chain_step 单步
local step1 = merkle.chain_step("0", "abc123")
check("chain_step 长度=16", #step1 == 16)

-- chain_hash 一致性
local hashes = {"aaa", "bbb", "ccc"}
local full = merkle.chain_hash(hashes)
local partial = merkle.chain_hash({"aaa", "bbb"})
local extended = merkle.chain_step(partial, "ccc")
check("增量推进一致", extended == full)

-- build_nodes
local nodes = merkle.build_nodes(hashes)
check("build_nodes 节点数=3", #nodes == 3)
check("末节点 hash == chain_hash", nodes[#nodes].pfx_hash == full)
check("首节点 prev='0'", nodes[1].node.prev == "0")
check("链式 prev 正确", nodes[2].node.prev == nodes[1].pfx_hash)
check("节点 seg_count", nodes[3].node.seg_count == 3)
check("节点 seg_ref", nodes[2].node.seg_ref == "bbb")

-- 加一段只增 O(1):新 pfx = chain_step(prev_pfx, new_seg_hash)
local new_seg_hash = "ddd"
local new_pfx = merkle.chain_step(full, new_seg_hash)
local new_full = merkle.chain_hash({"aaa", "bbb", "ccc", "ddd"})
check("加一段增量计算一致", new_pfx == new_full)

print(("\n总计: %d 通过, %d 失败"):format(pass, fail))
if fail > 0 then os.exit(1) end
