-- Merkle 前缀链 —— messages 段的增量 hash 链。
-- 对应设计文档 §3.4。
--
-- 核心定义:
--   H(prefix_0) = "0"                                    # 空(0 个 segment)
--   H(prefix_k) = sha256_hex16( H(prefix_{k-1}) || "|" || seg_hash_k )
--
-- 性质:H(prefix_k) 只依赖 H(prefix_{k-1}) 和 seg_hash_k,加一段只算 1 个 hash。
local hashing = require "kvcache.hashing"
local sha256_hex16 = hashing.sha256_hex16
local encode_message = hashing.encode_message

local M = {}

M.EMPTY_HASH = "0"  -- 空前缀的固定哈希

-- 一个 segment 的哈希。
-- @param seg table: segment(messages 数组)
-- @return string: 16 位 hex
function M.segment_hash(seg)
    local parts = {}
    for i, msg in ipairs(seg) do
        parts[i] = encode_message(msg)
    end
    return sha256_hex16(table.concat(parts, ""))
end

-- 单步推进:H(prefix_k) = sha256_hex16(H(prefix_{k-1}) || "|" || seg_hash_k)。
function M.chain_step(prev_hash, seg_hash)
    return sha256_hex16(prev_hash .. "|" .. seg_hash)
end

-- 从一组 segment hash 计算链式前缀哈希(到末尾)。
-- @param seg_hashes table: {seg_hash_1, ..., seg_hash_k}
-- @return string: H(prefix_k)
function M.chain_hash(seg_hashes)
    local cur = M.EMPTY_HASH
    for _, sh in ipairs(seg_hashes) do
        cur = M.chain_step(cur, sh)
    end
    return cur
end

-- 从 segment hash 列表构建完整的 Merkle 链节点。
-- @return table: {{pfx_hash=..., node={prev=..., seg_ref=..., seg_count=...}}, ...}
-- 用于:首次写入一个完整 messages 前缀时,一次性生成所有节点。
function M.build_nodes(seg_hashes)
    local nodes = {}
    local prev_hash = M.EMPTY_HASH
    for i, sh in ipairs(seg_hashes) do
        local cur_hash = M.chain_step(prev_hash, sh)
        nodes[#nodes + 1] = {
            pfx_hash = cur_hash,
            node = {
                prev = prev_hash,
                seg_ref = sh,
                seg_count = i,
            },
        }
        prev_hash = cur_hash
    end
    return nodes
end

return M
