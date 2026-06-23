-- 前缀哈希(字符串版)。
-- 对应设计文档第 5.2 节。Lua 侧不做真实 BPE 分词(成本高),
-- 而是把前缀 messages 序列化成稳定字符串后做 SHA256,取前 16 个十六进制字符。
-- 稳定性/抗碰撞:用长度前缀 + 分隔符编码,避免 "ab"+"c" == "a"+"bc"。
local resty_sha256 = require "resty.sha256"
local resty_str = require "resty.string"   -- to_hex

local M = {}

-- 对任意字符串计算 SHA256,返回 16 位十六进制前缀。
-- (导出供 merkle.lua 复用)
M.sha256_hex16 = function(s)
    local sha = resty_sha256:new()
    sha:update(s)
    local bin = sha:final()
    return string.sub(resty_str.to_hex(bin), 1, 16)
end

local sha256_hex16 = M.sha256_hex16

-- 把单条 message 编码成稳定字符串(参与哈希)。
-- (导出供 merkle.lua 复用)
M.encode_message = function(msg)
    local role = msg.role or ""
    local content = msg.content
    if type(content) == "table" then
        -- 多模态/工具:用 cjson 的稳定序列化
        local cjson = require "cjson.safe"
        content = cjson.encode(content)
    else
        content = tostring(content or "")
    end
    -- #role 的长度前缀 + 分隔符,杜绝边界歧义
    return tostring(#role) .. ":" .. role .. "\x00" ..
           tostring(#content) .. ":" .. content .. "\x01"
end

local encode_message = M.encode_message

-- 对一段 messages 列表(前缀)计算哈希。
-- @param messages table: OpenAI messages 数组
-- @return string: 16 位十六进制哈希
function M.prefix_hash(messages)
    if not messages or #messages == 0 then
        return sha256_hex16("empty")
    end
    local parts = {}
    for i, msg in ipairs(messages) do
        parts[i] = encode_message(msg)
    end
    local blob = table.concat(parts, "")
    return sha256_hex16(blob)
end

-- 仅对前 prefix_len 条消息计算哈希。
function M.prefix_hash_n(messages, prefix_len)
    if prefix_len <= 0 then
        return M.prefix_hash(nil)
    end
    local sub = {}
    for i = 1, prefix_len do
        sub[i] = messages[i]
    end
    return M.prefix_hash(sub)
end

return M
