-- Segment 切分 —— 按一个 LLM 回合切分 messages。
-- 对应设计文档 §3.1。
--
-- Segment 定义:
--   segment = (1 × assistant)? + (m × tool) + (n × user)
--   约束:
--     - assistant 至多 1 条(回合回复;首个 segment 可没有;streaming 续写可连续)
--     - m + n ≥ 1  且  m · n = 0  (tool 与 user 互斥,不同时出现)
--
-- 切分算法(从语义出发):
--   assistant 是"回合的回复",依附于其后 的非 assistant 消息。
--   遇到非 assistant(tool/user)且前一段已闭合 → 开新段,把累积 assistant 作新段开头。
--   m·n=0:同一段内 tool 与 user 互斥 —— 段内已有 tool 时遇 user(或反之)→ 闭合开新段。
--   末尾孤立 assistant(无非 assistant 收尾)→ 合并回前段(streaming 中断场景)。
local M = {}

local function is_assistant(msg) return msg.role == "assistant" end
local function is_tool(msg) return msg.role == "tool" end
local function is_user_like(msg) return msg.role == "user" or msg.role == "system" end

-- 把 messages 数组切成 segment 列表。不修改输入。
-- @return table: {{msg,...}, {msg,...}, ...},展平后与原数组一致。
function M.split(messages)
    if not messages or #messages == 0 then return {} end

    local segments = {}
    local cur = {}
    local cur_kind = nil  -- nil / "tool" / "user":当前段非 assistant 消息的类型

    local function close()
        if #cur > 0 then
            segments[#segments + 1] = cur
        end
        cur = {}
        cur_kind = nil
    end

    for _, msg in ipairs(messages) do
        if is_assistant(msg) then
            -- assistant:若当前段已有非 assistant(已闭合形态)→ 开新段
            if cur_kind ~= nil then
                close()
            end
            cur[#cur + 1] = msg
            -- assistant 不改变 cur_kind(它等"后续"非 assistant)
        elseif is_tool(msg) then
            -- tool:若段内已有 user → 违反 m·n=0 → 闭合开新段
            if cur_kind == "user" then
                close()
            end
            cur[#cur + 1] = msg
            cur_kind = "tool"
        else  -- user / system / 其它
            -- user:若段内已有 tool → 违反 m·n=0 → 闭合开新段
            if cur_kind == "tool" then
                close()
            end
            cur[#cur + 1] = msg
            cur_kind = "user"
        end
    end
    close()

    -- 收尾修正:末尾全是 assistant(无非 assistant 收尾,违反约束)→ 合并回前段。
    -- 真实 LLM 流的合法场景(streaming 中断、末尾 assistant 残留)。
    if #segments >= 2 then
        local last = segments[#segments]
        local has_non_assistant = false
        for _, m in ipairs(last) do
            if not is_assistant(m) then has_non_assistant = true; break end
        end
        if not has_non_assistant then
            -- 末尾全是 assistant,合并到前一段
            for _, m in ipairs(last) do
                segments[#segments - 1][#segments[#segments - 1] + 1] = m
            end
            segments[#segments] = nil
        end
    end
    return segments
end

-- 校验单个 segment 是否满足定义约束(测试用)。
-- @return boolean
function M.validate(seg)
    if not seg or #seg == 0 then return false end
    local n_tool = 0
    local n_user = 0
    for _, m in ipairs(seg) do
        if is_tool(m) then n_tool = n_tool + 1 end
        if is_user_like(m) then n_user = n_user + 1 end
    end
    if n_tool + n_user < 1 then return false end       -- m + n >= 1
    if n_tool > 0 and n_user > 0 then return false end  -- m · n = 0
    return true
end

-- 校验:segments 展平后与原 messages 完全一致(字节级,顺序不变)。
function M.flatten_match(segments, messages)
    local flat = {}
    for _, seg in ipairs(segments) do
        for _, m in ipairs(seg) do flat[#flat + 1] = m end
    end
    if #flat ~= #messages then return false end
    for i = 1, #flat do
        if flat[i] ~= messages[i] then return false end
    end
    return true
end

return M
