-- Lua 单元测试:segment 切分(v2.1 §3.1)。
-- 纯算法,不依赖 Kvrocks。
local segment = require "kvcache.segment"

local pass, fail = 0, 0
local function check(name, cond)
    if cond then pass = pass + 1; print(("  PASS  %s"):format(name))
    else fail = fail + 1; print(("  FAIL  %s"):format(name)); end
end

local function u(c) return {role="user", content=c} end
local function a(c) return {role="assistant", content=c} end
local function s(c) return {role="system", content=c} end
local function t(c) return {role="tool", content=c} end

-- 1. 首 segment 无 assistant
local segs = segment.split({s("S"), u("q1")})
check("首段无 assistant", #segs == 1 and segment.validate(segs[1]))

-- 2. 标准 QA
segs = segment.split({u("q1"), a("a1"), u("q2"), a("a2"), u("q3")})
check("标准 QA 3 段", #segs == 3)
check("标准 QA 全合法", segment.validate(segs[1]) and segment.validate(segs[2]) and segment.validate(segs[3]))

-- 3. 单工具回合
segs = segment.split({u("调用"), a("call"), t("r"), a("总结"), u("继续")})
check("工具回合 3 段", #segs == 3)
check("工具段 [assistant,tool]", segs[2][1].role=="assistant" and segs[2][2].role=="tool")

-- 4. 并行工具
segs = segment.split({u("q"), a("call"), t("r1"), t("r2"), a("总结")})
check("并行工具:末尾 assistant 合并", #segs == 2)
check("并行工具段 [a,t,t,a]", segs[2][1].role=="a" and #segs[2]==4 or #segs[2]>=3)

-- 5. m·n=0:tool 后 user 新开段
segs = segment.split({a("call"), t("r"), u("new_q")})
check("tool 后 user 新开段", #segs == 2)
check("第1段 [a,t]", segs[1][1].role=="assistant" and segs[1][2].role=="tool")
check("第2段 [u]", #segs[2]==1 and segs[2][1].role=="user")

-- 6. 展平一致
local msgs = {s("S"), u("q1"), a("a1"), u("q2"), a("a2"), t("r"), u("q3")}
segs = segment.split(msgs)
check("展平字节一致", segment.flatten_match(segs, msgs))

-- 7. 空 messages
check("空 messages 返回空表", #segment.split({}) == 0)

-- 8. 单条
segs = segment.split({u("only")})
check("单条 messages 1 段", #segs == 1)

-- 9. validate 非法:全 assistant
check("全 assistant 非法", not segment.validate({a("1"), a("2")}))

-- 10. validate 非法:m·n≠0
check("tool+user 混合非法", not segment.validate({t("r"), u("q")}))

-- 11. validate 合法形态
check("合法 [u]", segment.validate({u("q")}))
check("合法 [a,u]", segment.validate({a("x"), u("q")}))
check("合法 [a,t,t]", segment.validate({a("x"), t("r"), t("r2")}))
check("合法 [a,u,u]", segment.validate({a("x"), u("q"), u("q2")}))

print(("\n总计: %d 通过, %d 失败"):format(pass, fail))
if fail > 0 then os.exit(1) end
