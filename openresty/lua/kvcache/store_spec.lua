-- Lua 单元测试:store 模块(纯 Kvrocks,无 L1)。
-- 通过注入 mock resty.redis,模拟 Kvrocks 响应,验证 get/set_sync/set_async/del 逻辑。
-- 不依赖真实 Kvrocks 服务(真实链路由端到端测试覆盖)。
local pass, fail = 0, 0
local function check(name, cond)
    if cond then pass = pass + 1; print(("  PASS  %s"):format(name))
    else fail = fail + 1; print(("  FAIL  %s"):format(name)); end
end

-- 内存表模拟 Kvrocks,每次 store.connect 会创建一个新实例但共享同一个 kv
local kv = {}

local function make_mock_redis()
    local obj = {}
    function obj:set_timeouts() end
    function obj:connect() return true end
    function obj:set_keepalive() return true end
    function obj:set(k, v, ex, ttl)
        kv[k] = v
        return "OK"
    end
    function obj:get(k)
        if kv[k] == nil then return ngx.null end
        return kv[k]
    end
    function obj:del(k)
        local existed = kv[k] ~= nil
        kv[k] = nil
        return existed and 1 or 0
    end
    return obj
end

-- 让 require("resty.redis").new() 返回我们的 mock
package.preload["resty.redis"] = function()
    return { new = make_mock_redis }
end

-- 清掉已加载的 store,让它重新 require 我们的 mock redis
package.loaded["kvcache.store"] = nil
local store = require "kvcache.store"

local cjson = require "cjson.safe"
local cfg = {
    ttl = 100, jitter = 0,
    max_prefix_bytes = 8 * 1024 * 1024,
    hash_ns = "prefix_cache",
    kvrocks_host = "127.0.0.1", kvrocks_port = 6666,
}
local now = ngx and ngx.time() or os.time()

-- 1. set_sync 写入 + get 命中
local entry = {
    messages = { { role = "user", content = "hi" } },
    model = "m", prefix_length = 1, expire_at = now + 1000,
}
local ok, err = store.set_sync(cfg, "h1", entry)
check("set_sync 返回 true", ok == true)
local got, level = store.get(cfg, "h1")
check("set_sync 后 get 命中", got ~= nil)
check("get 命中 level=kvrocks", level == "kvrocks")
check("get 命中内容正确(messages)", got and got.messages[1].content == "hi")
check("get 命中 prefix_length 正确", got and got.prefix_length == 1)
check("get 命中 model 正确", got and got.model == "m")

-- 2. 不存在的 key
local got2, lvl2 = store.get(cfg, "nope")
check("get 不存在返回 nil", got2 == nil)
check("get 不存在 level=miss", lvl2 == "miss")

-- 3. del 删除后 miss
store.del(cfg, "h1")
local got3 = store.get(cfg, "h1")
check("del 后 get 返回 nil", got3 == nil)

-- 4. set_async 写入(直接传 blob)
local blob = cjson.encode(entry)
local ok4 = store.set_async(cfg, "h2", blob)
check("set_async 返回 true", ok4 == true)
check("set_async 后 get 命中", store.get(cfg, "h2") ~= nil)

-- 5. 超大内容 set_sync 拒绝
local big_entry = {
    messages = { { role = "user", content = string.rep("x", 1000) } },
    model = "m", prefix_length = 1, expire_at = now + 1000,
}
local cfg_small = { ttl = 100, jitter = 0, max_prefix_bytes = 10,
                    hash_ns = "prefix_cache", kvrocks_host = "127.0.0.1", kvrocks_port = 6666 }
local ok5, err5 = store.set_sync(cfg_small, "h3", big_entry)
check("超大内容 set_sync 返回 false", ok5 == false)
check("超大内容 err=too_large", err5 == "too_large")

-- 6. set_async 超大 blob 拒绝
local big_blob = string.rep("x", 1000)
local ok6, err6 = store.set_async(cfg_small, "h4", big_blob)
check("超大 blob set_async 返回 false", ok6 == false)
check("超大 blob err=too_large", err6 == "too_large")

-- 7. 过期 entry 被 get 判定为无效
kv["expired_key"] = cjson.encode({
    messages = { { role = "user", content = "old" } },
    model = "m", prefix_length = 1, expire_at = now - 100,  -- 已过期
})
local got7, lvl7 = store.get(cfg, "expired_key")
check("过期 entry get 返回 nil", got7 == nil)
-- level 可能是 "expired" 或经过 miss 路径,不强断言具体字符串

-- 8. 命中后内容是深拷贝语义(mock 直接存引用,但 store 不修改返回值)
local got8 = store.get(cfg, "h2")
check("命中 entry 含 expire_at", got8 and got8.expire_at ~= nil)

print(("\n总计: %d 通过, %d 失败"):format(pass, fail))
if fail > 0 then os.exit(1) end
