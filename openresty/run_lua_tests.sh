#!/usr/bin/env bash
# 统一运行所有 Lua 单元测试(用 OpenResty 的 resty CLI)。
# 用法: ./openresty/run_lua_tests.sh
set -e
PROJECT="/home/lloyd/ZCodeProject"
export PATH="$PROJECT/runtime/openresty/bin:$PATH"
export LUA_PATH="$PROJECT/openresty/lua/?.lua;;"

SPECS=(
    "$PROJECT/openresty/lua/kvcache/hashing_spec.lua"
    "$PROJECT/openresty/lua/kvcache/protocol_spec.lua"
    "$PROJECT/openresty/lua/kvcache/store_spec.lua"
)

total_pass=0
total_fail=0
for spec in "${SPECS[@]}"; do
    echo "=== $(basename "$spec") ==="
    out=$(resty "$spec" 2>&1) || true
    echo "$out"
    # 从最后一行 "总计: X 通过, Y 失败" 提取
    line=$(echo "$out" | grep "总计" | tail -1)
    p=$(echo "$line" | grep -oP '\d+(?= 通过)' || echo 0)
    f=$(echo "$line" | grep -oP '\d+(?= 失败)' || echo 0)
    total_pass=$((total_pass + p))
    total_fail=$((total_fail + f))
done

echo ""
echo "================ Lua 测试汇总 ================"
echo "  总通过: $total_pass"
echo "  总失败: $total_fail"
if [ "$total_fail" -gt 0 ]; then exit 1; fi
