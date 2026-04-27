#!/usr/bin/env bash
# Probe: malformed type tag strings via endpoints that parse them.
#
# Targets:
#   - iotax_getOwnedObjects with a `StructType` filter
#   - sui_getCoins with a `coin_type` arg
#   - iotax_queryEvents with `MoveEventType`/`MoveEventModule` filters
#
# Type-tag strings are parsed by parse_iota_type_tag → move-core-types
# parser. The parser claims to bound depth (MAX_TYPE_DEPTH=128). We probe:
#   - extremely deep `vector<vector<...>>`
#   - very long module / function identifiers
#   - structurally valid but huge type-tag strings
#   - malformed addresses
#
# Each call must return a JSON-RPC error and the node must remain alive.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "typetag/$1" "$2" || return 1; }

ADDR='0x0000000000000000000000000000000000000000000000000000000000000001'

build_deep_vector() {
  python3 - "$1" <<'PY'
import sys
n = int(sys.argv[1])
inner = "u8"
for _ in range(n):
  inner = f"vector<{inner}>"
sys.stdout.write(inner)
PY
}

# Test parser depth limit at and around MAX_TYPE_DEPTH=128.
for depth in 64 127 128 129 256 1024 10000; do
  ttype=$(build_deep_vector "$depth")
  probe "owned_objects_struct_filter_${depth}" '{
    "jsonrpc":"2.0","id":1,"method":"iotax_getOwnedObjects",
    "params":["'"$ADDR"'", {"filter":{"StructType":"'"$ttype"'"},"options":null}, null, 50]
  }'
done

# Query events with a very long Move event module name.
long_ident=$(python3 -c 'import sys;sys.stdout.write("a"*100000)')
probe "query_events_huge_module" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_queryEvents",
  "params":[{"MoveEventModule":{"package":"0x2","module":"'"$long_ident"'"}}, null, 10, false]
}'

# get_coins with deep nested coin_type
ttype=$(build_deep_vector 200)
probe "get_coins_deep_coin_type" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getCoins",
  "params":["'"$ADDR"'","'"$ttype"'",null,10]
}'

# Malformed object id (right length, but invalid hex)
probe "get_object_bad_hex" '{
  "jsonrpc":"2.0","id":1,"method":"iota_getObject",
  "params":["0xZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"]
}'

# Wrong-length object id
probe "get_object_wrong_length" '{
  "jsonrpc":"2.0","id":1,"method":"iota_getObject",
  "params":["0x1"]
}'
