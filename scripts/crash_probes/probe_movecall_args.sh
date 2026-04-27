#!/usr/bin/env bash
# Probe: unsafe_moveCall arguments. The endpoint converts JSON arguments
# to Move values via `IotaJsonValue::new` and `to_move_value`. The
# endpoint is reachable on a default-built fullnode.
#
# We pass valid call structure but stress argument parsing with edge
# cases (large numbers as strings, near-boundary integers, ascii-vs-hex
# byte vectors, empty optionals).
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "movecall/$1" "$2" || return 1; }

# Pick the faucet-funded address as sender.
SENDER=$(/home/user/iota/target/debug/iota client active-address 2>/dev/null | head -1)
GAS_OBJ=$(/home/user/iota/target/debug/iota client gas --json 2>/dev/null \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["gasCoinId"])' 2>/dev/null)
echo "sender=$SENDER gas=$GAS_OBJ"

if [ -z "$SENDER" ] || [ -z "$GAS_OBJ" ]; then
  echo "wallet not configured; skipping movecall probes"
  exit 0
fi

call() {
  # call <name> <package> <module> <function> <type_args_json> <args_json>
  local name="$1"; local pkg="$2"; local mod="$3"; local fn="$4"
  local ttypes="$5"; local args="$6"
  probe "$name" '{
    "jsonrpc":"2.0","id":1,"method":"unsafe_moveCall",
    "params":["'"$SENDER"'","'"$pkg"'","'"$mod"'","'"$fn"'",
      '"$ttypes"','"$args"',
      "'"$GAS_OBJ"'","100000000",null]
  }'
}

# A non-existent function — we only care that argument parsing happens.
# Many probes never reach package resolution because IotaJsonValue parsing
# happens earlier in the handler.
call "valid_small"        0x2 coin value '["0x2::iota::IOTA"]' '["'"$GAS_OBJ"'"]'
call "u64_max_str"        0x2 coin value '["0x2::iota::IOTA"]' '["18446744073709551615"]'
call "u64_overflow_str"   0x2 coin value '["0x2::iota::IOTA"]' '["99999999999999999999999999999999"]'
call "negative_str"       0x2 coin value '["0x2::iota::IOTA"]' '["-1"]'

huge_str=$(python3 -c 'import sys;sys.stdout.write("9"*500000)')
call "huge_string_arg"    0x2 coin value '["0x2::iota::IOTA"]' '["'"$huge_str"'"]'

call "many_args"          0x2 coin value '["0x2::iota::IOTA"]' '["1","2","3","4","5","6","7","8","9","10"]'

deep=$(python3 -c 'import sys;n=200;sys.stdout.write("["*n+"]"*n)')
call "deep_array_arg"     0x2 coin value '["0x2::iota::IOTA"]' '['"$deep"']'

call "many_type_args"     0x2 coin value '["u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64","u64"]' '["1"]'

deep_ttype=$(python3 -c '
import sys
n=200
inner="u64"
for _ in range(n): inner=f"vector<{inner}>"
sys.stdout.write(inner)
')
call "deep_type_arg_200"  0x2 coin value '["'"$deep_ttype"'"]' '["1"]'

# Mismatched array element types — should error out cleanly.
call "mixed_array"        0x2 coin value '["0x2::iota::IOTA"]' '[[1,"two",3]]'

# Object id with malformed length
call "bad_object_id"      0x2 coin value '["0x2::iota::IOTA"]' '["0xZZZ"]'
