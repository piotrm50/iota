#!/usr/bin/env bash
# Probe: malformed BCS / Base64 payloads sent to endpoints that bcs::from_bytes
# them.
#
# Targets:
#   - sui_dryRunTransactionBlock (decodes TransactionData via BCS)
#   - sui_executeTransactionBlock (decodes TransactionData via BCS)
#   - sui_devInspectTransactionBlock (decodes TransactionKind via BCS)
#
# We try:
#   - empty Base64
#   - "garbage" Base64 (valid base64 but invalid BCS)
#   - very large Base64 (~10MB) of zero bytes
#   - Base64 with invalid base64 chars (should error at decode, not panic)
#   - Base64 of a deeply nested type-input chain (vector<vector<...>>)
#
# Each call must return a JSON-RPC error and the node must remain alive.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "bcs/$1" "$2" || return 1; }

# 1) empty payload
probe "dry_run_empty_b64" '{
  "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
  "params":[""]
}'

# 2) base64 of 8 zero bytes - valid b64, invalid BCS for TransactionData
probe "dry_run_zero_bytes" '{
  "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
  "params":["AAAAAAAAAAA="]
}'

# 3) Very large base64 (~5MB of 'A's decoded). Tests whether the server
# imposes a body-size limit *before* attempting BCS decode.
big_b64=$(python3 -c 'import sys;sys.stdout.write("A"*5000000)')
probe "dry_run_5mb_zero" '{
  "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
  "params":["'"$big_b64"'"]
}'

# 4) base64 with unusual chars - should be rejected by base64 decoder cleanly
probe "dry_run_invalid_b64" '{
  "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
  "params":["!!!not_base64!!!"]
}'

# 5) Construct BCS bytes for a TypeInput chain Vector<Vector<...<U8>>>.
# TypeInput discriminants (from iota_types::type_input::TypeInput):
#   0 Bool, 1 U8, ..., 6 Vector(Box<TypeInput>), 7 Struct(...), 8 U16, 9 U32, 10 U256
# We build N bytes of "06" (Vector tag) followed by "01" (U8 tag), then base64.
# This is NOT a full TransactionData; we expect a clean BCS decode error.
# The interesting bit is whether the BCS decoder itself (recursive descent on
# Box<TypeInput>) blows the stack while parsing the prefix.
build_nested_typeinput() {
  local n="$1"
  python3 - "$n" <<'PY'
import base64, sys
n = int(sys.argv[1])
b = bytes([0x06]) * n + bytes([0x01])  # n*Vector tag, then U8
sys.stdout.write(base64.b64encode(b).decode())
PY
}

for n in 100 1000 10000 100000 500000; do
  payload=$(build_nested_typeinput "$n")
  probe "dry_run_nested_typeinput_${n}" '{
    "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
    "params":["'"$payload"'"]
  }'
done

# 6) sui_devInspectTransactionBlock with a similarly-shaped nested TypeInput
# wrapped as TransactionKind. This is best-effort — we expect early error.
ADDR='0x0000000000000000000000000000000000000000000000000000000000000001'
for n in 1000 10000 100000; do
  payload=$(build_nested_typeinput "$n")
  probe "dev_inspect_nested_typeinput_${n}" '{
    "jsonrpc":"2.0","id":1,"method":"iota_devInspectTransactionBlock",
    "params":["'"$ADDR"'","'"$payload"'",null,null,null]
  }'
done
