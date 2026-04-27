#!/usr/bin/env bash
# Probe: pagination edge cases on read endpoints.
#
# Targets: limit=0, negative-looking limits, very large limits, malformed
# cursors, zero-length filter sets. Most of these are read paths that touch
# `cap_page_limit`, `.last()`, `.pop()`, and slice indexing — historically
# the source of off-by-one panics in JSON-RPC handlers.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

# An object ID we expect not to exist. Used as filter input.
NONEXISTENT_OBJ='0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef'
NONEXISTENT_ADDR='0x0000000000000000000000000000000000000000000000000000000000000001'
NONEXISTENT_TX_DIGEST='11111111111111111111111111111111'  # base58 placeholder

probe() {
  local name="$1"; shift
  local body="$1"; shift
  run_probe "pagination/$name" "$body" || return 1
}

# Note: all bodies are valid JSON-RPC envelopes. The interesting parts are
# the params: limit==0, huge limit, etc.
probe "owned_objects_limit_0" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getOwnedObjects",
  "params":["'"$NONEXISTENT_ADDR"'", null, null, 0]
}'

probe "owned_objects_limit_negative_string" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getOwnedObjects",
  "params":["'"$NONEXISTENT_ADDR"'", null, null, -1]
}'

probe "owned_objects_limit_max_u32" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getOwnedObjects",
  "params":["'"$NONEXISTENT_ADDR"'", null, null, 4294967295]
}'

probe "query_events_limit_0" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_queryEvents",
  "params":[{"All":[]}, null, 0, false]
}'

probe "query_tx_blocks_limit_0" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_queryTransactionBlocks",
  "params":[{"filter":{"FromAddress":"'"$NONEXISTENT_ADDR"'"}}, null, 0, false]
}'

probe "dynamic_fields_limit_0" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getDynamicFields",
  "params":["'"$NONEXISTENT_OBJ"'", null, 0]
}'

probe "checkpoints_limit_0" '{
  "jsonrpc":"2.0","id":1,"method":"iota_getCheckpoints",
  "params":[null, 0, false]
}'

probe "owned_objects_bad_cursor" '{
  "jsonrpc":"2.0","id":1,"method":"iotax_getOwnedObjects",
  "params":["'"$NONEXISTENT_ADDR"'", null, "not_a_real_cursor", 10]
}'
