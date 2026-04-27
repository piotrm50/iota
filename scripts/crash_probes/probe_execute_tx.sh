#!/usr/bin/env bash
# Probe: iota_executeTransactionBlock with malformed BCS / signatures.
#
# This endpoint takes (tx_bytes: Base64, signatures: [Base64], opts).
# It bcs::from_bytes the tx_bytes into TransactionData and decodes
# signatures via GenericSignature::from_bytes. We probe edge cases on
# both arguments.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "exec/$1" "$2" || return 1; }

# 1) empty everything
probe "empty" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", [], null, null]
}'

# 2) empty tx, single empty sig
probe "empty_tx_one_empty_sig" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", [""], null, null]
}'

# 3) garbage tx, garbage sig
probe "garbage" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["AAAAAAAAAAA=", ["BBBB"], null, null]
}'

# 4) invalid base64 tx
probe "bad_b64_tx" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["!!!", [], null, null]
}'

# 5) Many empty signatures
many_sigs=$(python3 -c 'print(",".join(["\"\""]*1000))')
probe "many_empty_sigs" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", ['"$many_sigs"'], null, null]
}'

# 6) Large signature blob
big_sig=$(python3 -c 'print("A"*100000)')
probe "huge_sig" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", ["'"$big_sig"'"], null, null]
}'

# 7) Sig prefixed with invalid scheme byte (0xFF) followed by random bytes.
# This exercises GenericSignature::from_bytes scheme dispatch.
bad_scheme_sig=$(python3 -c '
import base64, os
# 0xFF flag + random bytes
data = bytes([0xFF]) + os.urandom(64)
print(base64.b64encode(data).decode())
')
probe "unknown_sig_scheme" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", ["'"$bad_scheme_sig"'"], null, null]
}'

# 8) Multisig flag (0x03) but truncated
truncated_multisig=$(python3 -c '
import base64
print(base64.b64encode(bytes([0x03, 0x01])).decode())
')
probe "truncated_multisig" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", ["'"$truncated_multisig"'"], null, null]
}'

# 9) zk-login flag (0x05) but malformed JWT/proof
truncated_zk=$(python3 -c '
import base64
# 0x05 flag + minimal random bytes
print(base64.b64encode(bytes([0x05, 0x00, 0x00, 0x00])).decode())
')
probe "truncated_zklogin" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", ["'"$truncated_zk"'"], null, null]
}'

# 10) passkey flag (0x06) truncated
truncated_passkey=$(python3 -c '
import base64
print(base64.b64encode(bytes([0x06, 0x00])).decode())
')
probe "truncated_passkey" '{
  "jsonrpc":"2.0","id":1,"method":"iota_executeTransactionBlock",
  "params":["", ["'"$truncated_passkey"'"], null, null]
}'
