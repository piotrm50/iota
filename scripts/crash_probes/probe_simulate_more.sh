#!/usr/bin/env bash
# Additional probes against SimulateTransactions / DisableVmChecks looking
# for further panic-reachable paths.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() {
  local name="$1"; shift
  local before
  before="$(log_line_count)"
  local out
  out="$(python3 -c "$1" 2>&1 || true)"
  echo "[sim_more/$name] result: $(printf '%s' "$out" | head -c 250)"
  if ! check_node_alive; then
    echo "[sim_more/$name] CRASHED"
    tail_log
    return 1
  fi
  if log_grew_with_panic "$before"; then
    echo "[sim_more/$name] PANIC LOGGED"
    tail -n $(( $(log_line_count) - before )) "$LOG" \
      | grep -E 'panicked at|stack overflow' | head -3
    return 2
  fi
  echo "[sim_more/$name] OK"
}

HELPER='
import grpc
def varint(n):
    out=bytearray()
    while True:
        b=n & 0x7f
        n>>=7
        if n: out.append(b|0x80)
        else: out.append(b); break
    return bytes(out)
def tag(f,w): return varint((f<<3)|w)
def field_len(f,d): return tag(f,2)+varint(len(d))+d
def field_var(f,n): return tag(f,0)+varint(n)
def submit(bcs_payload, disable_vm=True):
    bcs = field_len(1, bcs_payload)
    tx  = field_len(2, bcs)
    item = field_len(1, tx)
    if disable_vm:
        item += field_var(2, 0)  # DISABLE_VM_CHECKS
    req = field_len(1, item)
    ch = grpc.insecure_channel(
        "127.0.0.1:50051",
        options=[("grpc.max_send_message_length", 50*1024*1024)],
    )
    m = ch.unary_unary(
        "/iota.grpc.v1.transaction_execution_service.TransactionExecutionService/SimulateTransactions"
    )
    try:
        out = m(req, timeout=20)
        return ("ok bytes:", len(out))
    except grpc.RpcError as e:
        return ("rpc_error", str(e.code()), e.details()[:200])
    finally:
        ch.close()
'

# Common framework: TransactionData::V1 with one programmable command;
# trailing bytes are sender/gas/expiration.
TAIL='b"\x00"*32 + b"\x00" + b"\x00"*32 + (1000).to_bytes(8,"little") + (1000000000).to_bytes(8,"little") + b"\x00"'

# 1) Gas budget exactly equal to balance (should succeed) — sanity.
probe "budget_at_balance" "$HELPER"'
b = b"\x00\x00\x00\x01\x05\x01\x01\x00" + b"\x00"*32 + b"\x00" + b"\x00"*32
b += (1000).to_bytes(8,"little")
b += (200_000_000_000).to_bytes(8,"little")
b += b"\x00"
print(submit(b))
'

# 2) Gas budget = 0 (an underflow path).
probe "budget_zero" "$HELPER"'
b = b"\x00\x00\x00\x01\x05\x01\x01\x00" + b"\x00"*32 + b"\x00" + b"\x00"*32
b += (1000).to_bytes(8,"little")
b += (0).to_bytes(8,"little")
b += b"\x00"
print(submit(b))
'

# 3) Gas price overflow (price * budget overflows u64).
probe "price_budget_overflow" "$HELPER"'
b = b"\x00\x00\x00\x01\x05\x01\x01\x00" + b"\x00"*32 + b"\x00" + b"\x00"*32
b += (0xFFFFFFFFFFFFFFFF).to_bytes(8,"little")
b += (0xFFFFFFFFFFFFFFFF).to_bytes(8,"little")
b += b"\x00"
print(submit(b))
'

# 4) Multiple commands stuffed in - e.g. lots of MakeMoveVec(None, []).
probe "many_commands" "$HELPER"'
n = 50000
b  = b"\x00\x00\x00"
def uleb(n):
    out=bytearray()
    while True:
        v=n&0x7f
        n>>=7
        if n: out.append(v|0x80)
        else: out.append(v); break
    return bytes(out)
b += uleb(n)
b += (b"\x05\x00\x00") * n
b += b"\x00"*32 + b"\x00" + b"\x00"*32
b += (1000).to_bytes(8,"little")
b += (1000000000).to_bytes(8,"little")
b += b"\x00"
print(submit(b))
'

# 5) Argument referencing a non-existent input/result (out-of-bounds index).
#    With DISABLE_VM_CHECKS, the bounds check might be skipped.
probe "argument_oob" "$HELPER"'
# MakeMoveVec(Some(U64), [Result(65535), Result(65535)])
b  = b"\x00\x00\x00"
b += b"\x01"           # 1 command
b += b"\x05\x01\x02"   # MakeMoveVec, Some, U64
b += b"\x02"           # Vec<Argument> len=2
b += b"\x02\xff\xff"   # Argument::Result(0xFFFF)
b += b"\x02\xff\xff"   # Argument::Result(0xFFFF)
b += b"\x00"*32 + b"\x00" + b"\x00"*32
b += (1000).to_bytes(8,"little")
b += (1000000000).to_bytes(8,"little")
b += b"\x00"
print(submit(b))
'

# 6) Invalid CallArg variant.
probe "invalid_callarg_variant" "$HELPER"'
b  = b"\x00\x00"
b += b"\x01"           # inputs: 1
b += b"\xff"           # CallArg variant 255 (invalid)
b += b"\x00"           # commands: 0
b += b"\x00"*32 + b"\x00" + b"\x00"*32
b += (1000).to_bytes(8,"little")
b += (1000000000).to_bytes(8,"little")
b += b"\x00"
print(submit(b))
'

# 7) ExecuteTransactions (real execution) with an unsigned malformed tx.
#    Even a clearly-broken signature should be rejected without panic.
probe "execute_no_sig" "$HELPER"'
b = b"\x00\x00\x00\x01\x05\x00\x00" + b"\x00"*32 + b"\x00" + b"\x00"*32
b += (1000).to_bytes(8,"little")
b += (1000000000).to_bytes(8,"little")
b += b"\x00"
bcs = field_len(1, b)
tx  = field_len(2, bcs)
# Empty UserSignatures (signatures field 2)
sigs = b""
item = field_len(1, tx) + field_len(2, sigs)
req = field_len(1, item)
ch = grpc.insecure_channel("127.0.0.1:50051")
m = ch.unary_unary("/iota.grpc.v1.transaction_execution_service.TransactionExecutionService/ExecuteTransactions")
try:
    out = m(req, timeout=15)
    print("ok", len(out))
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'
