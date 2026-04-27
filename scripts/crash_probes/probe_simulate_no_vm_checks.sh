#!/usr/bin/env bash
# Probe: SimulateTransactions with `DISABLE_VM_CHECKS` set. This mode is
# explicitly documented as bypassing Move entry-function checks — meaning
# malformed/invalid call args reach the VM. We test whether the VM
# defends itself when the front-end checks are disabled.
#
# We construct minimal valid protobuf payloads using grpcio (no proto
# reflection, just by reading the .proto and encoding manually). The
# encoder is hand-rolled below.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() {
  local name="$1"; shift
  local before
  before="$(log_line_count)"
  local out
  out="$(python3 -c "$1" 2>&1 || true)"
  echo "[sim_no_vm_checks/$name] result: $(printf '%s' "$out" | head -c 250)"
  if ! check_node_alive; then
    echo "[sim_no_vm_checks/$name] CRASHED"
    tail_log
    return 1
  fi
  if log_grew_with_panic "$before"; then
    echo "[sim_no_vm_checks/$name] PANIC LOGGED"
    tail -n $(( $(log_line_count) - before )) "$LOG" \
      | grep -E 'panicked at|stack overflow|fatal' | head -5
    return 2
  fi
  echo "[sim_no_vm_checks/$name] OK"
}

# Helper Python: hand-rolled protobuf encoder.
PYHELPER='
import grpc, sys
def varint(n):
    out=bytearray()
    while True:
        b=n & 0x7f
        n>>=7
        if n: out.append(b|0x80)
        else: out.append(b); break
    return bytes(out)
def tag(field, wire): return varint((field<<3)|wire)
def field_len(field, data):  # length-delimited
    return tag(field,2)+varint(len(data))+data
def field_var(field, n):
    return tag(field,0)+varint(n)
'

# Build BCS for a TransactionData::V1 wrapping a ProgrammableTransaction
# with a Command::MakeMoveVec with a deeply nested TypeInput. With VM
# checks disabled, everything past BCS depth (500) is unreachable, but
# anything below that should reach Move VM.
build_bcs() {
  python3 - "$1" "$2" <<'PY'
import sys, struct
n = int(sys.argv[1])  # TypeInput nesting depth
budget = int(sys.argv[2])
def uleb(n):
    out=bytearray()
    while True:
        b=n&0x7f; n>>=7
        if n: out.append(b|0x80)
        else: out.append(b); break
    return bytes(out)

b  = b"\x00"            # TransactionData::V1
b += b"\x00"            # TransactionKind::ProgrammableTransaction
b += b"\x00"            # inputs: 0
b += b"\x01"            # commands: 1
b += b"\x05"            # Command::MakeMoveVec
b += b"\x01"            # Option::Some
b += b"\x06" * n + b"\x01"  # nested Vector<...> + U8
b += b"\x00"            # empty Vec<Argument>
b += b"\x00" * 32       # sender
b += b"\x00"            # GasData.payment len 0
b += b"\x00" * 32       # GasData.owner
b += (1000).to_bytes(8, "little")   # gas price
b += budget.to_bytes(8, "little")    # gas budget
b += b"\x00"            # TransactionExpiration::None
sys.stdout.buffer.write(b)
PY
}

# Send a SimulateTransactionsRequest with DISABLE_VM_CHECKS for various
# nesting depths.
send() {
  local name="$1"; local n="$2"; local budget="${3:-1000000000}"
  local bcs_path
  bcs_path="$(mktemp /tmp/iota-crash-probes-bcs.XXXXXX)"
  build_bcs "$n" "$budget" > "$bcs_path"

  probe "$name" "$PYHELPER"'
data = open("'"$bcs_path"'", "rb").read()
# BcsData has just `data` at field 1.
bcs_data = field_len(1, data)
# Transaction has `bcs` at field 2 (digest is field 1).
transaction_msg = field_len(2, bcs_data)
# SimulateTransactionItem: transaction=1, tx_checks=2 (repeated enum, DISABLE_VM_CHECKS=0)
item = field_len(1, transaction_msg) + field_var(2, 0)
# SimulateTransactionsRequest: transactions=1
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
    print("ok bytes:", len(out))
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'
  rm -f "$bcs_path"
}

# A few interesting depths.
for n in 0 16 64 256 499; do
  send "n_${n}" "$n"
done
# Just over BCS depth limit — should give clean BCS error.
send "n_500_overflow" 500
# Huge gas budget
send "n_0_huge_budget" 0 18446744073709551615
