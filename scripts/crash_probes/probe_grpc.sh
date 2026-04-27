#!/usr/bin/env bash
# Probe: gRPC API surface (port 50051). We use raw HTTP/2 + protobuf to
# send malformed messages without needing the .proto definitions. We
# explicitly target endpoints that wrap untrusted bytes in BCS.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() {
  # name, py-script
  local name="$1"; shift
  local before
  before="$(log_line_count)"
  local out
  out="$(python3 -c "$1" 2>&1 || true)"
  echo "[grpc/$name] result: $(printf '%s' "$out" | head -c 250)"
  if ! check_node_alive; then
    echo "[grpc/$name] CRASHED"
    tail_log
    return 1
  fi
  if log_grew_with_panic "$before"; then
    echo "[grpc/$name] PANIC LOGGED"
    tail -n $(( $(log_line_count) - before )) "$LOG" \
      | grep -E 'panicked at|stack overflow|fatal runtime error' | head -5
    return 2
  fi
  echo "[grpc/$name] OK"
}

# Hit the unary endpoint /iota.grpc.v1.ledger_service.LedgerService/GetServiceInfo with
# a sequence of malformed protobuf payloads. We do not rely on the actual
# .proto files — gRPC's protobuf decoder must reject malformed wire
# bytes cleanly.
probe "ledger_get_service_info_garbage" '
import grpc
ch = grpc.insecure_channel("127.0.0.1:50051")
# Fire the actual GetServiceInfo with garbage protobuf body.
m = ch.unary_unary("/iota.grpc.v1.ledger_service.LedgerService/GetServiceInfo")
try:
    out = m(b"\x00\x01\x02", timeout=10)
    print("ok", len(out))
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'

probe "transaction_exec_empty" '
import grpc
ch = grpc.insecure_channel("127.0.0.1:50051")
m = ch.unary_unary("/iota.grpc.v1.transaction_execution_service.TransactionExecutionService/ExecuteTransactions")
try:
    m(b"", timeout=10)
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'

probe "transaction_exec_random" '
import grpc, os
ch = grpc.insecure_channel("127.0.0.1:50051")
m = ch.unary_unary("/iota.grpc.v1.transaction_execution_service.TransactionExecutionService/ExecuteTransactions")
try:
    m(os.urandom(2048), timeout=10)
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'

probe "transaction_exec_huge" '
import grpc
ch = grpc.insecure_channel(
    "127.0.0.1:50051",
    options=[("grpc.max_send_message_length", 50*1024*1024)],
)
m = ch.unary_unary("/iota.grpc.v1.transaction_execution_service.TransactionExecutionService/ExecuteTransactions")
try:
    # 5MB of zero-byte protobuf (mostly invalid wire format)
    m(b"\x00"*5_000_000, timeout=20)
except grpc.RpcError as e:
    print("rpc_error", e.code(), e.details()[:200])
ch.close()
'

# Repeated/concurrent connections - look for a connection-handling crash.
probe "many_connections" '
import grpc, threading
errors = []
def hit():
    ch = grpc.insecure_channel("127.0.0.1:50051")
    try:
        m = ch.unary_unary("/iota.grpc.v1.ledger_service.LedgerService/GetServiceInfo")
        m(b"", timeout=5)
    except grpc.RpcError as e:
        errors.append(str(e.code()))
    finally:
        ch.close()
ts = [threading.Thread(target=hit) for _ in range(200)]
for t in ts: t.start()
for t in ts: t.join()
print("errors:", len(errors))
'
