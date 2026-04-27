#!/usr/bin/env bash
# Start a clean local network in the background.
#
# Layout:
#   - Fullnode JSON-RPC on http://127.0.0.1:9000
#   - Faucet (HTTP) on http://127.0.0.1:9123
#   - gRPC API on 127.0.0.1:50051
#
# Writes:
#   - log:  $TMPDIR/iota-localnet.log
#   - pid:  $TMPDIR/iota-localnet.pid
#
# This script does NOT block once the network is responding to RPC.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BIN="${IOTA_LOCALNET_BIN:-$REPO_ROOT/target/debug/iota-localnet}"
TMPDIR="${TMPDIR:-/tmp}/iota-crash-probes"
LOG="$TMPDIR/iota-localnet.log"
PID="$TMPDIR/iota-localnet.pid"

mkdir -p "$TMPDIR"
rm -f "$LOG" "$PID"

if [ ! -x "$BIN" ]; then
  echo "iota-localnet binary not found at $BIN" >&2
  echo "Build it first: cargo build --bin iota-localnet" >&2
  exit 2
fi

echo "Starting iota-localnet (log: $LOG)"
nohup "$BIN" start \
  --force-regenesis \
  --with-faucet \
  --with-grpc \
  --fullnode-rpc-port 9000 \
  --epoch-duration-ms 60000 \
  >"$LOG" 2>&1 &
echo $! > "$PID"

# Wait for JSON-RPC to come up (up to ~120s).
for i in $(seq 1 120); do
  if curl -sf -X POST -H 'content-type: application/json' \
       --data '{"jsonrpc":"2.0","id":1,"method":"iotax_getLatestIotaSystemState","params":[]}' \
       http://127.0.0.1:9000 \
     | grep -q '"result"'; then
    echo "Network is up."
    exit 0
  fi
  if ! kill -0 "$(cat "$PID")" 2>/dev/null; then
    echo "iota-localnet died during startup. Tail of log:" >&2
    tail -50 "$LOG" >&2
    exit 1
  fi
  sleep 1
done

echo "Timed out waiting for JSON-RPC. Tail of log:" >&2
tail -50 "$LOG" >&2
exit 1
