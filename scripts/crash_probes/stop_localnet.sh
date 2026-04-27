#!/usr/bin/env bash
# Stop the local network started by start_localnet.sh.
set -euo pipefail
TMPDIR="${TMPDIR:-/tmp}/iota-crash-probes"
PID="$TMPDIR/iota-localnet.pid"
if [ -f "$PID" ]; then
  kill "$(cat "$PID")" 2>/dev/null || true
  sleep 1
  kill -9 "$(cat "$PID")" 2>/dev/null || true
  rm -f "$PID"
fi
pkill -f 'target/debug/iota-localnet' 2>/dev/null || true
echo "stopped"
