#!/usr/bin/env bash
# Shared helpers for probe scripts.
#
# Each probe uses these helpers:
#   - check_node_alive: returns 0 if the iota-localnet process is still
#     running and JSON-RPC still responds, 1 otherwise.
#   - tail_log: prints the last lines of the localnet log (used after a
#     suspected crash to surface backtrace info).
#   - rpc: helper that posts a raw JSON body to the fullnode JSON-RPC
#     endpoint and prints the response body.

TMPDIR="${TMPDIR:-/tmp}/iota-crash-probes"
LOG="$TMPDIR/iota-localnet.log"
PID="$TMPDIR/iota-localnet.pid"
RPC_URL="${RPC_URL:-http://127.0.0.1:9000}"

check_node_alive() {
  if [ -f "$PID" ]; then
    if ! kill -0 "$(cat "$PID")" 2>/dev/null; then
      return 1
    fi
  fi
  curl -sf --max-time 5 -X POST -H 'content-type: application/json' \
    --data '{"jsonrpc":"2.0","id":0,"method":"iotax_getLatestIotaSystemState","params":[]}' \
    "$RPC_URL" \
    | grep -q '"result"'
}

# Returns 0 if the localnet log contains a Rust panic/abort message that
# was *not* present before the probe. Caller passes the line count
# captured before the probe ran. Even if the process didn't exit, a
# panic (e.g. in a Tokio worker task) is still a real bug to fix.
log_grew_with_panic() {
  local prev="$1"
  local cur
  cur="$(wc -l < "$LOG" 2>/dev/null || echo 0)"
  if [ "$cur" -le "$prev" ]; then
    return 1
  fi
  tail -n $(( cur - prev )) "$LOG" 2>/dev/null \
    | grep -qE 'panicked at|RUST_BACKTRACE|fatal runtime error|stack overflow|aborting due to'
}

log_line_count() {
  wc -l < "$LOG" 2>/dev/null || echo 0
}

tail_log() {
  echo "--- last 30 lines of $LOG ---"
  tail -30 "$LOG" 2>/dev/null || echo "(log unavailable)"
  echo "---"
}

rpc() {
  # rpc <body-json>  - POSTs the body, prints response body or curl error.
  # Bodies are routed through a temp file so neither bash nor curl hits
  # ARG_MAX for very large payloads.
  local body_file
  body_file="$(mktemp /tmp/iota-crash-probes-body.XXXXXX)"
  printf '%s' "$1" > "$body_file"
  curl -sS --max-time 60 -X POST -H 'content-type: application/json' \
    --data-binary "@$body_file" "$RPC_URL"
  local rc=$?
  rm -f "$body_file"
  return $rc
}

run_probe() {
  # run_probe <name> <body>
  # Sends the JSON body, then checks whether the node is still alive AND
  # whether a Rust panic/abort was newly logged. A panic in a Tokio
  # worker task may not exit the process but is still a real bug.
  local name="$1"; shift
  local body="$1"; shift
  local resp
  local before_lines
  before_lines="$(log_line_count)"
  resp="$(rpc "$body" 2>&1 || true)"
  echo "[$name] response: $(printf '%s' "$resp" | head -c 200)"

  local rc=0
  if ! check_node_alive; then
    echo "[$name] CRASHED (node no longer responsive)"
    tail_log
    rc=1
  fi
  if log_grew_with_panic "$before_lines"; then
    echo "[$name] PANIC LOGGED (process may still be up; this is still a bug)"
    tail -n $(( $(log_line_count) - before_lines )) "$LOG" \
      | grep -E 'panicked at|RUST_BACKTRACE|fatal runtime error|stack overflow|aborting due to' \
      | head -5
    rc=2
  fi
  if [ $rc -eq 0 ]; then
    echo "[$name] OK"
  fi
  return $rc
}
