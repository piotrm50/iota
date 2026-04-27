#!/usr/bin/env bash
# Probe: concurrent / batched requests. Looks for DoS-from-overload
# behaviour rather than a single-message panic.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

probe() { run_probe "concurrent/$1" "$2" || return 1; }

# 1) JSON-RPC batch with hundreds of distinct calls in one POST.
#    The framework should either succeed or return a clean batch error.
build_batch() {
  python3 - "$1" <<'PY'
import json, sys
n = int(sys.argv[1])
batch = [
  {"jsonrpc":"2.0","id":i,"method":"iotax_getLatestIotaSystemState","params":[]}
  for i in range(n)
]
sys.stdout.write(json.dumps(batch))
PY
}

probe "batch_100" "$(build_batch 100)"
probe "batch_1000" "$(build_batch 1000)"
probe "batch_10000" "$(build_batch 10000)"

# 2) 200 parallel curl POSTs of an expensive query.
echo "[concurrent/parallel_200] launching 200 parallel requests"
seq 1 200 | xargs -n1 -P200 -I{} curl -sS --max-time 30 -X POST \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":{},"method":"iota_getCheckpoints","params":[null,50,false]}' \
  http://127.0.0.1:9000 >/dev/null 2>&1
if check_node_alive; then
  echo "[concurrent/parallel_200] OK"
else
  echo "[concurrent/parallel_200] CRASHED"
  tail_log
  exit 1
fi
