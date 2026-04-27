#!/usr/bin/env bash
# Probe: submit transactions via the iota CLI client with edge-case
# arguments, then verify the node is still alive.
#
# This requires a working `iota` binary in target/debug and an active
# wallet pointing at the localnet. The script will:
#   1) ensure a wallet exists for localnet
#   2) request gas from the faucet
#   3) issue several `iota client call` invocations with malformed args
#
# Each call is allowed to fail with a client-side error; we only care
# whether the node remains up.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

REPO_ROOT="$(cd "$HERE"/../.. && pwd)"
IOTA="${IOTA_BIN:-$REPO_ROOT/target/debug/iota}"

if [ ! -x "$IOTA" ]; then
  echo "iota binary not found at $IOTA — skipping CLI probes."
  exit 0
fi

probe() {
  local name="$1"; shift
  echo "[cli/$name] running (args length: $(printf '%s' "$*" | wc -c) bytes)"
  "$IOTA" "$@" >/tmp/iota-crash-probes/cli_$name.out 2>&1 || true
  echo "[cli/$name] exit=$?"
  if check_node_alive; then
    echo "[cli/$name] OK (node alive)"
  else
    echo "[cli/$name] CRASHED"
    tail_log
    return 1
  fi
}

# Ensure config dir exists; iota client commands will fall through with
# a friendly error if not — that's still useful as a probe.
mkdir -p /tmp/iota-crash-probes

probe "client_active_env" client active-env

# Try to call a built-in package with a deeply nested type-arg (the
# CLI parses type-arg strings via parse_iota_type_tag — same parser as
# the JSON-RPC path, just exercised here from the wallet side).
deep_ttype=$(python3 -c '
n=2000
inner="u8"
for _ in range(n): inner=f"vector<{inner}>"
print(inner)
')
probe "client_call_deep_ttype" client call \
  --package 0x2 --module coin --function value \
  --type-args "$deep_ttype"

# Very long identifier strings
long_ident=$(python3 -c 'import sys;sys.stdout.write("a"*100000)')
probe "client_call_huge_module" client call \
  --package 0x2 --module "$long_ident" --function value
