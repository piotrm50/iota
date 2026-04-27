#!/usr/bin/env bash
# Run every probe in this directory against a running localnet, restarting
# the network whenever a probe crashes the node so we still discover all
# reproducible crashes (not just the first one).
#
# Usage:
#   ./run_all.sh
#
# This script handles its own start/stop. It exits 0 if no crashes were
# observed, 1 otherwise. All confirmed crashes are listed at the end.
set -u
HERE="$(dirname "$0")"
. "$HERE/lib.sh"

start_net() {
  bash "$HERE/stop_localnet.sh" >/dev/null 2>&1 || true
  bash "$HERE/start_localnet.sh"
}

# crashed array — names of probes that demonstrably crashed the node.
crashed=()

# Best-effort startup
start_net

for probe in "$HERE"/probe_*.sh; do
  name="$(basename "$probe")"
  echo
  echo "=========================================================="
  echo "running: $name"
  echo "=========================================================="

  # Make sure the network is alive before each probe; if it's not, we
  # restart so this probe gets a fair shot.
  if ! check_node_alive; then
    echo "node is down before $name — restarting"
    start_net
  fi

  bash "$probe"
  probe_rc=$?

  # rc semantics from run_probe:
  #   0 = clean (node alive, no new panic)
  #   1 = node went down (process exit)
  #   2 = node still up but panic was logged
  if ! check_node_alive; then
    echo "*** $name crashed the node ***"
    crashed+=("$name (process down)")
    tail_log
    start_net
  elif [ "$probe_rc" -ne 0 ]; then
    echo "*** $name logged a panic without crashing the process ***"
    crashed+=("$name (panic logged)")
    # Don't restart — the process is still serving.
  fi
done

bash "$HERE/stop_localnet.sh" >/dev/null 2>&1 || true

echo
echo "=========================================================="
if [ ${#crashed[@]} -eq 0 ]; then
  echo "ALL PROBES SURVIVED — no node crashes reproduced."
  exit 0
else
  echo "REPRODUCED CRASHES (${#crashed[@]}):"
  for c in "${crashed[@]}"; do echo "  - $c"; done
  exit 1
fi
