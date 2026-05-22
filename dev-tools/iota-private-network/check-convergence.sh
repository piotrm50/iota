#!/usr/bin/env bash
# Cross-validator scoreboard-convergence check.
#
# Each validator emits `validator score snapshot` log lines tagged with
# commit_index and score_vector_hash. The hash only fires when state changes,
# so each line corresponds to a distinct (commit_index, scores) point. This
# script matches lines across validators by commit_index and verifies that
# all observers compute the same hash for the same commit.
#
# Usage:
#   ./check-convergence.sh [validator-name...]   # default: validator-1..4

set -euo pipefail

validators=("${@}")
if [[ ${#validators[@]} -eq 0 ]]; then
  validators=(validator-1 validator-2 validator-3 validator-4)
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

# For each validator, grab all `validator score snapshot` lines and emit
# `<commit_index> <hash>` rows to a per-validator file.
for v in "${validators[@]}"; do
  docker logs "$v" 2>&1 \
    | grep "validator score snapshot" \
    | sed -nE 's/.*commit_index=(-?[0-9]+).*score_vector_hash="([0-9a-f]+)".*/\1 \2/p' \
    > "$tmpdir/$v"
  if ! [[ -s "$tmpdir/$v" ]]; then
    echo "FAIL: $v has no parsable score-snapshot lines"
    exit 1
  fi
done

# Build the set of commit_indexes seen on every validator.
common=$(cat "$tmpdir/${validators[0]}" | awk '{print $1}' | sort -u)
for v in "${validators[@]:1}"; do
  this=$(cat "$tmpdir/$v" | awk '{print $1}' | sort -u)
  common=$(comm -12 <(printf '%s\n' "$common") <(printf '%s\n' "$this"))
done

if [[ -z "$common" ]]; then
  echo "FAIL: no commit_index is present in all validators' logs"
  exit 1
fi

matched=0
mismatched=0
mismatch_detail=""

while IFS= read -r ci; do
  declare -A hashes_seen=()
  per_validator_hash=""
  for v in "${validators[@]}"; do
    # If the same commit_index appears multiple times (shouldn't with the
    # change-only-dedup), take the first.
    h=$(awk -v ci="$ci" '$1==ci {print $2; exit}' "$tmpdir/$v")
    hashes_seen["$h"]=1
    per_validator_hash+="$v=$h "
  done
  if [[ ${#hashes_seen[@]} -eq 1 ]]; then
    matched=$((matched + 1))
  else
    mismatched=$((mismatched + 1))
    mismatch_detail+="  commit_index=$ci: $per_validator_hash"$'\n'
  fi
  unset hashes_seen
done <<< "$common"

echo "Compared $((matched + mismatched)) shared commit_indexes."
echo "  matched   : $matched"
echo "  mismatched: $mismatched"

if [[ $mismatched -gt 0 ]]; then
  echo
  echo "DIVERGENCES:"
  printf '%s' "$mismatch_detail"
  exit 1
fi

# Show the latest converged snapshot for context.
latest_ci=$(printf '%s\n' "$common" | sort -n | tail -1)
echo
echo "Latest converged commit_index=$latest_ci:"
for v in "${validators[@]}"; do
  line=$(docker logs "$v" 2>&1 | grep "validator score snapshot" \
    | grep "commit_index=$latest_ci " | tail -1)
  scores=$(printf '%s' "$line" | sed -nE 's/.*scores=(\[.*\]).*/\1/p')
  printf '  %s: %s\n' "$v" "$scores"
done

echo
echo "PASS — all observers agree on score_vector_hash for every shared commit_index."
