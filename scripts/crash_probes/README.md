# Crash probes

Reproducible scripts that send malformed RPC / gRPC requests to a local
network and check whether the node process dies, panics in a Tokio task,
or handles the request cleanly. Confidential: do not publish; the
reproducer for the only confirmed panic is in `probe_simulate_panic_minimal.sh`.

## Usage

```bash
# Build the binaries (one-time):
cargo build --bin iota --bin iota-localnet

# Start a fresh localnet:
./start_localnet.sh

# Run all probes — each one decides whether it triggered a crash or
# a logged panic. The runner restarts the network between failures so
# every probe gets a fair shot.
./run_all.sh

# Tear down:
./stop_localnet.sh
```

## What each probe targets

| script | RPC surface | what we're probing |
|---|---|---|
| `probe_pagination.sh` | JSON-RPC | limit=0 / huge / cursors / negative — any `pop().unwrap()` reachable from pagination |
| `probe_bcs_payloads.sh` | JSON-RPC `dryRun` / `devInspect` | malformed Base64, oversize body, BCS prefixes that don't match the outer wrapper |
| `probe_nested_json.sh` | JSON-RPC `unsafe_moveCall` | deeply nested / oversized JSON arguments — `IotaJsonValue::new` recursion |
| `probe_typetag_strings.sh` | JSON-RPC | deeply nested `vector<vector<...>>`, oversized identifiers, malformed object IDs |
| `probe_get_stakes.sh` | JSON-RPC governance | `getStakesByIds` with system / nonexistent / duplicate IDs (was suspected to hit `version.one_before().unwrap()`) |
| `probe_execute_tx.sh` | JSON-RPC `executeTransactionBlock` | malformed signatures, unknown sig schemes, truncated multisig/zk/passkey |
| `probe_concurrent.sh` | JSON-RPC | batches and 200-way parallel reads |
| `probe_publish_bytecode.sh` | JSON-RPC `dryRun` | `Command::Publish` with empty / garbage / magic-only / oversize Move bytecode |
| `probe_movecall_args.sh` | JSON-RPC `unsafe_moveCall` | u64 boundary values, deeply nested type args, mismatched arg counts |
| `probe_deep_typeinput_bcs.sh` | JSON-RPC `dryRun` / `devInspect` | hand-crafted BCS with a deeply nested `Box<TypeInput>` chain to stress the BCS deserializer |
| `probe_grpc.sh` | gRPC | empty / garbage / oversized protobuf, 200 concurrent connections |
| `probe_simulate_no_vm_checks.sh` | gRPC `SimulateTransactions` | `DISABLE_VM_CHECKS` mode with various TypeInput depths and budgets |
| `probe_simulate_panic_minimal.sh` | gRPC `SimulateTransactions` | **CONFIRMED panic** — minimal reproducer |
| `probe_simulate_more.sh` | gRPC `SimulateTransactions` | argument OOB, oversized command count, invalid CallArg variants under `DISABLE_VM_CHECKS` |
| `probe_cli_tx.sh` | iota CLI | wallet-side / server-side parsing of edge-case type tags, very long identifiers |

## Findings

### Confirmed — `invariant_violation!` reachable from public gRPC

`probe_simulate_panic_minimal.sh` reproduces a panic in
`iota-execution/latest/iota-adapter/src/programmable_transactions/context.rs:197`
("Transaction input checker should check that there is enough gas").

Trigger:
1. Send a gRPC `SimulateTransactions` request to `:50051`.
2. Set `tx_checks = [DISABLE_VM_CHECKS]` on the item.
3. Provide a `TransactionData::V1` BCS body with `gas_budget = u64::MAX`
   and `gas_price >= reference_gas_price`.

What happens:
* **Debug build (the localnet built here):** panic with full backtrace
  is logged on the node. The Tokio task that handles the gRPC request
  unwinds, the connection is reset (`RST_STREAM` / `CANCELLED`). The
  process *does not* exit — task isolation absorbs the panic.
* **Release build (production):** the `invariant_violation!` macro
  downgrades to `return Err(...)` outside `cfg!(debug_assertions)` (see
  `crates/iota-types/src/error.rs` line 59), so the request gets a
  clean `ExecutionError`. No log spam, no RST_STREAM.

So this does not exit a release node, but it is still a real bug:
* `DISABLE_VM_CHECKS` is a public-facing simulation flag whose
  documentation says it bypasses *Move entry-function checks*. It also
  ends up bypassing the gas-budget-vs-coin-balance arithmetic
  precondition assumed by `ExecutionContext::new`.
* The "invariant_violation" message is misleading — there is no broken
  invariant, just an attacker-controlled input the code wasn't expected
  to encounter.
* On debug / CI nodes the panic shows up in logs and resets the
  connection, which is observable DoS-shaped behavior even though the
  process keeps running.

Fix direction (when authorized): in `programmable_transactions::context::checked::ExecutionContext::new`,
when `coin.balance.value().checked_sub(max_gas_in_balance)` underflows
under `DISABLE_VM_CHECKS`, return a regular `ExecutionError` (e.g.
`InsufficientGas`) instead of `invariant_violation!`. Alternately, keep
the gas-budget check active even when other VM checks are disabled.

### No reproducible process-level crash from any other probe

All other probes returned clean error responses with the node still
alive and **no** panic in logs. Specifically:

* JSON-RPC body size limit (~5 MB) is enforced before BCS / JSON parsing.
* `serde_json` recursion limit (~128) catches deeply nested arguments
  before any custom validator runs.
* `bcs::MAX_CONTAINER_DEPTH = 500` rejects deeply nested `TypeInput`
  before the type-arg post-validator runs.
* The Move type-tag string parser already enforces `MAX_TYPE_DEPTH = 128`
  and `MAX_TYPE_NODE_COUNT = 256`.
* gRPC default message-size limit (4 MB) is enforced before protobuf
  decoding.
* The `cap_page_limit` interactions are safe — every `.last().map_or(...)`
  / `.last().map(...).unwrap_or(...)` pattern in the JSON-RPC indexer
  handlers handled the empty-result case.
* Move bytecode verifier rejects empty / garbage / truncated / oversized
  modules with `Move Bytecode Verification Error`.
* 200 parallel JSON-RPC requests and 200 concurrent gRPC connections
  did not destabilize the node.

## Adding new probes

A probe is a shell script in this directory whose name starts with
`probe_`. Source `lib.sh` for the `rpc`, `check_node_alive`,
`log_grew_with_panic`, and `run_probe` helpers. Each probe should be
self-contained and exit non-zero if it observed a crash or a logged
panic. `run_all.sh` will pick it up automatically.
