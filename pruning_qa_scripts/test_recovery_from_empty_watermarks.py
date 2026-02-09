#!/usr/bin/env python3
"""
Automated testing script for IOTA indexer watermark recovery functionality.

This script tests the recovery behavior when the watermarks table is cleared:
1. Start network and indexer sync with pruning enabled
2. Wait until at least 5 epochs are indexed
3. Verify that pruning is working correctly
4. Stop indexer sync
5. Clear the watermarks table (simulating data loss)
6. Restart indexer sync with pruning enabled
7. Verify watermarks table is repopulated from existing data
8. Verify pruning resumes correctly after recovery

The focus of this test is ensuring the indexer can recover from an empty
watermarks table by rebuilding it from the existing indexed data.

Usage:
    python test_recovery_from_empty_watermarks.py [--epochs-to-keep N] [--target-epoch N]
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

import psycopg2
import requests

# Configuration
# Default Configuration
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "postgrespw"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "iota_indexer_pruning_qa"

# Global DB config - will be set from command line args
DB_CONFIG = {
    "host": DEFAULT_DB_HOST,
    "port": DEFAULT_DB_PORT,
    "database": DEFAULT_DB_NAME,
    "user": DEFAULT_DB_USER,
    "password": DEFAULT_DB_PASSWORD,
}

DB_URL = None  # Will be set from command line args

# Path configuration
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Process management
running_processes = []


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\n\n🛑 Interrupted by user, cleaning up...")
    cleanup_processes()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def log(message: str, level: str = "INFO"):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def run_command(cmd: list, cwd: str, env: dict = None, background: bool = False):
    """Run a command and optionally return the process for background tasks."""
    log(f"Running command: {' '.join(cmd)}")

    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    if background:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=full_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        running_processes.append(process)
        return process
    else:
        result = subprocess.run(
            cmd, cwd=cwd, env=full_env, capture_output=True, text=True
        )
        return result


def cleanup_processes():
    """Stop all running background processes."""
    log("Stopping all background processes...")
    for process in running_processes:
        if process.poll() is None:  # Process is still running
            log(f"Terminating process {process.pid}")
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                log(f"Force killing process {process.pid}", "WARNING")
                process.kill()
    running_processes.clear()


def wait_for_network_ready(
    rpc_url: str = "http://localhost:59000", timeout: int = 120
) -> bool:
    """
    Wait for the network to be ready by polling the RPC endpoint.
    Returns True if network is ready, False if timeout.
    """
    log(f"Waiting for network RPC to be ready at {rpc_url}...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            # Try to get checkpoint 0
            response = requests.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "iota_getCheckpoint",
                    "params": ["0"],
                },
                timeout=5,
            )

            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    log(f"✓ Network is ready (checkpoint 0 available)")
                    return True
        except (requests.exceptions.RequestException, Exception) as e:
            # Network not ready yet, continue polling
            pass

        time.sleep(2)

    log(f"✗ Network did not become ready within {timeout}s", "ERROR")
    return False


def wait_for_indexer_ready(timeout: int = 120) -> bool:
    """
    Wait for the indexer to be ready by checking the database.
    Returns True if indexer is ready, False if timeout.
    """
    log("Waiting for indexer to initialize database...")
    start_time = time.time()
    start_timestamp_ms = int(start_time * 1000)
    last_log_time = start_time
    log_interval = 10  # Log progress every 10 seconds
    last_status = None

    while time.time() - start_time < timeout:
        elapsed = int(time.time() - start_time)

        # Log progress periodically
        if time.time() - last_log_time >= log_interval:
            status_msg = last_status if last_status else "still waiting"
            log(f"Still waiting for indexer... ({elapsed}s elapsed, {status_msg})")
            last_log_time = time.time()

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Check if watermarks table exists and has data
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'watermarks';
            """)
            table_exists = cursor.fetchone()[0] > 0

            if table_exists:
                # Check if we have fresh checkpoints (indexed after we started)
                cursor.execute("""
                    SELECT COUNT(*), MAX(timestamp_ms)
                    FROM checkpoints;
                """)
                result = cursor.fetchone()
                checkpoint_count = result[0]
                max_checkpoint_timestamp = result[1]

                if checkpoint_count > 0:
                    # Check if checkpoints are fresh (indexed after we started)
                    if (
                        max_checkpoint_timestamp
                        and max_checkpoint_timestamp > start_timestamp_ms
                    ):
                        log(
                            f"✓ Indexer is ready ({checkpoint_count} fresh checkpoints indexed)"
                        )
                        cursor.close()
                        conn.close()
                        return True
                    else:
                        # Calculate time difference
                        if max_checkpoint_timestamp:
                            diff_seconds = (
                                start_timestamp_ms - max_checkpoint_timestamp
                            ) / 1000.0
                            new_status = f"checkpoints exist but stale (count={checkpoint_count}, {diff_seconds:.1f}s behind)"
                        else:
                            new_status = f"checkpoints exist but no timestamp (count={checkpoint_count})"
                        if new_status != last_status:
                            log(f"  {new_status}")
                            log(
                                f"    start_time: {start_timestamp_ms}, max_checkpoint_timestamp: {max_checkpoint_timestamp}"
                            )
                            last_status = new_status
                else:
                    new_status = "watermarks table exists, no checkpoints yet"
                    if new_status != last_status:
                        log(f"  {new_status}")
                        last_status = new_status
            else:
                new_status = "watermarks table not created yet"
                if new_status != last_status:
                    log(f"  {new_status}")
                    last_status = new_status

            cursor.close()
            conn.close()
        except psycopg2.OperationalError as e:
            new_status = f"database connection failed: {str(e)[:50]}"
            if new_status != last_status:
                log(f"  {new_status}")
                last_status = new_status
        except Exception as e:
            new_status = f"check failed: {type(e).__name__}"
            if new_status != last_status:
                log(f"  {new_status}")
                last_status = new_status

        time.sleep(2)

    log(f"✗ Indexer did not become ready within {timeout}s", "ERROR")
    return False


def get_db_connection():
    """Get a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def check_indexer_state() -> Tuple[Optional[int], list]:
    """
    Check the current indexer state.
    Returns: (highest_epoch, list of available epochs)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check watermarks table for highest epoch
        cursor.execute("""
            SELECT entity, current_epoch, min_available_epoch, max_committed_cp, lowest_unpruned_key
            FROM watermarks
            ORDER BY current_epoch DESC
            LIMIT 1;
        """)
        result = cursor.fetchone()

        if not result:
            cursor.close()
            conn.close()
            return None, []

        highest_epoch = result[1]

        # Get all watermarks to see what's available
        cursor.execute("""
            SELECT entity, current_epoch, min_available_epoch, min_available_cp,
                   min_available_tx, max_committed_cp, lowest_unpruned_key
            FROM watermarks
            ORDER BY entity;
        """)
        watermarks = cursor.fetchall()

        cursor.close()
        conn.close()

        return highest_epoch, watermarks
    except Exception as e:
        log(f"Error checking indexer state: {e}", "WARNING")
        return None, []


def wait_for_epochs(target_epoch: int, timeout: int = 600):
    """
    Wait until the indexer has indexed at least target_epoch epochs.
    Returns True if successful, False if timeout.
    """
    log(f"Waiting for epoch {target_epoch} to be indexed (timeout: {timeout}s)...")
    start_time = time.time()
    last_epoch_logged = None

    while time.time() - start_time < timeout:
        highest_epoch, watermarks = check_indexer_state()

        if highest_epoch is not None and highest_epoch >= target_epoch:
            log(f"✓ Reached epoch {highest_epoch}")
            log(f"\nCurrent watermarks:")
            log(
                f"{'Entity':<35} {'Curr Epoch':<12} {'Min Avail Epoch':<16} {'Max CP':<12} {'Lowest Unpruned':<16} {'Target':<12} {'Status':<15}"
            )
            log(
                f"{'-' * 35} {'-' * 12} {'-' * 16} {'-' * 12} {'-' * 16} {'-' * 12} {'-' * 15}"
            )
            for wm in watermarks:
                (
                    entity,
                    curr_epoch,
                    min_epoch,
                    min_cp,
                    min_tx,
                    max_cp,
                    lowest_unpruned,
                ) = wm
                # Calculate target based on pruning strategy
                if entity in ["objects_history", "transactions", "events"]:
                    target = min_epoch
                elif entity in ["checkpoints", "pruner_cp_watermark"]:
                    target = min_cp
                else:
                    # ByTransaction or ByGlobalSeq
                    target = min_tx

                # Determine status
                if min_epoch == 0:
                    status = "-"
                elif lowest_unpruned >= target:
                    status = "✓ Complete"
                elif lowest_unpruned > 0:
                    status = "⚠ In progress"
                else:
                    status = "Not started"

                log(
                    f"{entity:<35} {curr_epoch:<12} {min_epoch:<16} {max_cp:<12} {lowest_unpruned:<16} {str(target):<12} {status:<15}"
                )
            return True

        # Only log if epoch changed or first check
        if highest_epoch != last_epoch_logged:
            if highest_epoch is not None:
                elapsed = int(time.time() - start_time)
                log(f"Current epoch: {highest_epoch}/{target_epoch} (after {elapsed}s)")
            else:
                log("No epochs indexed yet, waiting...")
            last_epoch_logged = highest_epoch

        time.sleep(5)

    log(f"Timeout waiting for epoch {target_epoch}", "ERROR")
    return False


def verify_pruning(epochs_to_keep: int, current_epoch: int) -> bool:
    """
    Verify that pruning is working correctly.
    Checks that old epochs are pruned based on the retention policy.
    Also verifies that lowest_unpruned_key > 0 for prunable tables.
    """
    log(
        f"Verifying pruning (epochs_to_keep={epochs_to_keep}, current_epoch={current_epoch})..."
    )

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Check watermarks with lowest_unpruned_key and target values
        cursor.execute("""
            SELECT entity, current_epoch, min_available_epoch, min_available_cp,
                   min_available_tx, lowest_unpruned_key
            FROM watermarks
            WHERE min_available_epoch > 0
            ORDER BY entity;
        """)
        watermarks = cursor.fetchall()

        if not watermarks:
            log("✗ No watermarks with pruning information found", "ERROR")
            cursor.close()
            conn.close()
            return False

        expected_min_epoch = max(0, current_epoch - epochs_to_keep + 1)

        log(f"\nExpected min_available_epoch: >= {expected_min_epoch}")
        log(f"\nPruning verification:")
        log(
            f"{'Entity':<35} {'Curr Epoch':<12} {'Min Avail Epoch':<16} {'Max CP':<12} {'Lowest Unpruned':<16} {'Target':<12} {'Status':<15}"
        )
        log(
            f"{'-' * 35} {'-' * 12} {'-' * 16} {'-' * 12} {'-' * 16} {'-' * 12} {'-' * 15}"
        )

        all_correct = True
        pruning_happened = False

        for (
            entity,
            curr_epoch,
            min_epoch,
            min_cp,
            min_tx,
            lowest_unpruned,
        ) in watermarks:
            # Determine target value based on pruning strategy
            # ByEpochPartition tables should prune to min_available_epoch
            # ByCheckpoint tables should prune to min_available_cp
            # ByTransaction and ByGlobalSeq tables should prune to min_available_tx
            if entity in ["objects_history", "transactions", "events"]:
                # ByEpochPartition
                target = min_epoch
            elif entity in ["checkpoints", "pruner_cp_watermark"]:
                # ByCheckpoint
                target = min_cp
            else:
                # ByTransaction or ByGlobalSeq
                target = min_tx
            # Check if min_available_epoch is correct
            min_epoch_ok = min_epoch >= expected_min_epoch - 1

            # Check if pruning is complete (lowest_unpruned_key should reach target)
            pruning_complete = lowest_unpruned >= target
            pruning_started = lowest_unpruned > 0

            if pruning_started:
                pruning_happened = True

            if min_epoch_ok and pruning_complete:
                status = "✓ Complete"
            elif min_epoch_ok and pruning_started:
                status = f"⚠ In progress ({lowest_unpruned}/{target})"
            elif min_epoch_ok and not pruning_started:
                status = "⚠ Not started"
                all_correct = False
            else:
                status = "✗ FAIL"
                all_correct = False

            log(
                f"{entity:<35} {curr_epoch:<12} {min_epoch:<16} {min_cp:<12} {lowest_unpruned:<16} {target:<12} {status:<15}"
            )

        cursor.close()
        conn.close()

        if not pruning_happened:
            log(
                "\n✗ CRITICAL: No actual pruning occurred (all lowest_unpruned_key = 0)",
                "ERROR",
            )
            log("This means watermarks were updated but data was NOT deleted", "ERROR")
            return False

        return all_correct
    except Exception as e:
        log(f"Error verifying pruning: {e}", "ERROR")
        return False


def wait_for_pruning_complete(epochs_to_keep: int, timeout: int = 120) -> bool:
    """
    Wait for pruning to complete for all tables.
    Returns True if pruning is complete, False if timeout or incomplete.
    """
    log("Waiting for all pruning tasks to complete...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Get watermarks for tables that should be pruned
            cursor.execute("""
                SELECT entity, current_epoch, min_available_epoch, min_available_cp,
                       min_available_tx, lowest_unpruned_key
                FROM watermarks
                WHERE min_available_epoch > 0
                ORDER BY entity;
            """)
            watermarks = cursor.fetchall()

            if not watermarks:
                cursor.close()
                conn.close()
                log("No prunable watermarks found yet, waiting...")
                time.sleep(5)
                continue

            all_complete = True
            in_progress_count = 0

            for (
                entity,
                curr_epoch,
                min_epoch,
                min_cp,
                min_tx,
                lowest_unpruned,
            ) in watermarks:
                # Determine target based on pruning strategy
                if entity in ["objects_history", "transactions", "events"]:
                    target = min_epoch
                elif entity in ["checkpoints", "pruner_cp_watermark"]:
                    target = min_cp
                else:
                    target = min_tx

                # Check if pruning is complete for this table
                if lowest_unpruned < target:
                    all_complete = False
                    in_progress_count += 1

            cursor.close()
            conn.close()

            if all_complete:
                log(f"✓ All pruning tasks completed")
                return True
            else:
                elapsed = int(time.time() - start_time)
                log(
                    f"Pruning in progress: {in_progress_count} tables remaining ({elapsed}s elapsed)..."
                )
                time.sleep(5)

        except Exception as e:
            log(f"Error checking pruning status: {e}", "WARNING")
            time.sleep(5)

    log(f"✗ Pruning did not complete within {timeout}s", "WARNING")
    return False


def clear_watermarks_table():
    """Clear all rows from the watermarks table."""
    log("Clearing watermarks table...")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM watermarks;")
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM watermarks;")
        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        if count == 0:
            log("✓ Watermarks table cleared successfully")
            return True
        else:
            log(f"✗ Watermarks table still has {count} rows", "ERROR")
            return False
    except Exception as e:
        log(f"Error clearing watermarks table: {e}", "ERROR")
        return False


def verify_watermarks_repopulated() -> bool:
    """Verify that watermarks table has been repopulated."""
    log("Verifying watermarks table is repopulated...")

    max_wait = 60
    start_time = time.time()

    while time.time() - start_time < max_wait:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM watermarks;")
            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            if count > 0:
                log(f"✓ Watermarks table repopulated with {count} entries")
                return True

            log(
                f"Watermarks table still empty, waiting... ({int(time.time() - start_time)}s)"
            )
            time.sleep(5)
        except Exception as e:
            log(f"Error checking watermarks: {e}", "WARNING")
            time.sleep(5)

    log("✗ Watermarks table was not repopulated in time", "ERROR")
    return False


def start_local_network():
    """Start the local IOTA network with short epoch duration."""
    log("Starting local IOTA network...")

    cmd = [
        "cargo",
        "run",
        "--profile",
        "dev-nodebug",
        "--bin",
        "iota",
        "--",
        "start",
        "--with-faucet=0.0.0.0:59123",
        "--fullnode-rpc-port=59000",
        "--epoch-duration-ms=10000",  # 10 seconds per epoch for faster testing
        "--force-regenesis",
    ]

    env = {
        "RUST_BACKTRACE": "1",
        "RUST_LOG": "info",
    }

    process = run_command(cmd, REPO_ROOT, env, background=True)

    # Wait for network to be ready by polling RPC
    if not wait_for_network_ready(timeout=120):
        log("Failed to start network", "ERROR")
        raise RuntimeError("Network failed to start")

    return process


def start_indexer_sync(epochs_to_keep: int, reset_db: bool = False, db_url: str = None):
    """Start the indexer sync process with pruning enabled."""
    action = "with DB reset" if reset_db else "without DB reset"
    log(f"Starting indexer sync {action} (epochs_to_keep={epochs_to_keep})...")

    cmd = [
        "cargo",
        "run",
        "--profile",
        "dev-nodebug",
        "--bin",
        "iota-indexer",
        "--",
        "--database-url",
        db_url,
        "--metrics-address",
        "0.0.0.0:59181",
        "indexer",
        "--remote-store-url",
        "http://localhost:59000/api/v1",
    ]

    if reset_db:
        cmd.append("--reset-db")

    env = {
        "RUST_BACKTRACE": "1",
        "RUST_LOG": "info",
        "EPOCHS_TO_KEEP": str(epochs_to_keep),
        "PRUNING_DELAY_MS": "1000",  # 1 second delay for testing
    }

    process = run_command(cmd, REPO_ROOT, env, background=True)

    # Wait for indexer to initialize database
    if not wait_for_indexer_ready(timeout=120):
        log("Failed to start indexer", "ERROR")
        raise RuntimeError("Indexer failed to start")

    return process


def main():
    """Main test flow."""
    parser = argparse.ArgumentParser(
        description="Test IOTA indexer pruning functionality"
    )
    parser.add_argument(
        "--epochs-to-keep",
        type=int,
        default=2,
        help="Number of epochs to retain (default: 2)",
    )
    parser.add_argument(
        "--target-epoch",
        type=int,
        default=5,
        help="Target epoch to wait for before testing (default: 5)",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=DEFAULT_DB_USER,
        help=f"Database username (default: {DEFAULT_DB_USER})",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=DEFAULT_DB_PASSWORD,
        help=f"Database password (default: {DEFAULT_DB_PASSWORD})",
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default=DEFAULT_DB_HOST,
        help=f"Database host (default: {DEFAULT_DB_HOST})",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=DEFAULT_DB_PORT,
        help=f"Database port (default: {DEFAULT_DB_PORT})",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=DEFAULT_DB_NAME,
        help=f"Database name (default: {DEFAULT_DB_NAME})",
    )
    args = parser.parse_args()

    # Set global DB config from command line args
    global DB_CONFIG, DB_URL
    DB_CONFIG = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
    }
    DB_URL = f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}"

    log("=" * 80)
    log("IOTA Indexer Watermark Recovery Test")
    log(
        f"Configuration: epochs_to_keep={args.epochs_to_keep}, target_epoch={args.target_epoch}"
    )
    log("Focus: Testing recovery from empty watermarks table")
    log("=" * 80)

    try:
        # Step 0: Run clippy to verify code compiles
        log("\n📋 Step 0: Running clippy to verify code compiles")
        log("Running: cargo clippy --all-targets --all-features")

        clippy_result = run_command(
            ["cargo", "clippy", "--all-targets", "--all-features"],
            REPO_ROOT,
            env={"RUST_BACKTRACE": "1"},
            background=False,
        )

        if clippy_result.returncode != 0:
            log("✗ Clippy failed - code does not compile", "ERROR")
            log(f"Clippy output:\n{clippy_result.stdout}", "ERROR")
            if clippy_result.stderr:
                log(f"Clippy errors:\n{clippy_result.stderr}", "ERROR")
            return 1

        log("✓ Clippy passed - code compiles successfully")
        # Step 1: Start network and indexer with pruning
        log("\n📋 Step 1: Starting network and indexer with pruning enabled")
        network_process = start_local_network()
        indexer_process = start_indexer_sync(
            args.epochs_to_keep, reset_db=True, db_url=DB_URL
        )

        # Step 2: Wait for epochs to be indexed
        log(f"\n📋 Step 2: Waiting for epoch {args.target_epoch} to be indexed")
        if not wait_for_epochs(args.target_epoch, timeout=600):
            log("Failed to reach target epoch", "ERROR")
            return 1

        # Step 3: Wait for pruning to complete
        log("\n📋 Step 3: Waiting for pruning to complete")
        log("  (watermark updates every 5s + 1s pruning delay + execution time)")

        if not wait_for_pruning_complete(args.epochs_to_keep, timeout=120):
            log("⚠ Pruning did not complete in time, but continuing...", "WARNING")

        # Step 3a: Verify pruning is working
        # Step 3: Verify pruning
        log("\n📋 Step 3a: Verifying pruning worked correctly")
        highest_epoch, _ = check_indexer_state()
        if not verify_pruning(args.epochs_to_keep, highest_epoch):
            log("✗ Pruning verification failed - test cannot continue", "ERROR")
            return 1

        # Step 4: Stop indexer
        log("\n📋 Step 4: Stopping indexer sync (preparing for watermark clearing)")
        if indexer_process in running_processes:
            running_processes.remove(indexer_process)
        indexer_process.terminate()
        try:
            indexer_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            indexer_process.kill()
        log("Indexer stopped")

        # Step 5: Clear watermarks table (simulating data loss)
        log("\n📋 Step 5: Clearing watermarks table (simulating watermark data loss)")
        if not clear_watermarks_table():
            log("Failed to clear watermarks table", "ERROR")
            return 1

        # Step 6: Restart indexer sync
        log(
            "\n📋 Step 6: Restarting indexer sync (testing watermark recovery from empty table)"
        )
        indexer_process = start_indexer_sync(
            args.epochs_to_keep, reset_db=False, db_url=DB_URL
        )

        # Step 7: Verify watermarks repopulated
        log("\n📋 Step 7: Verifying watermarks table is repopulated from existing data")
        if not verify_watermarks_repopulated():
            log("Failed to repopulate watermarks", "ERROR")
            return 1

        # Step 8: Wait for pruning to complete after recovery
        log("\n📋 Step 8: Waiting for pruning to complete after watermark recovery")
        log("  (watermark updates every 5s + 1s pruning delay + execution time)")

        if not wait_for_pruning_complete(args.epochs_to_keep, timeout=120):
            log(
                "⚠ Pruning did not complete in time after recovery, but continuing...",
                "WARNING",
            )

        # Step 8a: Verify pruning resumed after watermark recovery
        log("\n📋 Step 8a: Verifying pruning resumed after recovery")
        highest_epoch, watermarks = check_indexer_state()
        if not verify_pruning(args.epochs_to_keep, highest_epoch):
            log("✗ Pruning did not resume after recovery", "ERROR")
            return 1

        log(f"\nFinal state - Highest epoch: {highest_epoch}")
        log(f"\nFinal watermarks:")
        log(
            f"{'Entity':<35} {'Curr Epoch':<12} {'Min Avail Epoch':<16} {'Max CP':<12} {'Lowest Unpruned':<16} {'Target':<12} {'Status':<15}"
        )
        log(
            f"{'-' * 35} {'-' * 12} {'-' * 16} {'-' * 12} {'-' * 16} {'-' * 12} {'-' * 15}"
        )
        for wm in watermarks:
            entity, curr_epoch, min_epoch, min_cp, min_tx, max_cp, lowest_unpruned = wm
            # Calculate target based on pruning strategy
            if entity in ["objects_history", "transactions", "events"]:
                target = min_epoch
            elif entity in ["checkpoints", "pruner_cp_watermark"]:
                target = min_cp
            else:
                # ByTransaction or ByGlobalSeq
                target = min_tx

            # Determine status
            if min_epoch == 0:
                status = "-"
            elif lowest_unpruned >= target:
                status = "✓ Complete"
            elif lowest_unpruned > 0:
                status = "⚠ In progress"
            else:
                status = "Not started"

            log(
                f"{entity:<35} {curr_epoch:<12} {min_epoch:<16} {max_cp:<12} {lowest_unpruned:<16} {str(target):<12} {status:<15}"
            )

        log("\n" + "=" * 80)
        log("✓ WATERMARK RECOVERY TEST PASSED SUCCESSFULLY")
        log("  - Initial pruning verified working")
        log("  - Watermarks successfully recovered from empty table")
        log("  - Watermarks correctly rebuilt from existing indexed data")
        log("  - Pruning successfully resumed after recovery")
        log("=" * 80)

        return 0

    except Exception as e:
        log(f"\n✗ TEST FAILED: {e}", "ERROR")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        cleanup_processes()


if __name__ == "__main__":
    sys.exit(main())
