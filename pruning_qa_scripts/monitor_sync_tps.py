#!/usr/bin/env python3
"""
Monitor IOTA indexer TPS while syncing from testnet.

This script:
1. Starts indexer sync from testnet WITHOUT pruning
2. Monitors TPS for 30 seconds
3. Stops indexer
4. Starts indexer sync WITH pruning
5. Monitors TPS for 30 seconds
6. Compares results
7. Generates plots showing ingestion TPS, pruning TPS, and epochs

Usage:
    python monitor_sync_tps.py [--epochs-to-keep N] [--duration SECONDS]
"""

import argparse
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

# Set matplotlib backend to non-interactive for headless servers
import matplotlib

matplotlib.use("Agg")  # Must be set before importing pyplot
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import psycopg2
from matplotlib.figure import Figure

# Default Configuration
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "postgrespw"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "iota_indexer_pruning_qa"

DEFAULT_REMOTE_STORE_URL = "http://archive-wg.r.testnet.iota.cafe:9001/api/v1"

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


def cleanup_processes():
    """Stop all running background processes."""
    log("Stopping all background processes...")
    for process in running_processes:
        if process.poll() is None:  # Process is still running
            log(f"Terminating process {process.pid}")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log(f"Force killing process {process.pid}")
                process.kill()
    running_processes.clear()


def get_db_connection(
    db_user: str, db_password: str, db_host: str, db_port: int, db_name: str
):
    """Get a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
    )


def monitor_tps(
    stop_event: threading.Event,
    samples: List[Tuple[float, int, int]],
    pruning_tx_samples: List[Tuple[float, int]] = None,
    pruning_cp_samples: List[Tuple[float, int]] = None,
    db_user: str = DEFAULT_DB_USER,
    db_password: str = DEFAULT_DB_PASSWORD,
    db_host: str = DEFAULT_DB_HOST,
    db_port: int = DEFAULT_DB_PORT,
    db_name: str = DEFAULT_DB_NAME,
):
    """Background thread that monitors TPS by sampling DB every 10 seconds.

    Args:
        stop_event: Event to signal when to stop monitoring
        samples: List to store (timestamp, tx_sequence_number, epoch) tuples for ingestion
        pruning_tx_samples: Optional list to store (timestamp, lowest_unpruned_tx) tuples for tx-based pruning
        pruning_cp_samples: Optional list to store (timestamp, lowest_unpruned_cp) tuples for checkpoint-based pruning
        db_user: Database username
        db_password: Database password
        db_host: Database host
        db_port: Database port
        db_name: Database name
    """
    while not stop_event.is_set():
        try:
            conn = get_db_connection(db_user, db_password, db_host, db_port, db_name)
            cursor = conn.cursor()
            # Read from watermarks table (much faster than MAX on transactions)
            # Use NOW() to get timestamp from same transaction snapshot as max_committed_tx
            # Both values reflect the same consistent state (READ COMMITTED isolation)
            cursor.execute(
                "SELECT max_committed_tx, current_epoch, EXTRACT(EPOCH FROM NOW()) "
                "FROM watermarks WHERE entity = 'transactions';"
            )
            result = cursor.fetchone()
            if result and result[0] is not None:
                tx_seq = result[0]
                current_epoch = result[1] if result[1] is not None else 0
                # PostgreSQL EXTRACT returns seconds as float with microsecond precision
                db_timestamp = float(result[2])
            else:
                tx_seq = 0
                current_epoch = 0
                db_timestamp = time.time()

            samples.append((db_timestamp, tx_seq, current_epoch))

            # If monitoring pruning, track both tx-based and checkpoint-based pruning
            if pruning_tx_samples is not None:
                cursor.execute(
                    "SELECT lowest_unpruned_key, EXTRACT(EPOCH FROM NOW()) FROM watermarks WHERE entity = 'tx_digests';"
                )
                pruning_tx_result = cursor.fetchone()

                if pruning_tx_result and pruning_tx_result[0] is not None:
                    lowest_unpruned_tx = pruning_tx_result[0]
                    pruning_tx_timestamp = float(pruning_tx_result[1])
                    pruning_tx_samples.append(
                        (pruning_tx_timestamp, lowest_unpruned_tx)
                    )

            if pruning_cp_samples is not None:
                cursor.execute(
                    "SELECT lowest_unpruned_key, EXTRACT(EPOCH FROM NOW()) FROM watermarks WHERE entity = 'checkpoints';"
                )
                pruning_cp_result = cursor.fetchone()

                if pruning_cp_result and pruning_cp_result[0] is not None:
                    lowest_unpruned_cp = pruning_cp_result[0]
                    pruning_cp_timestamp = float(pruning_cp_result[1])
                    pruning_cp_samples.append(
                        (pruning_cp_timestamp, lowest_unpruned_cp)
                    )

            # Close cursor and connection after all queries
            cursor.close()
            conn.close()

            # Calculate and print current TPS if we have at least 2 samples
            if len(samples) >= 2:
                time_diff = samples[-1][0] - samples[-2][0]
                tx_diff = samples[-1][1] - samples[-2][1]
                current_epoch = samples[-1][2]
                if time_diff > 0 and tx_diff > 0:
                    current_tps = tx_diff / time_diff

                    # Calculate tx-based pruning TPS
                    pruning_tx_info = ""
                    if pruning_tx_samples and len(pruning_tx_samples) >= 2:
                        pruning_tx_time_diff = (
                            pruning_tx_samples[-1][0] - pruning_tx_samples[-2][0]
                        )
                        pruning_tx_diff = (
                            pruning_tx_samples[-1][1] - pruning_tx_samples[-2][1]
                        )
                        if pruning_tx_time_diff > 0:
                            pruning_tx_tps = (
                                pruning_tx_diff / pruning_tx_time_diff
                                if pruning_tx_diff > 0
                                else 0.0
                            )
                            pruning_tx_info = f", Pruning TX TPS: {pruning_tx_tps:.2f}"

                    # Calculate checkpoint-based pruning TPS
                    pruning_cp_info = ""
                    if pruning_cp_samples and len(pruning_cp_samples) >= 2:
                        pruning_cp_time_diff = (
                            pruning_cp_samples[-1][0] - pruning_cp_samples[-2][0]
                        )
                        pruning_cp_diff = (
                            pruning_cp_samples[-1][1] - pruning_cp_samples[-2][1]
                        )
                        if pruning_cp_time_diff > 0:
                            pruning_cp_tps = (
                                pruning_cp_diff / pruning_cp_time_diff
                                if pruning_cp_diff > 0
                                else 0.0
                            )
                            pruning_cp_info = f", Pruning CP TPS: {pruning_cp_tps:.2f}"

                    # Add lowest unpruned info
                    lowest_unpruned_info = ""
                    if pruning_tx_samples and len(pruning_tx_samples) >= 1:
                        lowest_unpruned_info += (
                            f", lowest_unpruned_tx: {pruning_tx_samples[-1][1]}"
                        )
                    if pruning_cp_samples and len(pruning_cp_samples) >= 1:
                        lowest_unpruned_info += (
                            f", lowest_unpruned_cp: {pruning_cp_samples[-1][1]}"
                        )

                    print(
                        f"  [TPS Monitor] Ingestion TPS: {current_tps:.2f} (epoch: {current_epoch}, tx: {samples[-1][1]}{pruning_tx_info}{pruning_cp_info}{lowest_unpruned_info}, samples: {len(samples)})",
                        flush=True,
                    )
        except Exception as e:
            # Log errors to help debug issues
            print(f"  [TPS Monitor ERROR] {e}", flush=True)
            import traceback

            traceback.print_exc()

        # Sleep for 10 seconds, but check stop_event more frequently for responsiveness
        for _ in range(100):
            if stop_event.is_set():
                break
            time.sleep(0.1)


def start_indexer_sync(
    epochs_to_keep: int = None,
    reset_db: bool = False,
    remote_store_url: str = None,
    db_url: str = None,
    db_user: str = DEFAULT_DB_USER,
    db_password: str = DEFAULT_DB_PASSWORD,
    db_host: str = DEFAULT_DB_HOST,
    db_port: int = DEFAULT_DB_PORT,
    db_name: str = DEFAULT_DB_NAME,
) -> subprocess.Popen:
    """Start the indexer sync process."""
    if epochs_to_keep:
        log(f"Starting indexer sync WITH pruning (epochs_to_keep={epochs_to_keep})...")
    else:
        log("Starting indexer sync WITHOUT pruning...")

    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "iota-indexer",
        "--",
        "--database-url",
        db_url,
        "--metrics-address",
        "0.0.0.0:59181",
        "indexer",
        "--remote-store-url",
        remote_store_url,
    ]

    if reset_db:
        cmd.append("--reset-db")

    env = os.environ.copy()
    env["RUST_BACKTRACE"] = "1"
    env["RUST_LOG"] = "info"

    if epochs_to_keep:
        env["EPOCHS_TO_KEEP"] = str(epochs_to_keep)
        env["PRUNING_DELAY_MS"] = "1000"

    # Log the command being executed
    cmd_str = " ".join(cmd)
    env_vars = []
    if epochs_to_keep:
        env_vars.append(f"EPOCHS_TO_KEEP={epochs_to_keep}")
        env_vars.append("PRUNING_DELAY_MS=1000")
    if env_vars:
        log(f"Command: {' '.join(env_vars)} {cmd_str}")
    else:
        log(f"Command: {cmd_str}")

    process = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    running_processes.append(process)

    # Wait for indexer to initialize and start syncing
    # Need to see two consecutive increases to ensure stable syncing
    log("Waiting for indexer to initialize and start syncing...")
    start_time = time.time()
    max_wait = 1500  # Maximum 1500 seconds (25 minutes) to wait
    prev_checkpoint = None
    current_checkpoint = None
    consecutive_increases = 0

    while time.time() - start_time < max_wait:
        try:
            conn = get_db_connection(db_user, db_password, db_host, db_port, db_name)
            cursor = conn.cursor()

            # Check if objects_snapshot watermark exists and has max_committed_cp > 0
            cursor.execute(
                "SELECT max_committed_cp FROM watermarks WHERE entity = 'objects_snapshot'"
            )
            watermark_result = cursor.fetchone()

            if (
                not watermark_result
                or watermark_result[0] is None
                or watermark_result[0] <= 0
            ):
                cursor.close()
                conn.close()
                elapsed = time.time() - start_time
                log(
                    f"  Waiting... objects_snapshot watermark not ready yet, elapsed: {elapsed:.1f}s"
                )
                # Reset checkpoint tracking
                prev_checkpoint = None
                current_checkpoint = None
                consecutive_increases = 0
            else:
                # Get the latest checkpoint's sequence number
                cursor.execute(
                    "SELECT sequence_number, timestamp_ms FROM checkpoints "
                    "ORDER BY sequence_number DESC LIMIT 1;"
                )
                result = cursor.fetchone()
                cursor.close()
                conn.close()

                if result and result[0] is not None:
                    sequence_number = result[0]
                    elapsed = time.time() - start_time

                    # Track checkpoint progression
                    prev_checkpoint = current_checkpoint
                    current_checkpoint = sequence_number

                    if prev_checkpoint is not None:
                        if current_checkpoint > prev_checkpoint:
                            consecutive_increases += 1
                            log(
                                f"  Progress: checkpoint {prev_checkpoint} → {current_checkpoint} ({consecutive_increases}/2)"
                            )
                            if consecutive_increases >= 2:
                                log(f"✓ Indexer is syncing stably")
                                return process
                        else:
                            # Reset if checkpoint didn't increase (e.g., DB reset happened)
                            consecutive_increases = 0
                            log(
                                f"  Waiting... Latest checkpoint: {current_checkpoint}, elapsed: {elapsed:.1f}s"
                            )
                    else:
                        log(
                            f"  First checkpoint seen: {current_checkpoint}, waiting for progress..."
                        )
                else:
                    log(
                        f"  Waiting... No checkpoints yet, elapsed: {time.time() - start_time:.1f}s"
                    )
        except Exception as e:
            # Table might not exist yet during DB reset, this is normal
            elapsed = time.time() - start_time
            log(f"  Waiting... DB not ready yet, elapsed: {elapsed:.1f}s")
            # Reset checkpoint tracking since DB was likely reset
            prev_checkpoint = None
            current_checkpoint = None
            consecutive_increases = 0

        time.sleep(5)

    log("⚠ Timeout waiting for sync progress, continuing anyway...", "WARNING")
    return process


def measure_sync_tps(
    duration: int,
    pruning: bool,
    epochs_to_keep: int = None,
    remote_store_url: str = None,
    db_user: str = DEFAULT_DB_USER,
    db_password: str = DEFAULT_DB_PASSWORD,
    db_host: str = DEFAULT_DB_HOST,
    db_port: int = DEFAULT_DB_PORT,
    db_name: str = DEFAULT_DB_NAME,
    output_log_file: str = None,
    bootstrap_db: str = None,
) -> Dict[str, Any]:
    """Measure TPS during sync for specified duration.

    Args:
        output_log_file: Optional file path to save TPS readings
        bootstrap_db: Optional database name to copy from for initialization

    Returns:
        Dictionary with TPS statistics and raw sample data
    """
    # Build database URL
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Initialize database - either reset or copy from bootstrap
    reset_db = True
    if bootstrap_db:
        log(f"Initializing database from bootstrap: {bootstrap_db}")
        # Drop existing database if it exists
        import subprocess

        drop_cmd = f'PGPASSWORD={db_password} psql -h {db_host} -p {db_port} -U {db_user} -c "DROP DATABASE IF EXISTS {db_name};"'
        subprocess.run(drop_cmd, shell=True, capture_output=True)

        # Create new database from template
        create_cmd = f'PGPASSWORD={db_password} psql -h {db_host} -p {db_port} -U {db_user} -c "CREATE DATABASE {db_name} WITH TEMPLATE {bootstrap_db};"'
        result = subprocess.run(create_cmd, shell=True, capture_output=True)
        if result.returncode != 0:
            log(
                f"Failed to create database from template: {result.stderr.decode()}",
                "ERROR",
            )
            raise RuntimeError(f"Failed to bootstrap database from {bootstrap_db}")
        log(f"Database {db_name} created from {bootstrap_db}")
        reset_db = False  # Don't reset DB since we just copied it

    # Start indexer
    indexer_process = start_indexer_sync(
        epochs_to_keep if pruning else None,
        reset_db,
        remote_store_url,
        db_url,
        db_user,
        db_password,
        db_host,
        db_port,
        db_name,
    )

    # Start TPS monitoring
    # Always monitor pruning samples to see if pruning happens even without explicit config
    stop_monitor = threading.Event()
    tps_samples = []
    pruning_tx_samples = []
    pruning_cp_samples = []
    monitor_thread = threading.Thread(
        target=monitor_tps,
        args=(stop_monitor, tps_samples, pruning_tx_samples, pruning_cp_samples),
        kwargs={
            "db_user": db_user,
            "db_password": db_password,
            "db_host": db_host,
            "db_port": db_port,
            "db_name": db_name,
        },
    )
    monitor_thread.daemon = True
    monitor_thread.start()

    log(f"Monitoring TPS for {duration} seconds...")

    # Monitor the indexer output for errors
    start_time = time.time()
    while time.time() - start_time < duration:
        # Read a line from indexer output
        line = indexer_process.stdout.readline()
        if line:
            # Print interesting log lines
            if "ERROR" in line or "WARN" in line:
                print(f"  [Indexer] {line.strip()}")

        # Check if process died
        if indexer_process.poll() is not None:
            log("Indexer process stopped unexpectedly!", "ERROR")
            break

        time.sleep(0.1)

    # Stop monitoring
    stop_monitor.set()
    monitor_thread.join(timeout=2)

    # Save TPS readings to file if requested
    if output_log_file:
        log(f"Saving TPS readings to: {output_log_file}")
        with open(output_log_file, "w") as f:
            f.write("# TPS Monitor Log\n")
            f.write(
                "# Format: timestamp,tx_sequence_number,epoch,ingestion_tps,pruning_tx_tps,pruning_cp_tps,lowest_unpruned_tx,lowest_unpruned_cp\n"
            )
            for i in range(1, len(tps_samples)):
                time_diff = tps_samples[i][0] - tps_samples[i - 1][0]
                tx_diff = tps_samples[i][1] - tps_samples[i - 1][1]
                tx_number = tps_samples[i][1]
                epoch = tps_samples[i][2]
                timestamp = tps_samples[i][0]

                ingestion_tps = (
                    tx_diff / time_diff if time_diff > 0 and tx_diff > 0 else 0.0
                )

                # Calculate tx-based pruning TPS
                pruning_tx_tps = 0.0
                lowest_unpruned_tx = 0
                if pruning_tx_samples and i < len(pruning_tx_samples):
                    if i >= 1:
                        p_time_diff = (
                            pruning_tx_samples[i][0] - pruning_tx_samples[i - 1][0]
                        )
                        p_tx_diff = (
                            pruning_tx_samples[i][1] - pruning_tx_samples[i - 1][1]
                        )
                        if p_time_diff > 0:
                            pruning_tx_tps = (
                                p_tx_diff / p_time_diff if p_tx_diff > 0 else 0.0
                            )
                    lowest_unpruned_tx = pruning_tx_samples[i][1]

                # Calculate checkpoint-based pruning TPS
                pruning_cp_tps = 0.0
                lowest_unpruned_cp = 0
                if pruning_cp_samples and i < len(pruning_cp_samples):
                    if i >= 1:
                        p_time_diff = (
                            pruning_cp_samples[i][0] - pruning_cp_samples[i - 1][0]
                        )
                        p_cp_diff = (
                            pruning_cp_samples[i][1] - pruning_cp_samples[i - 1][1]
                        )
                        if p_time_diff > 0:
                            pruning_cp_tps = (
                                p_cp_diff / p_time_diff if p_cp_diff > 0 else 0.0
                            )
                    lowest_unpruned_cp = pruning_cp_samples[i][1]

                f.write(
                    f"{timestamp},{tx_number},{epoch},{ingestion_tps:.2f},{pruning_tx_tps:.2f},{pruning_cp_tps:.2f},{lowest_unpruned_tx},{lowest_unpruned_cp}\n"
                )

    # Calculate ingestion TPS statistics
    max_ingestion_tps = 0.0
    median_ingestion_tps = 0.0
    ingestion_tps_series = []  # List of (tx_number, tps, epoch) for plotting

    if len(tps_samples) >= 2:
        tps_values = []
        for i in range(1, len(tps_samples)):
            time_diff = tps_samples[i][0] - tps_samples[i - 1][0]
            tx_diff = tps_samples[i][1] - tps_samples[i - 1][1]
            tx_number = tps_samples[i][1]
            epoch = tps_samples[i][2]
            if time_diff > 0 and tx_diff > 0:
                tps = tx_diff / time_diff
                tps_values.append(tps)
                ingestion_tps_series.append((tx_number, tps, epoch))

        if tps_values:
            max_ingestion_tps = max(tps_values)
            median_ingestion_tps = statistics.median(tps_values)
            log(f"Ingestion TPS Statistics:")
            log(f"  Samples collected: {len(tps_samples)}")
            log(f"  Max TPS (10s window): {max_ingestion_tps:.2f}")
            log(f"  Median TPS: {median_ingestion_tps:.2f}")
            log(f"  Total transactions indexed: {tps_samples[-1][1]}")
    else:
        log("Not enough ingestion samples collected!", "WARNING")

    # Calculate tx-based pruning TPS statistics
    max_pruning_tx_tps = 0.0
    median_pruning_tx_tps = 0.0
    pruning_tx_tps_series = []  # List of (tx_number, pruning_tx_tps) for plotting

    if pruning_tx_samples and len(pruning_tx_samples) >= 2:
        pruning_tx_tps_values = []
        for i in range(1, len(pruning_tx_samples)):
            time_diff = pruning_tx_samples[i][0] - pruning_tx_samples[i - 1][0]
            tx_diff = pruning_tx_samples[i][1] - pruning_tx_samples[i - 1][1]
            pruning_timestamp = pruning_tx_samples[i][0]
            tx_number = 0
            for ts, tx, epoch in tps_samples:
                if ts <= pruning_timestamp:
                    tx_number = tx
                else:
                    break

            if time_diff > 0:
                pruning_tx_tps = tx_diff / time_diff if tx_diff > 0 else 0.0
                pruning_tx_tps_values.append(pruning_tx_tps)
                pruning_tx_tps_series.append((tx_number, pruning_tx_tps))

        if pruning_tx_tps_values:
            max_pruning_tx_tps = max(pruning_tx_tps_values)
            median_pruning_tx_tps = statistics.median(pruning_tx_tps_values)
            log(f"\nTX-based Pruning TPS Statistics:")
            log(f"  Samples collected: {len(pruning_tx_samples)}")
            log(f"  Max Pruning TX TPS (10s window): {max_pruning_tx_tps:.2f}")
            log(f"  Median Pruning TX TPS: {median_pruning_tx_tps:.2f}")
            log(f"  Lowest unpruned tx: {pruning_tx_samples[-1][1]}")
        else:
            log("\nNo tx-based pruning activity detected")
    else:
        log("\nNot enough tx-based pruning samples collected")

    # Calculate checkpoint-based pruning TPS statistics
    max_pruning_cp_tps = 0.0
    median_pruning_cp_tps = 0.0
    pruning_cp_tps_series = []  # List of (tx_number, pruning_cp_tps) for plotting

    if pruning_cp_samples and len(pruning_cp_samples) >= 2:
        pruning_cp_tps_values = []
        for i in range(1, len(pruning_cp_samples)):
            time_diff = pruning_cp_samples[i][0] - pruning_cp_samples[i - 1][0]
            cp_diff = pruning_cp_samples[i][1] - pruning_cp_samples[i - 1][1]
            pruning_timestamp = pruning_cp_samples[i][0]
            tx_number = 0
            for ts, tx, epoch in tps_samples:
                if ts <= pruning_timestamp:
                    tx_number = tx
                else:
                    break

            if time_diff > 0:
                pruning_cp_tps = cp_diff / time_diff if cp_diff > 0 else 0.0
                pruning_cp_tps_values.append(pruning_cp_tps)
                pruning_cp_tps_series.append((tx_number, pruning_cp_tps))

        if pruning_cp_tps_values:
            max_pruning_cp_tps = max(pruning_cp_tps_values)
            median_pruning_cp_tps = statistics.median(pruning_cp_tps_values)
            log(f"\nCheckpoint-based Pruning TPS Statistics:")
            log(f"  Samples collected: {len(pruning_cp_samples)}")
            log(f"  Max Pruning CP TPS (10s window): {max_pruning_cp_tps:.2f}")
            log(f"  Median Pruning CP TPS: {median_pruning_cp_tps:.2f}")
            log(f"  Lowest unpruned checkpoint: {pruning_cp_samples[-1][1]}")
        else:
            log("\nNo checkpoint-based pruning activity detected")
    else:
        log("\nNot enough checkpoint-based pruning samples collected")

    # Stop indexer
    log("Stopping indexer...")
    if indexer_process in running_processes:
        running_processes.remove(indexer_process)
    indexer_process.terminate()
    try:
        indexer_process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        indexer_process.kill()

    return {
        "max_ingestion_tps": max_ingestion_tps,
        "median_ingestion_tps": median_ingestion_tps,
        "max_pruning_tx_tps": max_pruning_tx_tps,
        "median_pruning_tx_tps": median_pruning_tx_tps,
        "max_pruning_cp_tps": max_pruning_cp_tps,
        "median_pruning_cp_tps": median_pruning_cp_tps,
        "ingestion_tps_series": ingestion_tps_series,
        "pruning_tx_tps_series": pruning_tx_tps_series,
        "pruning_cp_tps_series": pruning_cp_tps_series,
    }


def measure_sync_tps_sequential(
    duration: int,
    epochs_to_keep: int,
    remote_store_url: str = None,
    db_user: str = DEFAULT_DB_USER,
    db_password: str = DEFAULT_DB_PASSWORD,
    db_host: str = DEFAULT_DB_HOST,
    db_port: int = DEFAULT_DB_PORT,
    db_name: str = DEFAULT_DB_NAME,
    output_log_file_1: str = None,
    output_log_file_2: str = None,
    output_log_file_3: str = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Run indexer sequentially: no-pruning -> pruning -> no-pruning on existing DB.

    Does NOT reset or copy the database between runs.

    Args:
        duration: Duration in seconds for each run
        epochs_to_keep: Number of epochs to keep when pruning
        output_log_file_1: Output file for first run (no pruning)
        output_log_file_2: Output file for second run (with pruning)
        output_log_file_3: Output file for third run (no pruning again)

    Returns:
        Tuple of three result dictionaries (run1, run2, run3)
    """
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Run 1: WITHOUT pruning
    log("\n📋 Run 1/3: Indexing WITHOUT pruning")
    indexer_process_1 = start_indexer_sync(
        epochs_to_keep=None,
        reset_db=False,  # Don't reset - use existing DB
        remote_store_url=remote_store_url,
        db_url=db_url,
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
    )

    stop_monitor_1 = threading.Event()
    tps_samples_1 = []
    pruning_tx_samples_1 = []
    pruning_cp_samples_1 = []
    monitor_thread_1 = threading.Thread(
        target=monitor_tps,
        args=(
            stop_monitor_1,
            tps_samples_1,
            pruning_tx_samples_1,
            pruning_cp_samples_1,
        ),
        kwargs={
            "db_user": db_user,
            "db_password": db_password,
            "db_host": db_host,
            "db_port": db_port,
            "db_name": db_name,
        },
    )
    monitor_thread_1.daemon = True
    monitor_thread_1.start()

    log(f"Monitoring TPS for {duration} seconds...")
    time.sleep(duration)

    stop_monitor_1.set()
    monitor_thread_1.join(timeout=2)

    # Stop indexer 1
    log("Stopping indexer (run 1)...")
    if indexer_process_1 in running_processes:
        running_processes.remove(indexer_process_1)
    indexer_process_1.terminate()
    try:
        indexer_process_1.wait(timeout=10)
    except subprocess.TimeoutExpired:
        indexer_process_1.kill()

    results_1 = _calculate_tps_results(
        tps_samples_1, pruning_tx_samples_1, pruning_cp_samples_1, output_log_file_1
    )

    # Wait between runs
    log("\nWaiting 5 seconds before next run...")
    time.sleep(5)

    # Run 2: WITH pruning
    log(f"\n📋 Run 2/3: Indexing WITH pruning (epochs_to_keep={epochs_to_keep})")
    indexer_process_2 = start_indexer_sync(
        epochs_to_keep=epochs_to_keep,
        reset_db=False,  # Don't reset - continue on same DB
        remote_store_url=remote_store_url,
        db_url=db_url,
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
    )

    stop_monitor_2 = threading.Event()
    tps_samples_2 = []
    pruning_tx_samples_2 = []
    pruning_cp_samples_2 = []
    monitor_thread_2 = threading.Thread(
        target=monitor_tps,
        args=(
            stop_monitor_2,
            tps_samples_2,
            pruning_tx_samples_2,
            pruning_cp_samples_2,
        ),
        kwargs={
            "db_user": db_user,
            "db_password": db_password,
            "db_host": db_host,
            "db_port": db_port,
            "db_name": db_name,
        },
    )
    monitor_thread_2.daemon = True
    monitor_thread_2.start()

    log(f"Monitoring TPS for {duration} seconds...")
    time.sleep(duration)

    stop_monitor_2.set()
    monitor_thread_2.join(timeout=2)

    # Stop indexer 2
    log("Stopping indexer (run 2)...")
    if indexer_process_2 in running_processes:
        running_processes.remove(indexer_process_2)
    indexer_process_2.terminate()
    try:
        indexer_process_2.wait(timeout=10)
    except subprocess.TimeoutExpired:
        indexer_process_2.kill()

    results_2 = _calculate_tps_results(
        tps_samples_2, pruning_tx_samples_2, pruning_cp_samples_2, output_log_file_2
    )

    # Wait between runs
    log("\nWaiting 5 seconds before next run...")
    time.sleep(5)

    # Run 3: WITHOUT pruning again
    log("\n📋 Run 3/3: Indexing WITHOUT pruning (again)")
    indexer_process_3 = start_indexer_sync(
        epochs_to_keep=None,
        reset_db=False,  # Don't reset - continue on same DB
        remote_store_url=remote_store_url,
        db_url=db_url,
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
    )

    stop_monitor_3 = threading.Event()
    tps_samples_3 = []
    pruning_tx_samples_3 = []
    pruning_cp_samples_3 = []
    monitor_thread_3 = threading.Thread(
        target=monitor_tps,
        args=(
            stop_monitor_3,
            tps_samples_3,
            pruning_tx_samples_3,
            pruning_cp_samples_3,
        ),
        kwargs={
            "db_user": db_user,
            "db_password": db_password,
            "db_host": db_host,
            "db_port": db_port,
            "db_name": db_name,
        },
    )
    monitor_thread_3.daemon = True
    monitor_thread_3.start()

    log(f"Monitoring TPS for {duration} seconds...")
    time.sleep(duration)

    stop_monitor_3.set()
    monitor_thread_3.join(timeout=2)

    # Stop indexer 3
    log("Stopping indexer (run 3)...")
    if indexer_process_3 in running_processes:
        running_processes.remove(indexer_process_3)
    indexer_process_3.terminate()
    try:
        indexer_process_3.wait(timeout=10)
    except subprocess.TimeoutExpired:
        indexer_process_3.kill()

    results_3 = _calculate_tps_results(
        tps_samples_3, pruning_tx_samples_3, pruning_cp_samples_3, output_log_file_3
    )

    return results_1, results_2, results_3


def _calculate_tps_results(
    tps_samples, pruning_tx_samples, pruning_cp_samples, output_log_file
):
    """Helper function to calculate TPS statistics from samples."""
    # Save TPS readings to file if requested
    if output_log_file:
        log(f"Saving TPS readings to: {output_log_file}")
        with open(output_log_file, "w") as f:
            f.write("# TPS Monitor Log\n")
            f.write(
                "# Format: timestamp,tx_sequence_number,epoch,ingestion_tps,pruning_tx_tps,pruning_cp_tps,lowest_unpruned_tx,lowest_unpruned_cp\n"
            )
            for i in range(1, len(tps_samples)):
                time_diff = tps_samples[i][0] - tps_samples[i - 1][0]
                tx_diff = tps_samples[i][1] - tps_samples[i - 1][1]
                tx_number = tps_samples[i][1]
                epoch = tps_samples[i][2]
                timestamp = tps_samples[i][0]

                ingestion_tps = (
                    tx_diff / time_diff if time_diff > 0 and tx_diff > 0 else 0.0
                )

                # Calculate tx-based pruning TPS
                pruning_tx_tps = 0.0
                lowest_unpruned_tx = 0
                if pruning_tx_samples and i < len(pruning_tx_samples):
                    if i >= 1:
                        p_time_diff = (
                            pruning_tx_samples[i][0] - pruning_tx_samples[i - 1][0]
                        )
                        p_tx_diff = (
                            pruning_tx_samples[i][1] - pruning_tx_samples[i - 1][1]
                        )
                        if p_time_diff > 0:
                            pruning_tx_tps = (
                                p_tx_diff / p_time_diff if p_tx_diff > 0 else 0.0
                            )
                    lowest_unpruned_tx = pruning_tx_samples[i][1]

                # Calculate checkpoint-based pruning TPS
                pruning_cp_tps = 0.0
                lowest_unpruned_cp = 0
                if pruning_cp_samples and i < len(pruning_cp_samples):
                    if i >= 1:
                        p_time_diff = (
                            pruning_cp_samples[i][0] - pruning_cp_samples[i - 1][0]
                        )
                        p_cp_diff = (
                            pruning_cp_samples[i][1] - pruning_cp_samples[i - 1][1]
                        )
                        if p_time_diff > 0:
                            pruning_cp_tps = (
                                p_cp_diff / p_time_diff if p_cp_diff > 0 else 0.0
                            )
                    lowest_unpruned_cp = pruning_cp_samples[i][1]

                f.write(
                    f"{timestamp},{tx_number},{epoch},{ingestion_tps:.2f},{pruning_tx_tps:.2f},{pruning_cp_tps:.2f},{lowest_unpruned_tx},{lowest_unpruned_cp}\n"
                )

    # Calculate ingestion TPS statistics
    max_ingestion_tps = 0.0
    median_ingestion_tps = 0.0
    ingestion_tps_series = []

    if len(tps_samples) >= 2:
        tps_values = []
        for i in range(1, len(tps_samples)):
            time_diff = tps_samples[i][0] - tps_samples[i - 1][0]
            tx_diff = tps_samples[i][1] - tps_samples[i - 1][1]
            tx_number = tps_samples[i][1]
            epoch = tps_samples[i][2]
            if time_diff > 0 and tx_diff > 0:
                tps = tx_diff / time_diff
                tps_values.append(tps)
                ingestion_tps_series.append((tx_number, tps, epoch))

        if tps_values:
            max_ingestion_tps = max(tps_values)
            median_ingestion_tps = statistics.median(tps_values)
            log(f"Ingestion TPS Statistics:")
            log(f"  Samples collected: {len(tps_samples)}")
            log(f"  Max TPS (10s window): {max_ingestion_tps:.2f}")
            log(f"  Median TPS: {median_ingestion_tps:.2f}")
            log(f"  Total transactions indexed: {tps_samples[-1][1]}")
    else:
        log("Not enough ingestion samples collected!", "WARNING")

    # Calculate tx-based pruning TPS statistics
    max_pruning_tx_tps = 0.0
    median_pruning_tx_tps = 0.0
    pruning_tx_tps_series = []

    if pruning_tx_samples and len(pruning_tx_samples) >= 2:
        pruning_tx_tps_values = []
        for i in range(1, len(pruning_tx_samples)):
            time_diff = pruning_tx_samples[i][0] - pruning_tx_samples[i - 1][0]
            tx_diff = pruning_tx_samples[i][1] - pruning_tx_samples[i - 1][1]
            pruning_timestamp = pruning_tx_samples[i][0]
            tx_number = 0
            for ts, tx, epoch in tps_samples:
                if ts <= pruning_timestamp:
                    tx_number = tx
                else:
                    break

            if time_diff > 0:
                pruning_tx_tps = tx_diff / time_diff if tx_diff > 0 else 0.0
                pruning_tx_tps_values.append(pruning_tx_tps)
                pruning_tx_tps_series.append((tx_number, pruning_tx_tps))

        if pruning_tx_tps_values:
            max_pruning_tx_tps = max(pruning_tx_tps_values)
            median_pruning_tx_tps = statistics.median(pruning_tx_tps_values)
            log(f"\nTX-based Pruning TPS Statistics:")
            log(f"  Samples collected: {len(pruning_tx_samples)}")
            log(f"  Max Pruning TX TPS (10s window): {max_pruning_tx_tps:.2f}")
            log(f"  Median Pruning TX TPS: {median_pruning_tx_tps:.2f}")
            log(f"  Lowest unpruned tx: {pruning_tx_samples[-1][1]}")
        else:
            log("\nNo tx-based pruning activity detected")
    else:
        log("\nNot enough tx-based pruning samples collected")

    # Calculate checkpoint-based pruning TPS statistics
    max_pruning_cp_tps = 0.0
    median_pruning_cp_tps = 0.0
    pruning_cp_tps_series = []

    if pruning_cp_samples and len(pruning_cp_samples) >= 2:
        pruning_cp_tps_values = []
        for i in range(1, len(pruning_cp_samples)):
            time_diff = pruning_cp_samples[i][0] - pruning_cp_samples[i - 1][0]
            cp_diff = pruning_cp_samples[i][1] - pruning_cp_samples[i - 1][1]
            pruning_timestamp = pruning_cp_samples[i][0]
            tx_number = 0
            for ts, tx, epoch in tps_samples:
                if ts <= pruning_timestamp:
                    tx_number = tx
                else:
                    break

            if time_diff > 0:
                pruning_cp_tps = cp_diff / time_diff if cp_diff > 0 else 0.0
                pruning_cp_tps_values.append(pruning_cp_tps)
                pruning_cp_tps_series.append((tx_number, pruning_cp_tps))

        if pruning_cp_tps_values:
            max_pruning_cp_tps = max(pruning_cp_tps_values)
            median_pruning_cp_tps = statistics.median(pruning_cp_tps_values)
            log(f"\nCheckpoint-based Pruning TPS Statistics:")
            log(f"  Samples collected: {len(pruning_cp_samples)}")
            log(f"  Max Pruning CP TPS (10s window): {max_pruning_cp_tps:.2f}")
            log(f"  Median Pruning CP TPS: {median_pruning_cp_tps:.2f}")
            log(f"  Lowest unpruned checkpoint: {pruning_cp_samples[-1][1]}")
        else:
            log("\nNo checkpoint-based pruning activity detected")
    else:
        log("\nNot enough checkpoint-based pruning samples collected")

    return {
        "max_ingestion_tps": max_ingestion_tps,
        "median_ingestion_tps": median_ingestion_tps,
        "max_pruning_tx_tps": max_pruning_tx_tps,
        "median_pruning_tx_tps": median_pruning_tx_tps,
        "max_pruning_cp_tps": max_pruning_cp_tps,
        "median_pruning_cp_tps": median_pruning_cp_tps,
        "ingestion_tps_series": ingestion_tps_series,
        "pruning_tx_tps_series": pruning_tx_tps_series,
        "pruning_cp_tps_series": pruning_cp_tps_series,
    }


def create_plots(
    results_no_pruning: Dict[str, Any],
    results_with_pruning: Dict[str, Any],
    epochs_to_keep: int,
):
    """Create plots comparing ingestion TPS, pruning TPS, and epochs."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(
        f"IOTA Indexer Sync Performance: With vs Without Pruning (epochs_to_keep={epochs_to_keep})",
        fontsize=16,
        fontweight="bold",
    )

    # Plot 1: Ingestion TPS comparison
    ax1.set_title("Ingestion TPS Over Time", fontsize=14, fontweight="bold")
    ax1.set_xlabel("Current Transaction Number", fontsize=12)
    ax1.set_ylabel("Ingestion TPS", fontsize=12)
    ax1.grid(True, alpha=0.3)

    # Plot no-pruning data
    if results_no_pruning["ingestion_tps_series"]:
        tx_nums_no_pruning = [x[0] for x in results_no_pruning["ingestion_tps_series"]]
        tps_no_pruning = [x[1] for x in results_no_pruning["ingestion_tps_series"]]
        ax1.plot(
            tx_nums_no_pruning,
            tps_no_pruning,
            "b-",
            linewidth=2,
            label="Without Pruning",
            alpha=0.7,
        )
        ax1.scatter(tx_nums_no_pruning, tps_no_pruning, c="blue", s=20, alpha=0.5)

    # Plot with-pruning data
    if results_with_pruning["ingestion_tps_series"]:
        tx_nums_with_pruning = [
            x[0] for x in results_with_pruning["ingestion_tps_series"]
        ]
        tps_with_pruning = [x[1] for x in results_with_pruning["ingestion_tps_series"]]
        ax1.plot(
            tx_nums_with_pruning,
            tps_with_pruning,
            "r-",
            linewidth=2,
            label="With Pruning",
            alpha=0.7,
        )
        ax1.scatter(tx_nums_with_pruning, tps_with_pruning, c="red", s=20, alpha=0.5)

    ax1.legend(loc="best", fontsize=11)
    ax1.set_ylim(bottom=0)

    # Add median lines
    if results_no_pruning["median_ingestion_tps"] > 0:
        ax1.axhline(
            y=results_no_pruning["median_ingestion_tps"],
            color="blue",
            linestyle="--",
            linewidth=1,
            alpha=0.5,
            label=f"Median (no pruning): {results_no_pruning['median_ingestion_tps']:.0f} TPS",
        )
    if results_with_pruning["median_ingestion_tps"] > 0:
        ax1.axhline(
            y=results_with_pruning["median_ingestion_tps"],
            color="red",
            linestyle="--",
            linewidth=1,
            alpha=0.5,
            label=f"Median (with pruning): {results_with_pruning['median_ingestion_tps']:.0f} TPS",
        )

    # Plot 2: Pruning TPS and Epoch
    ax2_twin = ax2.twinx()
    ax2.set_title("Pruning TPS and Epoch Progression", fontsize=14, fontweight="bold")
    ax2.set_xlabel("Current Transaction Number", fontsize=12)
    ax2.set_ylabel("Pruning TPS", fontsize=12, color="green")
    ax2_twin.set_ylabel("Epoch", fontsize=12, color="purple")
    ax2.grid(True, alpha=0.3)

    # Plot tx-based pruning TPS
    if results_with_pruning["pruning_tx_tps_series"]:
        tx_nums_pruning = [x[0] for x in results_with_pruning["pruning_tx_tps_series"]]
        pruning_tx_tps = [x[1] for x in results_with_pruning["pruning_tx_tps_series"]]
        ax2.plot(
            tx_nums_pruning,
            pruning_tx_tps,
            "g-",
            linewidth=2,
            label="TX Pruning TPS",
            alpha=0.7,
        )
        ax2.scatter(tx_nums_pruning, pruning_tx_tps, c="green", s=20, alpha=0.5)

        # Add median line
        if results_with_pruning["median_pruning_tx_tps"] > 0:
            ax2.axhline(
                y=results_with_pruning["median_pruning_tx_tps"],
                color="green",
                linestyle="--",
                linewidth=1,
                alpha=0.5,
                label=f"Median TX pruning: {results_with_pruning['median_pruning_tx_tps']:.0f}",
            )

    # Plot checkpoint-based pruning TPS
    if results_with_pruning["pruning_cp_tps_series"]:
        tx_nums_cp_pruning = [
            x[0] for x in results_with_pruning["pruning_cp_tps_series"]
        ]
        pruning_cp_tps = [x[1] for x in results_with_pruning["pruning_cp_tps_series"]]
        ax2.plot(
            tx_nums_cp_pruning,
            pruning_cp_tps,
            "orange",
            linewidth=2,
            label="CP Pruning TPS",
            alpha=0.7,
            linestyle="--",
        )
        ax2.scatter(tx_nums_cp_pruning, pruning_cp_tps, c="orange", s=20, alpha=0.5)

        # Add median line
        if results_with_pruning["median_pruning_cp_tps"] > 0:
            ax2.axhline(
                y=results_with_pruning["median_pruning_cp_tps"],
                color="orange",
                linestyle=":",
                linewidth=1,
                alpha=0.5,
                label=f"Median CP pruning: {results_with_pruning['median_pruning_cp_tps']:.0f}",
            )

    ax2.tick_params(axis="y", labelcolor="green")
    ax2.set_ylim(bottom=0)

    # Plot epoch progression for with-pruning test
    if results_with_pruning["ingestion_tps_series"]:
        tx_nums_epoch = [x[0] for x in results_with_pruning["ingestion_tps_series"]]
        epochs = [x[2] for x in results_with_pruning["ingestion_tps_series"]]
        ax2_twin.plot(
            tx_nums_epoch,
            epochs,
            "purple",
            linewidth=2,
            label="Epoch (with pruning)",
            linestyle="-",
            marker="o",
            markersize=4,
            alpha=0.6,
        )
        ax2_twin.tick_params(axis="y", labelcolor="purple")
        ax2_twin.set_ylim(bottom=0)

    # Add legends
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc="best", fontsize=10)

    plt.tight_layout()

    # Save plot
    plot_filename = f"indexer_tps_comparison_epochs{epochs_to_keep}.png"
    plt.savefig(plot_filename, dpi=150, bbox_inches="tight")
    log(f"\n📊 Plot saved to: {plot_filename}")


def main():
    """Main test flow."""
    parser = argparse.ArgumentParser(
        description="Monitor IOTA indexer sync TPS with and without pruning"
    )
    parser.add_argument(
        "--epochs-to-keep",
        type=int,
        default=2,
        help="Number of epochs to retain when pruning (default: 2)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration to monitor TPS in seconds (default: 30)",
    )
    parser.add_argument(
        "--remote-store-url",
        type=str,
        default=DEFAULT_REMOTE_STORE_URL,
        help=f"Remote store URL to sync from (default: {DEFAULT_REMOTE_STORE_URL})",
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
    parser.add_argument(
        "--output-no-pruning",
        type=str,
        required=True,
        help="Output log file for test WITHOUT pruning",
    )
    parser.add_argument(
        "--output-with-pruning",
        type=str,
        required=True,
        help="Output log file for test WITH pruning",
    )
    parser.add_argument(
        "--bootstrap-db",
        type=str,
        default=None,
        help="Optional database name to copy from for initialization (instead of --reset-db)",
    )
    parser.add_argument(
        "--watermarks-dump",
        type=str,
        required=True,
        help="Output file to dump watermarks table state at end of execution",
    )
    parser.add_argument(
        "--sequential-mode",
        action="store_true",
        help="Run in sequential mode: no-pruning -> pruning -> no-pruning on existing DB without reset",
    )
    parser.add_argument(
        "--output-sequential-3",
        type=str,
        help="Output log file for third sequential run (no pruning again)",
    )
    args = parser.parse_args()

    # Validate arguments for sequential mode
    if args.sequential_mode:
        if not args.output_no_pruning:
            log("ERROR: --output-no-pruning is required in sequential mode", "ERROR")
            return 1
        if not args.output_with_pruning:
            log("ERROR: --output-with-pruning is required in sequential mode", "ERROR")
            return 1
        if not args.output_sequential_3:
            log("ERROR: --output-sequential-3 is required in sequential mode", "ERROR")
            return 1

    log("=" * 80)
    log("IOTA Indexer Sync TPS Monitoring: With and Without Pruning")
    log(
        f"Configuration: epochs_to_keep={args.epochs_to_keep}, duration={args.duration}s"
    )
    log(f"Remote store: {args.remote_store_url}")
    log(f"Sequential mode: {args.sequential_mode}")
    log("=" * 80)

    try:
        if args.sequential_mode:
            # Sequential mode: Run no-pruning -> pruning -> no-pruning on existing DB
            log("\n🔄 Running in SEQUENTIAL mode on existing database")
            log("No database reset or copy will be performed")

            results_run1, results_run2, results_run3 = measure_sync_tps_sequential(
                args.duration,
                args.epochs_to_keep,
                remote_store_url=args.remote_store_url,
                db_user=args.db_user,
                db_password=args.db_password,
                db_host=args.db_host,
                db_port=args.db_port,
                db_name=args.db_name,
                output_log_file_1=args.output_no_pruning,
                output_log_file_2=args.output_with_pruning,
                output_log_file_3=args.output_sequential_3,
            )

            # Compare results
            log("\n" + "=" * 80)
            log("SEQUENTIAL MODE RESULTS")
            log("=" * 80)

            log(f"\nRun 1 - WITHOUT Pruning:")
            log(f"  Max Ingestion TPS: {results_run1['max_ingestion_tps']:.2f}")
            log(f"  Median Ingestion TPS: {results_run1['median_ingestion_tps']:.2f}")

            log(f"\nRun 2 - WITH Pruning (epochs_to_keep={args.epochs_to_keep}):")
            log(f"  Max Ingestion TPS: {results_run2['max_ingestion_tps']:.2f}")
            log(f"  Median Ingestion TPS: {results_run2['median_ingestion_tps']:.2f}")
            log(f"  Max TX Pruning TPS: {results_run2['max_pruning_tx_tps']:.2f}")
            log(f"  Median TX Pruning TPS: {results_run2['median_pruning_tx_tps']:.2f}")

            log(f"\nRun 3 - WITHOUT Pruning (again):")
            log(f"  Max Ingestion TPS: {results_run3['max_ingestion_tps']:.2f}")
            log(f"  Median Ingestion TPS: {results_run3['median_ingestion_tps']:.2f}")

            log("=" * 80)

        else:
            # Original mode: Test with and without pruning (with DB reset/copy)
            # Test 1: Sync WITHOUT pruning
            log("\n📋 Test 1: Measuring sync TPS WITHOUT pruning")
            results_no_pruning = measure_sync_tps(
                args.duration,
                pruning=False,
                remote_store_url=args.remote_store_url,
                db_user=args.db_user,
                db_password=args.db_password,
                db_host=args.db_host,
                db_port=args.db_port,
                db_name=args.db_name,
                output_log_file=args.output_no_pruning,
                bootstrap_db=args.bootstrap_db,
            )

            # Wait a bit between tests
            log("\nWaiting 5 seconds before next test...")
            time.sleep(5)

            # Test 2: Sync WITH pruning
            log(
                f"\n📋 Test 2: Measuring sync TPS WITH pruning (epochs_to_keep={args.epochs_to_keep})"
            )
            results_with_pruning = measure_sync_tps(
                args.duration,
                pruning=True,
                epochs_to_keep=args.epochs_to_keep,
                remote_store_url=args.remote_store_url,
                db_user=args.db_user,
                db_password=args.db_password,
                db_host=args.db_host,
                db_port=args.db_port,
                db_name=args.db_name,
                output_log_file=args.output_with_pruning,
                bootstrap_db=args.bootstrap_db,
            )

            # Compare results
            log("\n" + "=" * 80)
            log("COMPARISON RESULTS")
            log("=" * 80)

            log(f"WITHOUT Pruning:")
            log(f"  Max Ingestion TPS: {results_no_pruning['max_ingestion_tps']:.2f}")
            log(
                f"  Median Ingestion TPS: {results_no_pruning['median_ingestion_tps']:.2f}"
            )
            log(f"  Max TX Pruning TPS: {results_no_pruning['max_pruning_tx_tps']:.2f}")
            log(
                f"  Median TX Pruning TPS: {results_no_pruning['median_pruning_tx_tps']:.2f}"
            )
            log(f"  Max CP Pruning TPS: {results_no_pruning['max_pruning_cp_tps']:.2f}")
            log(
                f"  Median CP Pruning TPS: {results_no_pruning['median_pruning_cp_tps']:.2f}"
            )

            log(f"\nWITH Pruning (epochs_to_keep={args.epochs_to_keep}):")
            log(f"  Max Ingestion TPS: {results_with_pruning['max_ingestion_tps']:.2f}")
            log(
                f"  Median Ingestion TPS: {results_with_pruning['median_ingestion_tps']:.2f}"
            )
            log(
                f"  Max TX Pruning TPS: {results_with_pruning['max_pruning_tx_tps']:.2f}"
            )
            log(
                f"  Median TX Pruning TPS: {results_with_pruning['median_pruning_tx_tps']:.2f}"
            )
            log(
                f"  Max CP Pruning TPS: {results_with_pruning['max_pruning_cp_tps']:.2f}"
            )
            log(
                f"  Median CP Pruning TPS: {results_with_pruning['median_pruning_cp_tps']:.2f}"
            )

            if results_no_pruning["max_ingestion_tps"] > 0:
                max_diff = (
                    results_with_pruning["max_ingestion_tps"]
                    - results_no_pruning["max_ingestion_tps"]
                )
                max_diff_pct = (
                    max_diff / results_no_pruning["max_ingestion_tps"]
                ) * 100
                log(f"\nMax TPS Difference:")
                log(f"  {max_diff:+.2f} TPS ({max_diff_pct:+.2f}%)")

            if results_no_pruning["median_ingestion_tps"] > 0:
                median_diff = (
                    results_with_pruning["median_ingestion_tps"]
                    - results_no_pruning["median_ingestion_tps"]
                )
                median_diff_pct = (
                    median_diff / results_no_pruning["median_ingestion_tps"]
                ) * 100
                log(f"\nMedian TPS Difference:")
                log(f"  {median_diff:+.2f} TPS ({median_diff_pct:+.2f}%)")

                if abs(median_diff_pct) < 5:
                    log("\n✓ Pruning has minimal impact on sync TPS (< 5% difference)")
                elif abs(median_diff_pct) < 10:
                    log(
                        "\n⚠ Pruning has moderate impact on sync TPS (5-10% difference)"
                    )
                else:
                    log(
                        "\n✗ Pruning has significant impact on sync TPS (> 10% difference)"
                    )

            log("=" * 80)

            # Create plots
            log("\n📊 Generating plots...")
            create_plots(results_no_pruning, results_with_pruning, args.epochs_to_keep)

        # Dump watermarks table
        log(f"\n📊 Dumping watermarks table to: {args.watermarks_dump}")
        try:
            conn = get_db_connection(
                args.db_user, args.db_password, args.db_host, args.db_port, args.db_name
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT entity, max_committed_tx, current_epoch, max_committed_cp, "
                "lowest_unpruned_key, min_available_tx, min_available_cp, min_available_epoch, "
                "min_bounds_updated_at_timestamp_ms "
                "FROM watermarks ORDER BY entity;"
            )
            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            with open(args.watermarks_dump, "w") as f:
                f.write("# Watermarks Table Dump\n")
                f.write(
                    "# Format: entity,max_committed_tx,current_epoch,max_committed_cp,"
                    "lowest_unpruned_key,min_available_tx,min_available_cp,min_available_epoch,"
                    "min_bounds_updated_at_timestamp_ms\n"
                )
                for row in rows:
                    # Convert None to empty string for CSV
                    row_str = ",".join(str(x) if x is not None else "" for x in row)
                    f.write(f"{row_str}\n")

            log(f"✅ Watermarks dumped: {len(rows)} entries")
        except Exception as e:
            log(f"⚠ Failed to dump watermarks: {e}", "WARNING")

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
