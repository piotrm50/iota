#!/usr/bin/env python3
"""
Monitor IOTA indexer performance and disk usage through epoch-based phases.

Phase 1: Sync without pruning until target_epoch_1.
Phase 2: Sync with pruning until target_epoch_2.

Usage:
    python monitor_pruning.py --target-epoch-1 150 --target-epoch-2 155 --postgres-dir /path/to/data
"""

import argparse
import csv
import json
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Set matplotlib backend to non-interactive
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg2
from psycopg2.extras import RealDictCursor

# Default Configuration
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "postgrespw"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "iota_indexer"
DEFAULT_REMOTE_STORE_URL = "http://archive-wg.r.testnet.iota.cafe:9001/api/v1"
BOGUS_REMOTE_STORE_URL = "http://127.0.0.1:1/bogus"

# Path configuration
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Pruning Strategy Definitions
EPOCH_PARTITIONED = {"objects_history", "transactions", "events"}
CHECKPOINT_BASED = {"checkpoints", "pruner_cp_watermark"}
TRANSACTION_BASED = {
    "event_emit_package",
    "event_emit_module",
    "event_senders",
    "event_struct_instantiation",
    "event_struct_module",
    "event_struct_name",
    "event_struct_package",
    "tx_calls_pkg",
    "tx_calls_mod",
    "tx_calls_fun",
    "tx_changed_objects",
    "tx_digests",
    "tx_input_objects",
    "tx_kinds",
    "tx_recipients",
    "tx_senders",
    "tx_wrapped_or_deleted_objects",
}
GLOBAL_SEQ_BASED = {"tx_global_order"}

# Global process tracker
running_processes = []


def signal_handler(sig, frame):
    print("\n\n🛑 Interrupted by user, cleaning up...")
    cleanup_processes()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def log(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def cleanup_processes():
    for process in running_processes:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
    running_processes.clear()


def get_db_connection(args):
    return psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )


def get_physical_disk_usage(path: str) -> int:
    """Gets directory size in bytes using du. Fixed for compatibility."""
    try:
        if not os.path.exists(path):
            return 0
        # -s for summary, -k for kilobytes (more portable than -b)
        output = subprocess.check_output(["du", "-sk", path]).split()[0].decode("utf-8")
        return int(output) * 1024
    except Exception as e:
        # Silently fail if du has issues to avoid crashing the monitor
        return 0


def format_bytes(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def get_key_columns_for_table(table_name: str) -> Tuple[str, str]:
    """Returns (lowest_available_col, max_committed_col) based on strategy."""
    if table_name in EPOCH_PARTITIONED:
        if table_name == "objects_history":
            return ("min_available_cp", "current_epoch")
        else:
            return ("min_available_tx", "current_epoch")
    elif table_name in CHECKPOINT_BASED:
        return ("min_available_cp", "max_committed_cp")
    elif table_name in TRANSACTION_BASED or table_name in GLOBAL_SEQ_BASED:
        return ("min_available_tx", "max_committed_tx")
    else:
        return ("min_available_tx", "max_committed_tx")


def monitor_loop(
    stop_event: threading.Event,
    data_points: List[Dict],
    args: argparse.Namespace,
    phase_name: str,
):
    """Background thread to poll metrics every 10 seconds."""

    csv_file = args.metrics_csv
    file_exists = os.path.isfile(csv_file)

    while not stop_event.is_set():
        try:
            conn = get_db_connection(args)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Get Watermarks for Ingestion
                cur.execute("""
                    SELECT max_committed_tx, max_committed_cp, current_epoch, EXTRACT(EPOCH FROM NOW()) as ts
                    FROM watermarks WHERE entity = 'transactions';
                """)
                watermark = cur.fetchone()

                # Check if DB is initialized enough to return data
                if watermark is None:
                    # DB might be empty or in reset state
                    conn.close()
                    time.sleep(2)
                    continue

                # 2. Get Full Watermarks for Pruning Percentages
                cur.execute("SELECT * FROM watermarks;")
                all_watermarks = cur.fetchall()

                pruning_details = []
                for row in all_watermarks:
                    entity = row["entity"]
                    lowest_unpruned = row["lowest_unpruned_key"]

                    if lowest_unpruned is not None:
                        low_avail_col, max_comm_col = get_key_columns_for_table(entity)
                        lowest_avail_val = row.get(low_avail_col, 0) or 0
                        max_comm_val = row.get(max_comm_col, 0) or 0

                        try:
                            l_unpruned = int(lowest_unpruned)
                            l_avail = int(lowest_avail_val)
                            m_comm = int(max_comm_val)

                            pct_pruneable = (
                                (l_unpruned / l_avail * 100) if l_avail > 0 else 0.0
                            )
                            pct_total = (
                                (l_unpruned / m_comm * 100) if m_comm > 0 else 0.0
                            )
                        except (ValueError, TypeError, ZeroDivisionError):
                            pct_pruneable = 0.0
                            pct_total = 0.0

                        pruning_details.append(
                            {
                                "entity": entity,
                                "pct_total": round(pct_total, 2),
                                "pct_pruneable": round(pct_pruneable, 2),
                            }
                        )

                # 3. Get Sizes for tables using Partition-aware logic
                cur.execute("""
                    WITH RECURSIVE partition_info AS (
                        SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename, COALESCE(p.relname, c.relname) AS parent_table, COALESCE(pn.nspname, n.nspname) AS parent_schema, CASE WHEN c.relispartition THEN true ELSE false END AS is_partition
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
                        LEFT JOIN pg_class p ON p.oid = i.inhparent
                        LEFT JOIN pg_namespace pn ON pn.oid = p.relnamespace
                        WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                    )
                    SELECT pi.parent_schema AS schemaname, pi.parent_table AS tablename, COUNT(CASE WHEN pi.is_partition THEN 1 END) AS partition_count, pg_size_pretty(SUM(pg_total_relation_size(format('%I.%I', pi.schemaname, pi.tablename)::regclass))) AS total_size, SUM(psut.n_live_tup) AS live_rows, SUM(psut.n_dead_tup) AS dead_rows, ROUND(100.0 * SUM(psut.n_dead_tup) / NULLIF(SUM(psut.n_live_tup + psut.n_dead_tup), 0), 2) AS "bloat_pct", SUM(pg_total_relation_size(format('%I.%I', pi.schemaname, pi.tablename)::regclass)) AS total_bytes, MAX(psut.last_autovacuum) AS last_autovacuum, MAX(psut.last_vacuum) AS last_vacuum, SUM(psut.autovacuum_count) AS autovacuum_count, SUM(psut.vacuum_count) AS vacuum_count
                    FROM partition_info pi
                    JOIN pg_stat_user_tables psut ON psut.relid = pi.oid
                    GROUP BY pi.parent_schema, pi.parent_table
                    ORDER BY total_bytes DESC;
                """)
                detailed_table_stats = cur.fetchall()
                # Convert Decimals to native types for JSON compatibility
                all_table_stats = {
                    row["tablename"]: {
                        "total_bytes": int(row["total_bytes"])
                        if row.get("total_bytes") is not None
                        else 0,
                        "live_rows": int(row["live_rows"])
                        if row.get("live_rows") is not None
                        else 0,
                        "dead_rows": int(row["dead_rows"])
                        if row.get("dead_rows") is not None
                        else 0,
                        "bloat_pct": float(row["bloat_pct"])
                        if row.get("bloat_pct") is not None
                        else 0.0,
                        "last_autovacuum": row["last_autovacuum"].isoformat()
                        if row.get("last_autovacuum")
                        else None,
                        "last_vacuum": row["last_vacuum"].isoformat()
                        if row.get("last_vacuum")
                        else None,
                        "autovacuum_count": int(row["autovacuum_count"])
                        if row.get("autovacuum_count") is not None
                        else 0,
                        "vacuum_count": int(row["vacuum_count"])
                        if row.get("vacuum_count") is not None
                        else 0,
                    }
                    for row in detailed_table_stats
                }

                # Find the biggest table
                biggest_table = (
                    detailed_table_stats[0] if detailed_table_stats else None
                )
                biggest_str = (
                    f"BIGGEST: {biggest_table['tablename']} ({format_bytes(int(biggest_table['total_bytes']))}, {float(biggest_table.get('bloat_pct') or 0.0):.2f}% bloat)"
                    if biggest_table
                    else ""
                )

                # 4. Get Postgres Logical DB Size
                cur.execute("SELECT pg_database_size(current_database()) as pg_size;")
                pg_db_size = int(cur.fetchone()["pg_size"])

                # 5. Physical Disk Usage
                physical_size = get_physical_disk_usage(args.postgres_dir)

                now_ts = float(watermark["ts"])
                tx_seq = watermark["max_committed_tx"] or 0
                cp_seq = watermark["max_committed_cp"] or 0
                epoch = watermark["current_epoch"] or 0

                entry = {
                    "timestamp": datetime.now().isoformat(),
                    "unix_ts": now_ts,
                    "phase": phase_name,
                    "epoch": epoch,
                    "tx_seq": tx_seq,
                    "cp_seq": cp_seq,
                    "pg_db_size_bytes": pg_db_size,
                    "physical_disk_bytes": physical_size,
                    "all_table_stats": all_table_stats,
                    "pruning_details": pruning_details,
                    "detailed_stats": [dict(r) for r in detailed_table_stats],
                }
                data_points.append(entry)

                # Calculate TPS
                tps = 0.0
                if len(data_points) >= 2:
                    prev = data_points[-2]
                    time_diff = entry["unix_ts"] - prev["unix_ts"]
                    tx_diff = entry["tx_seq"] - prev["tx_seq"]
                    if time_diff > 0:
                        tps = tx_diff / time_diff

                # Log to CSV
                with open(csv_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(
                            [
                                "timestamp",
                                "phase",
                                "epoch",
                                "tx_seq",
                                "cp_seq",
                                "pg_db_size_bytes",
                                "physical_disk_bytes",
                                "table_stats_json",
                                "pruning_details_json",
                            ]
                        )
                        file_exists = True
                    writer.writerow(
                        [
                            entry["timestamp"],
                            phase_name,
                            epoch,
                            tx_seq,
                            cp_seq,
                            pg_db_size,
                            physical_size,
                            json.dumps(all_table_stats),
                            json.dumps(pruning_details),
                        ]
                    )

                # Stdout progress
                summary_tables = ["transactions", "tx_digests", "checkpoints"]
                p_summary = []
                details_map = {d["entity"]: d for d in pruning_details}

                for table in summary_tables:
                    pct = (
                        details_map[table]["pct_total"] if table in details_map else 0.0
                    )
                    stats = all_table_stats.get(table, {})
                    size_bytes = stats.get("total_bytes", 0)
                    bloat_pct = stats.get("bloat_pct", 0.0)
                    p_summary.append(
                        f"{table}: {pct}% ({format_bytes(size_bytes)}, {bloat_pct:.2f}% bloat)"
                    )

                p_str = " | ".join(p_summary)
                print(
                    f"  [{phase_name}] Ep: {epoch} | CP: {cp_seq} | TPS: {tps:>7.2f} | DB: {format_bytes(pg_db_size)} | Disk: {format_bytes(physical_size)} | {p_str} | {biggest_str}",
                    flush=True,
                )

            conn.close()
        except Exception as e:
            pass

        for _ in range(100):
            if stop_event.is_set():
                break
            time.sleep(0.1)


def start_indexer(args, pruning_enabled: bool, reset_db: bool = False):
    log(f"Launching indexer (Pruning: {pruning_enabled})...")

    remote_url = args.remote_store_url
    if pruning_enabled and args.no_ingestion_during_pruning:
        log("Ingestion disabled for pruning phase. Using bogus remote store URL.")
        remote_url = BOGUS_REMOTE_STORE_URL

    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "iota-indexer",
        "--",
        "--database-url",
        f"postgresql://{args.db_user}:{args.db_password}@{args.db_host}:{args.db_port}/{args.db_name}",
        "--metrics-address",
        "0.0.0.0:59181",
        "indexer",
        "--remote-store-url",
        remote_url,
    ]
    if reset_db:
        cmd.append("--reset-db")

    env = os.environ.copy()
    if pruning_enabled:
        env["EPOCHS_TO_KEEP"] = str(args.epochs_to_keep)
        env["PRUNING_DELAY_MS"] = "1000"

    command_str = " ".join(cmd)
    log(f"Executing command: {command_str}")
    if pruning_enabled:
        log(
            f"Environment Variables: EPOCHS_TO_KEEP={env.get('EPOCHS_TO_KEEP')}, PRUNING_DELAY_MS={env.get('PRUNING_DELAY_MS')}"
        )

    process = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    running_processes.append(process)
    return process


def run_phase(
    args,
    pruning_enabled: bool,
    target_epoch: int,
    phase_name: str,
    data_points: List[Dict],
    reset_db: bool = False,
):
    log(f"Starting {phase_name}. Target Epoch: {target_epoch}")

    indexer = start_indexer(args, pruning_enabled, reset_db)
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_loop, args=(stop_event, data_points, args, phase_name)
    )
    monitor_thread.start()

    try:
        current_epoch = 0
        while current_epoch < target_epoch:
            if indexer.poll() is not None:
                if not (pruning_enabled and args.no_ingestion_during_pruning):
                    log("Indexer process exited prematurely!", "ERROR")
                break

            try:
                conn = get_db_connection(args)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT current_epoch FROM watermarks WHERE entity = 'transactions';"
                    )
                    res = cur.fetchone()
                    if res:
                        current_epoch = res[0] or 0
                conn.close()
            except:
                pass
            time.sleep(5)

        log(f"Target epoch {target_epoch} reached for {phase_name}.")
    finally:
        stop_event.set()
        monitor_thread.join()
        indexer.terminate()
        indexer.wait()
        if indexer in running_processes:
            running_processes.remove(indexer)


def main():
    parser = argparse.ArgumentParser(
        description="IOTA Indexer Pruning Monitor (Epoch-to-Epoch)"
    )
    parser.add_argument(
        "--target-epoch-1",
        type=int,
        required=True,
        help="Epoch to reach without pruning",
    )
    parser.add_argument(
        "--target-epoch-2", type=int, required=True, help="Epoch to reach with pruning"
    )
    parser.add_argument("--epochs-to-keep", type=int, default=2)
    parser.add_argument(
        "--postgres-dir",
        type=str,
        required=True,
        help="Physical path to Postgres data directory",
    )
    default_csv = f"sync_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    parser.add_argument("--metrics-csv", type=str, default=default_csv)
    parser.add_argument(
        "--remote-store-url", type=str, default=DEFAULT_REMOTE_STORE_URL
    )
    parser.add_argument(
        "--no-ingestion-during-pruning",
        action="store_true",
        help="Disable ingestion during Phase 2",
    )
    parser.add_argument("--db-user", type=str, default=DEFAULT_DB_USER)
    parser.add_argument("--db-password", type=str, default=DEFAULT_DB_PASSWORD)
    parser.add_argument("--db-host", type=str, default=DEFAULT_DB_HOST)
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--db-name", type=str, default=DEFAULT_DB_NAME)
    parser.add_argument(
        "--reset-start", action="store_true", help="Reset DB at very beginning"
    )

    args = parser.parse_args()
    data_points = []

    log("=" * 60)
    log(f"PHASE 1: Sync to Epoch {args.target_epoch_1} (No Pruning)")
    run_phase(
        args,
        pruning_enabled=False,
        target_epoch=args.target_epoch_1,
        phase_name="NO_PRUNING",
        data_points=data_points,
        reset_db=args.reset_start,
    )

    log("=" * 60)
    log(f"PHASE 2: Sync to Epoch {args.target_epoch_2} (Pruning Enabled)")
    run_phase(
        args,
        pruning_enabled=True,
        target_epoch=args.target_epoch_2,
        phase_name="WITH_PRUNING",
        data_points=data_points,
        reset_db=False,
    )

    log(f"Done. Metrics saved to {args.metrics_csv}")


if __name__ == "__main__":
    main()
