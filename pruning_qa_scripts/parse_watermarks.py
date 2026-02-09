#!/usr/bin/env python3
"""
Parse watermarks dump log and generate a markdown table showing:
- Current epoch
- Min available epoch
- Lowest unpruned key
- Lowest available key
- Max committed key

The key columns vary by table based on pruning strategy:
- Epoch-partitioned tables (objects_history, transactions, events): use checkpoint/tx sequence numbers
- Checkpoint-based tables (checkpoints, pruner_cp_watermark): use checkpoint sequence numbers
- Transaction-based tables: use transaction sequence numbers
- Global sequence tables (tx_global_order): use global sequence numbers
"""

import re
import sys
from typing import Dict, List, Tuple

# Define pruning strategies based on the Rust code
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


def parse_watermarks_file(filepath: str) -> List[Dict[str, str]]:
    """Parse the watermarks dump file and return a list of table records."""
    records = []

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if line.startswith("#") or not line:
                continue

            parts = line.split(",")
            if len(parts) >= 9:
                record = {
                    "entity": parts[0],
                    "max_committed_tx": parts[1],
                    "current_epoch": parts[2],
                    "max_committed_cp": parts[3],
                    "lowest_unpruned_key": parts[4],
                    "min_available_tx": parts[5],
                    "min_available_cp": parts[6],
                    "min_available_epoch": parts[7],
                    "min_bounds_updated_at_timestamp_ms": parts[8],
                }
                records.append(record)

    return records


def get_key_columns(table_name: str) -> Tuple[str, str, str, str, str, str]:
    """
    Return the appropriate column names for (max_committed_col, min_available, lowest_unpruned, key_type, lowest_available_col, max_committed_col_for_display)
    based on the table's pruning strategy.

    The lowest_available is determined from min_available_epoch, min_available_cp, or min_available_tx
    depending on the pruning strategy.
    The max_committed is determined from current_epoch, max_committed_cp, or max_committed_tx
    depending on the pruning strategy.
    """
    if table_name in EPOCH_PARTITIONED:
        # Epoch-partitioned tables - pruned by dropping partitions
        # Use checkpoint sequence for objects_history, tx sequence for transactions/events
        if table_name == "objects_history":
            return (
                "max_committed_cp",
                "min_available_cp",
                "lowest_unpruned_key",
                "Epoch",
                "min_available_epoch",
                "current_epoch",
            )
        else:  # transactions, events
            return (
                "max_committed_tx",
                "min_available_tx",
                "lowest_unpruned_key",
                "Epoch",
                "min_available_epoch",
                "current_epoch",
            )

    elif table_name in CHECKPOINT_BASED:
        return (
            "max_committed_cp",
            "min_available_cp",
            "lowest_unpruned_key",
            "Checkpoint",
            "min_available_cp",
            "max_committed_cp",
        )

    elif table_name in TRANSACTION_BASED:
        return (
            "max_committed_tx",
            "min_available_tx",
            "lowest_unpruned_key",
            "Transaction",
            "min_available_tx",
            "max_committed_tx",
        )

    elif table_name in GLOBAL_SEQ_BASED:
        # tx_global_order uses transaction sequence numbers
        return (
            "max_committed_tx",
            "min_available_tx",
            "lowest_unpruned_key",
            "Transaction",
            "min_available_tx",
            "max_committed_tx",
        )

    else:
        # Default to transaction-based
        return (
            "max_committed_tx",
            "min_available_tx",
            "lowest_unpruned_key",
            "Transaction",
            "min_available_tx",
            "max_committed_tx",
        )


def generate_markdown_table(records: List[Dict[str, str]]) -> str:
    """Generate a markdown table from the records."""

    # Prepare data rows
    rows = []
    for record in records:
        table_name = record["entity"]
        current_epoch = record["current_epoch"]
        min_available_epoch = record["min_available_epoch"]
        lowest_unpruned = record["lowest_unpruned_key"]

        # Get the appropriate key columns for this table
        (
            max_key_col,
            min_key_col,
            unpruned_key_col,
            key_type,
            lowest_available_col,
            max_committed_col,
        ) = get_key_columns(table_name)

        lowest_available = record[lowest_available_col]
        max_committed = record[max_committed_col]

        # Calculate percentages
        # % of pruneable data that is pruned: lowest_unpruned / lowest_available
        # % of total data that is pruned: lowest_unpruned / max_committed
        try:
            lowest_unpruned_val = int(lowest_unpruned)
            lowest_available_val = int(lowest_available)
            max_committed_val = int(max_committed)

            if lowest_available_val > 0:
                pct_pruneable = (lowest_unpruned_val / lowest_available_val) * 100
            else:
                pct_pruneable = 0.0

            if max_committed_val > 0:
                pct_total = (lowest_unpruned_val / max_committed_val) * 100
            else:
                pct_total = 0.0
        except (ValueError, ZeroDivisionError):
            pct_pruneable = 0.0
            pct_total = 0.0

        rows.append(
            {
                "table": table_name,
                "current_epoch": current_epoch,
                "min_available_epoch": min_available_epoch,
                "lowest_unpruned": lowest_unpruned,
                "lowest_available": lowest_available,
                "max_committed": max_committed,
                "key_type": key_type,
                "pct_pruneable_pruned": f"{pct_pruneable:.2f}%",
                "pct_total_pruned": f"{pct_total:.2f}%",
            }
        )

    # Sort rows by pruning strategy (Epoch, Checkpoint, Transaction) then by table name
    def sort_key(row):
        key_type = row["key_type"]
        if key_type == "Epoch":
            order = 0
        elif key_type == "Checkpoint":
            order = 1
        else:  # Transaction
            order = 2
        return (order, row["table"])

    rows.sort(key=sort_key)

    # Filter out rows where min_available_epoch is 0
    rows = [row for row in rows if row["min_available_epoch"] != "0"]

    # Calculate column widths
    col_widths = {
        "table": max(len("Table"), max(len(row["table"]) for row in rows)),
        "current_epoch": max(
            len("Current Epoch"), max(len(row["current_epoch"]) for row in rows)
        ),
        "min_available_epoch": max(
            len("Min Available Epoch"),
            max(len(row["min_available_epoch"]) for row in rows),
        ),
        "key_type": max(len("Key Type"), max(len(row["key_type"]) for row in rows)),
        "lowest_unpruned": max(
            len("Lowest Unpruned Key"), max(len(row["lowest_unpruned"]) for row in rows)
        ),
        "lowest_available": max(
            len("Lowest Available Key"),
            max(len(row["lowest_available"]) for row in rows),
        ),
        "max_committed": max(
            len("Max Committed Key"), max(len(row["max_committed"]) for row in rows)
        ),
        "pct_pruneable_pruned": max(
            len("% Pruneable Pruned"),
            max(len(row["pct_pruneable_pruned"]) for row in rows),
        ),
        "pct_total_pruned": max(
            len("% Total Pruned"), max(len(row["pct_total_pruned"]) for row in rows)
        ),
    }

    # Build the table
    md = f"| {'Table'.ljust(col_widths['table'])} | {'Current Epoch'.ljust(col_widths['current_epoch'])} | {'Min Available Epoch'.ljust(col_widths['min_available_epoch'])} | {'Key Type'.ljust(col_widths['key_type'])} | {'Lowest Unpruned Key'.ljust(col_widths['lowest_unpruned'])} | {'Lowest Available Key'.ljust(col_widths['lowest_available'])} | {'Max Committed Key'.ljust(col_widths['max_committed'])} | {'% Pruneable Pruned'.ljust(col_widths['pct_pruneable_pruned'])} | {'% Total Pruned'.ljust(col_widths['pct_total_pruned'])} |\n"
    md += f"|{'-' * (col_widths['table'] + 2)}|{'-' * (col_widths['current_epoch'] + 2)}|{'-' * (col_widths['min_available_epoch'] + 2)}|{'-' * (col_widths['key_type'] + 2)}|{'-' * (col_widths['lowest_unpruned'] + 2)}|{'-' * (col_widths['lowest_available'] + 2)}|{'-' * (col_widths['max_committed'] + 2)}|{'-' * (col_widths['pct_pruneable_pruned'] + 2)}|{'-' * (col_widths['pct_total_pruned'] + 2)}|\n"

    for row in rows:
        md += f"| {row['table'].ljust(col_widths['table'])} | {row['current_epoch'].ljust(col_widths['current_epoch'])} | {row['min_available_epoch'].ljust(col_widths['min_available_epoch'])} | {row['key_type'].ljust(col_widths['key_type'])} | {row['lowest_unpruned'].ljust(col_widths['lowest_unpruned'])} | {row['lowest_available'].ljust(col_widths['lowest_available'])} | {row['max_committed'].ljust(col_widths['max_committed'])} | {row['pct_pruneable_pruned'].ljust(col_widths['pct_pruneable_pruned'])} | {row['pct_total_pruned'].ljust(col_widths['pct_total_pruned'])} |\n"

    return md


def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_watermarks.py <watermarks_dump.log>")
        sys.exit(1)

    filepath = sys.argv[1]

    try:
        records = parse_watermarks_file(filepath)
        markdown_table = generate_markdown_table(records)
        print(markdown_table)
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
