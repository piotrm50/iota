#!/usr/bin/env python3
"""
Plot TPS comparison from monitor_sync_tps.py log files.

Usage:
    ./plot_tps_comparison.py <no_pruning_log> <with_pruning_log> [output_plot]
    ./plot_tps_comparison.py --single-file <combined_log> [output_plot]
"""

import argparse
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.signal import medfilt


def parse_log_file(filename):
    """Parse TPS monitor log file."""
    data = {
        "timestamp": [],
        "tx_number": [],
        "epoch": [],
        "ingestion_tps": [],
        "pruning_tx_tps": [],
        "pruning_cp_tps": [],
        "lowest_unpruned_tx": [],
        "lowest_unpruned_cp": [],
    }

    with open(filename, "r") as f:
        for line in f:
            if line.startswith("#"):
                continue
            parts = line.strip().split(",")
            if len(parts) >= 8:
                data["timestamp"].append(float(parts[0]))
                data["tx_number"].append(int(parts[1]))
                data["epoch"].append(int(parts[2]))
                data["ingestion_tps"].append(float(parts[3]))
                data["pruning_tx_tps"].append(float(parts[4]))
                data["pruning_cp_tps"].append(float(parts[5]))
                data["lowest_unpruned_tx"].append(int(parts[6]))
                data["lowest_unpruned_cp"].append(int(parts[7]))

    return data


def smooth_data(data, window_size=11, method="median"):
    """Apply smoothing filter.

    Args:
        data: Data to smooth
        window_size: Window size for smoothing
        method: 'median' or 'mean'
    """
    if method == "median":
        return medfilt(data, kernel_size=window_size)
    elif method == "mean":
        # Use numpy convolution for moving average
        kernel = np.ones(window_size) / window_size
        # Use 'same' mode to keep same length, pad edges
        return np.convolve(data, kernel, mode="same")
    else:
        raise ValueError(f"Unknown smoothing method: {method}")


def main():
    parser = argparse.ArgumentParser(description="Plot TPS comparison from log files")
    parser.add_argument(
        "--single-file", type=str, help="Single combined log file from sequential runs"
    )
    parser.add_argument(
        "--window-size",
        type=int,
        required=True,
        help="Smoothing window size",
    )
    parser.add_argument(
        "--smoothing-method",
        type=str,
        choices=["median", "mean"],
        default="median",
        help="Smoothing method: median or mean (default: median)",
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Log files (2 or 3 files for comparison mode, or output filename for single-file mode)",
    )
    args = parser.parse_args()

    # Validate arguments and determine mode
    if args.single_file:
        # Single file mode
        if len(args.files) > 1:
            print(
                "ERROR: When using --single-file, provide at most one argument (output filename)"
            )
            sys.exit(1)
        output_file = args.files[0] if args.files else "tps_comparison_plot.png"
        # Parse single file
        print(f"Parsing {args.single_file}...")
        all_data = parse_log_file(args.single_file)
        print(f"  Found {len(all_data['tx_number'])} samples")

        # For single file mode, just plot all data as one continuous series
        data_no_pruning = None
        data_with_pruning = all_data
    else:
        # Comparison mode - need 2 or 3 files
        if len(args.files) < 2 or len(args.files) > 3:
            print("ERROR: Provide 2 log files and optional output filename")
            print(
                "Usage: plot_tps_comparison.py <no_pruning_log> <with_pruning_log> [output]"
            )
            sys.exit(1)

        no_pruning_log = args.files[0]
        with_pruning_log = args.files[1]
        output_file = (
            args.files[2] if len(args.files) == 3 else "tps_comparison_plot.png"
        )

        # Parse two separate files (original mode)
        print(f"Parsing {no_pruning_log}...")
        data_no_pruning = parse_log_file(no_pruning_log)
        print(f"  Found {len(data_no_pruning['tx_number'])} samples")

        print(f"Parsing {with_pruning_log}...")
        data_with_pruning = parse_log_file(with_pruning_log)
        print(f"  Found {len(data_with_pruning['tx_number'])} samples")

    # Apply smoothing
    if data_no_pruning:
        no_pruning_smooth = smooth_data(
            data_no_pruning["ingestion_tps"], args.window_size, args.smoothing_method
        )
    with_pruning_smooth = smooth_data(
        data_with_pruning["ingestion_tps"], args.window_size, args.smoothing_method
    )
    pruning_tx_smooth = smooth_data(
        data_with_pruning["pruning_tx_tps"], args.window_size, args.smoothing_method
    )
    pruning_cp_smooth = smooth_data(
        data_with_pruning["pruning_cp_tps"], args.window_size, args.smoothing_method
    )

    # Create plots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    title = (
        "IOTA Indexer TPS (Sequential Runs)"
        if args.single_file
        else "IOTA Indexer TPS Comparison: With vs Without Pruning"
    )
    fig.suptitle(title, fontsize=16, fontweight="bold")

    # Plot 1: Ingestion TPS Comparison
    ax1.set_title("Ingestion TPS Comparison", fontsize=14, fontweight="bold")
    ax1.set_xlabel("Transaction Number", fontsize=12)
    ax1.set_ylabel("Ingestion TPS", fontsize=12)
    ax1.grid(True, alpha=0.3)

    if data_no_pruning:
        # Plot no pruning
        ax1.plot(
            data_no_pruning["tx_number"],
            data_no_pruning["ingestion_tps"],
            "b-",
            linewidth=0.5,
            alpha=0.15,
            label="No Pruning (raw)",
        )
        ax1.plot(
            data_no_pruning["tx_number"],
            no_pruning_smooth,
            "b-",
            linewidth=2.5,
            alpha=0.8,
            label="No Pruning (smoothed)",
        )

    # Plot with/all data
    label_raw = "All Runs (raw)" if args.single_file else "With Pruning (raw)"
    label_smooth = (
        "All Runs (smoothed)" if args.single_file else "With Pruning (smoothed)"
    )
    ax1.plot(
        data_with_pruning["tx_number"],
        data_with_pruning["ingestion_tps"],
        "r-",
        linewidth=0.5,
        alpha=0.15,
        label=label_raw,
    )
    ax1.plot(
        data_with_pruning["tx_number"],
        with_pruning_smooth,
        "r-",
        linewidth=2.5,
        alpha=0.8,
        label=label_smooth,
    )

    ax1.legend(loc="upper left", fontsize=11, framealpha=0.9)
    ax1.set_ylim(bottom=0)

    # Add epoch on secondary axis
    ax1_twin = ax1.twinx()
    ax1_twin.plot(
        data_with_pruning["tx_number"],
        data_with_pruning["epoch"],
        "purple",
        linewidth=1.5,
        alpha=0.4,
        linestyle="--",
        label="Epoch",
    )
    ax1_twin.set_ylabel("Epoch", fontsize=12, color="purple")
    ax1_twin.tick_params(axis="y", labelcolor="purple")
    ax1_twin.legend(loc="upper right", fontsize=11, framealpha=0.9)

    # Plot 2: Pruning TPS (TX and CP)
    ax2.set_title(
        "Pruning TPS (TX-based and Checkpoint-based)", fontsize=14, fontweight="bold"
    )
    ax2.set_xlabel("Transaction Number", fontsize=12)
    ax2.set_ylabel("Pruning TPS", fontsize=12)
    ax2.grid(True, alpha=0.3)

    # Plot TX-based pruning
    ax2.plot(
        data_with_pruning["tx_number"],
        data_with_pruning["pruning_tx_tps"],
        "g-",
        linewidth=0.5,
        alpha=0.15,
        label="TX Pruning (raw)",
    )
    ax2.plot(
        data_with_pruning["tx_number"],
        pruning_tx_smooth,
        "g-",
        linewidth=2.5,
        alpha=0.8,
        label="TX Pruning (smoothed)",
    )

    # Plot CP-based pruning
    ax2.plot(
        data_with_pruning["tx_number"],
        data_with_pruning["pruning_cp_tps"],
        "orange",
        linewidth=0.5,
        alpha=0.15,
        label="CP Pruning (raw)",
    )
    ax2.plot(
        data_with_pruning["tx_number"],
        pruning_cp_smooth,
        "orange",
        linewidth=2.5,
        alpha=0.8,
        label="CP Pruning (smoothed)",
        linestyle="--",
    )

    ax2.legend(loc="upper left", fontsize=11, framealpha=0.9)
    ax2.set_ylim(bottom=0)

    plt.tight_layout()

    # Save plot
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    print(f"\n✅ Plot saved to: {output_file}")

    # Print statistics
    print("\n=== Statistics ===")

    if data_no_pruning:
        print("\nNo Pruning:")
        print(
            f"  TX range: {min(data_no_pruning['tx_number']):,} - {max(data_no_pruning['tx_number']):,}"
        )
        print(
            f"  Ingestion TPS: min={min(data_no_pruning['ingestion_tps']):.2f}, "
            f"max={max(data_no_pruning['ingestion_tps']):.2f}, "
            f"avg={sum(data_no_pruning['ingestion_tps']) / len(data_no_pruning['ingestion_tps']):.2f}"
        )

    run_label = "All Runs" if args.single_file else "With Pruning"
    print(f"\n{run_label}:")
    print(
        f"  TX range: {min(data_with_pruning['tx_number']):,} - {max(data_with_pruning['tx_number']):,}"
    )
    print(
        f"  Ingestion TPS: min={min(data_with_pruning['ingestion_tps']):.2f}, "
        f"max={max(data_with_pruning['ingestion_tps']):.2f}, "
        f"avg={sum(data_with_pruning['ingestion_tps']) / len(data_with_pruning['ingestion_tps']):.2f}"
    )

    # Non-zero pruning stats
    non_zero_tx_pruning = [p for p in data_with_pruning["pruning_tx_tps"] if p > 0]
    if non_zero_tx_pruning:
        print(
            f"  TX Pruning TPS (active): min={min(non_zero_tx_pruning):.2f}, "
            f"max={max(non_zero_tx_pruning):.2f}, "
            f"avg={sum(non_zero_tx_pruning) / len(non_zero_tx_pruning):.2f}"
        )
        print(
            f"  TX Pruning active: {len(non_zero_tx_pruning)}/{len(data_with_pruning['pruning_tx_tps'])} "
            f"samples ({100 * len(non_zero_tx_pruning) / len(data_with_pruning['pruning_tx_tps']):.1f}%)"
        )

    non_zero_cp_pruning = [p for p in data_with_pruning["pruning_cp_tps"] if p > 0]
    if non_zero_cp_pruning:
        print(
            f"  CP Pruning TPS (active): min={min(non_zero_cp_pruning):.2f}, "
            f"max={max(non_zero_cp_pruning):.2f}, "
            f"avg={sum(non_zero_cp_pruning) / len(non_zero_cp_pruning):.2f}"
        )
        print(
            f"  CP Pruning active: {len(non_zero_cp_pruning)}/{len(data_with_pruning['pruning_cp_tps'])} "
            f"samples ({100 * len(non_zero_cp_pruning) / len(data_with_pruning['pruning_cp_tps']):.1f}%)"
        )


if __name__ == "__main__":
    main()
