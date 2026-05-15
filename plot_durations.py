#!/usr/bin/env python3
"""
Per-request latency time-series plot for BFT-Smart with leader kill.
Averages 5 runs, styled to match the throughput plot script.
"""

import csv
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def load_timed(filepath):
    """Load a .timed.csv, drop negative/zero values, sort by ElapsedMs."""
    rows = []
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            e = int(row["ElapsedMs"])
            l = int(row["Latency (µs)"])
            if e > 0 and l > 0:          # drop negatives and zeros
                rows.append((e, l))
    rows.sort(key=lambda r: r[0])
    elapsed_s  = np.array([r[0] / 1000 for r in rows])
    latency_ms = np.array([r[1] / 1000 for r in rows])
    return elapsed_s, latency_ms


def interpolate_to_common_grid(all_elapsed, all_latency, num_points=500):
    t_min = max(e[0]  for e in all_elapsed)
    t_max = min(e[-1] for e in all_elapsed)
    grid  = np.linspace(t_min, t_max, num_points)
    interpolated = np.array([
        np.interp(grid, elapsed, latency)
        for elapsed, latency in zip(all_elapsed, all_latency)
    ])
    return grid, interpolated


def main():
    Path("images").mkdir(exist_ok=True)

    runs = [1, 2, 3, 4, 5]
    base = "BFT.Smart.Gorums.T100.R0"
    all_elapsed, all_latency = [], []

    for r in runs:
        path = Path(f"csv/{base}-{r}.timed.csv")
        if not path.exists():
            print(f"Warning: {path} not found, skipping")
            continue
        print(f"Loaded {path}")
        e, l = load_timed(path)
        all_elapsed.append(e)
        all_latency.append(l)

    if not all_elapsed:
        print("No data loaded — exiting.")
        return

    grid, latency_matrix = interpolate_to_common_grid(all_elapsed, all_latency)

    mean_lat = np.mean(latency_matrix, axis=0)
    std_lat  = np.std(latency_matrix,  axis=0)

    # ── plot ──────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 6))

# mean line
    ax.plot(grid, mean_lat, color="#d62728", linewidth=2.5, label="Mean", zorder=5)

    # ±1 std dev band
    ax.fill_between(grid,
                    np.maximum(mean_lat - std_lat, 0),
                    mean_lat + std_lat,
                    alpha=0.4, color="#ff7f0e", label="±1 Std Dev")

    # ±2 std dev band
    ax.fill_between(grid,
                    np.maximum(mean_lat - 2*std_lat, 0),
                    mean_lat + 2*std_lat,
                    alpha=0.2, color="#ffbb78", label="±2 Std Dev")

    # spike annotation on the mean
    peak_idx = int(np.argmax(mean_lat))
    ax.annotate(
        f"spike: {mean_lat[peak_idx]:.0f} ms\n(leader change)",
        xy=(grid[peak_idx], mean_lat[peak_idx]),
        xytext=(grid[peak_idx] + grid[-1] * 0.03, mean_lat[peak_idx] * 0.9),
        arrowprops=dict(arrowstyle="->", color="red"),
        color="red", fontsize=9,
    )

    ax.set_xlabel("Elapsed time (s)", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_title(
        "BFT-Smart Gorums — per-request latency (primary killed during run)",
        fontsize=13, fontweight="bold",
    )
    ax.legend(loc="upper left", fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = "images/latency_spike.png"
    fig.savefig(out, dpi=300, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.show()


if __name__ == "__main__":
    main()