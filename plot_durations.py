import csv
import matplotlib.pyplot as plt

elapsed, latencies = [], []
with open("./csv/BFT.Smart.Gorums.T100.R0.durations.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        lat = int(row["Latency (µs)"])
        el = int(row["Elapsed (ms)"])
        elapsed.append(el / 1000)   # ms → s
        latencies.append(lat / 1000)  # µs → ms

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(elapsed, latencies, linewidth=0.8, color="steelblue")
ax.set_xlabel("Elapsed time (s)")
ax.set_ylabel("Latency (ms)")
ax.set_title("BFT-Smart Gorums — per-request latency (primary killed during run)")

# mark the spike
peak_idx = latencies.index(max(latencies))
ax.annotate(
    f"spike: {latencies[peak_idx]:.0f} ms\n(leader change)",
    xy=(elapsed[peak_idx], latencies[peak_idx]),
    xytext=(elapsed[peak_idx] + elapsed[-1] * 0.03, latencies[peak_idx] * 0.9),
    arrowprops=dict(arrowstyle="->", color="red"),
    color="red", fontsize=9,
)

ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("images/latency_spike.png", dpi=150)
print("saved images/latency_spike.png")