package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
	"slices"
	"cmp"
)

func WriteThroughputVsLatency(name string, throughputVsLatency [][]string) error {
	path := fmt.Sprintf("./csv/%s.csv", name)
	fmt.Println("writing throughput vs latency file...")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	data := make([][]string, 1, len(throughputVsLatency)+1)
	data[0] = []string{"Throughput", "Latency (avg)", "Latency (med)"}
	data = append(data, throughputVsLatency...)
	return w.WriteAll(data)
}

func WriteDurations(name string, durations []time.Duration, starts []time.Time) error {
    path := fmt.Sprintf("./csv/%s.csv", name)
    fmt.Println("writing durations file...")
    file, err := os.Create(path)
    if err != nil {
        return err
    }
    defer file.Close()

    type entry struct {
        elapsed time.Duration
        latency time.Duration
    }

    var t0 time.Time
    if len(starts) > 0 {
        t0 = starts[0]
        for _, s := range starts {
            if s.Before(t0) {
                t0 = s
            }
        }
    }

    entries := make([]entry, 0, len(durations))
    for i, latency := range durations {
        if i < len(starts) {
            entries = append(entries, entry{
                elapsed: starts[i].Sub(t0),
                latency: latency,
            })
        }
    }

  slices.SortFunc(entries, func(a, b entry) int {
    return cmp.Compare(a.elapsed, b.elapsed)
})

    w := csv.NewWriter(file)
    data := make([][]string, 1, len(entries)+1)
    data[0] = []string{"Number", "Elapsed (ms)", "Latency (µs)"}
    for i, e := range entries {
        data = append(data, []string{
            strconv.Itoa(i),
            strconv.FormatInt(e.elapsed.Milliseconds(), 10),
            strconv.Itoa(int(e.latency.Microseconds())),
        })
    }
    return w.WriteAll(data)
}

// WriteTimedDurations writes a time-series CSV: one row per response in
// response-arrival order. ElapsedMs is the wall-clock offset (ms) from the
// start of response collection; Latency is the per-request round-trip in µs.
// Use this to visualise latency spikes caused by reconfiguration events.
func WriteTimedDurations(name string, starts []int64, latencies []time.Duration) error {
	path := fmt.Sprintf("./csv/%s.csv", name)
	fmt.Println("writing timed durations file...")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	data := make([][]string, 1, len(starts)+1)
	data[0] = []string{"ElapsedMs", "Latency (µs)"}
	for i, start := range starts {
		var lat int64
		if i < len(latencies) {
			lat = latencies[i].Microseconds()
		}
		data = append(data, []string{strconv.FormatInt(start, 10), strconv.FormatInt(lat, 10)})
	}
	return w.WriteAll(data)
}

func WritePerformance(name string, results []string) error {
	path := fmt.Sprintf("./csv/%s.csv", name)
	fmt.Println("writing performance file...")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	data := make([][]string, 2)
	data[0] = []string{"Reqs/client", "Mean (µs)", "Median (µs)", "Std. dev.", "Min (µs)", "Max (µs)"}
	data[1] = results
	return w.WriteAll(data)
}
