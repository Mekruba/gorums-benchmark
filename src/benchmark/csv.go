package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
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

func WriteDurations(name string, durations []time.Duration) error {
	path := fmt.Sprintf("./csv/%s.csv", name)
	fmt.Println("writing durations file...")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := csv.NewWriter(file)
	data := make([][]string, 1, len(durations)+1)
	data[0] = []string{"Number", "Latency (µs)"}
	for number, latency := range durations {
		data = append(data, []string{strconv.Itoa(number), strconv.Itoa(int(latency.Microseconds()))})
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
