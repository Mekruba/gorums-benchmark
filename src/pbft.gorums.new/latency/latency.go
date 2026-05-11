// This file is inspired by and partially taken from:
// https://github.com/relab/hotstuff/blob/main/internal/latency/latency.go
// Original authors: the relab/hotstuff contributors.
// Licensed under the MIT License.

package latency

import (
	"fmt"
	"math"
	"slices"
	"time"
)

// allLocations maps index to node name, e.g. "node-1" .. "node-7"
var allLocations = []string{
	"node-1",
	"node-2",
	"node-3",
	"node-4",
	"node-5",
	"node-6",
	"node-7",
}

// allLatencies[i][j] = simulated one-way latency from allLocations[i] to allLocations[j]
var allLatencies = [][]time.Duration{
	// to:  n1               n2               n3               n4               n5               n6               n7
	{0, 5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 30 * time.Millisecond}, // from n1
	{5 * time.Millisecond, 0, 5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond},  // from n2
	{10 * time.Millisecond, 5 * time.Millisecond, 0, 5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond, 20 * time.Millisecond},  // from n3
	{15 * time.Millisecond, 10 * time.Millisecond, 5 * time.Millisecond, 0, 5 * time.Millisecond, 10 * time.Millisecond, 15 * time.Millisecond},  // from n4
	{20 * time.Millisecond, 15 * time.Millisecond, 10 * time.Millisecond, 5 * time.Millisecond, 0, 5 * time.Millisecond, 10 * time.Millisecond},  // from n5
	{25 * time.Millisecond, 20 * time.Millisecond, 15 * time.Millisecond, 10 * time.Millisecond, 5 * time.Millisecond, 0, 5 * time.Millisecond},  // from n6
	{30 * time.Millisecond, 25 * time.Millisecond, 20 * time.Millisecond, 15 * time.Millisecond, 10 * time.Millisecond, 5 * time.Millisecond, 0}, // from n7
}

const DefaultLocation = "default"

// Between returns the one-way latency between locations a and b.
func Between(a, b string) time.Duration {
	fromIdx := slices.Index(allLocations, a)
	toIdx := slices.Index(allLocations, b)
	return allLatencies[fromIdx][toIdx]
}

// ValidLocation returns the location if valid, or an error otherwise.
func ValidLocation(location string) (string, error) {
	if location == "" || location == DefaultLocation {
		return DefaultLocation, nil
	}
	if !slices.Contains(allLocations, location) {
		return "", fmt.Errorf("location %q not found", location)
	}
	return location, nil
}

// Matrix represents a latency matrix for a subset of locations.
type Matrix struct {
	enabled bool
	lm      [][]time.Duration
	locs    []string
}

// MatrixFrom returns a Matrix for the given location names.
func MatrixFrom(locations []string) Matrix {
	locationIndices := make([]int, len(locations))
	for i, loc := range locations {
		if loc == DefaultLocation {
			return Matrix{}
		}
		idx := slices.Index(allLocations, loc)
		if idx == -1 {
			panic(fmt.Sprintf("location %q not found", loc))
		}
		locationIndices[i] = idx
	}
	lm := make([][]time.Duration, len(locationIndices))
	for i, fromIdx := range locationIndices {
		lm[i] = make([]time.Duration, len(locationIndices))
		for j, toIdx := range locationIndices {
			lm[i][j] = allLatencies[fromIdx][toIdx]
		}
	}
	return Matrix{
		enabled: len(locations) > 0,
		lm:      lm,
		locs:    locations,
	}
}

// MatrixFromIDs constructs a Matrix by mapping node IDs to "node-N" locations.
func MatrixFromIDs(ids []uint32) Matrix {
	locs := make([]string, len(ids))
	for i, id := range ids {
		locs[i] = fmt.Sprintf("node-%d", id)
	}
	return MatrixFrom(locs)
}

// Latency returns the one-way latency between nodes a and b.
func (m Matrix) Latency(a, b uint32) time.Duration {
	return m.lm[a-1][b-1]
}

// Location returns the location string for the given node ID.
func (m Matrix) Location(id uint32) string {
	if id == 0 || !m.enabled {
		return DefaultLocation
	}
	if int(id) > len(m.locs) {
		panic(fmt.Sprintf("ID %d out of range", id))
	}
	return m.locs[id-1]
}

// BestSubset returns the indices (0-based into the Matrix's locs slice) of the
// k nodes whose induced submatrix has the lowest mean off-diagonal latency.
// It exhaustively checks all C(n, k) subsets, which is fine for small n.
func (m Matrix) BestSubset(k int) ([]int, time.Duration) {
	n := len(m.lm)
	if k <= 0 || k > n {
		panic(fmt.Sprintf("k=%d is out of range [1, %d]", k, n))
	}

	bestMean := time.Duration(math.MaxInt64)
	var bestSubset []int

	// Enumerate all C(n,k) index combinations.
	subset := make([]int, k)
	var enumerate func(start, depth int)
	enumerate = func(start, depth int) {
		if depth == k {
			mean := subsetMean(m.lm, subset)
			if mean < bestMean {
				bestMean = mean
				bestSubset = append([]int(nil), subset...)
			}
			return
		}
		for i := start; i <= n-(k-depth); i++ {
			subset[depth] = i
			enumerate(i+1, depth+1)
		}
	}
	enumerate(0, 0)

	return bestSubset, bestMean
}

// subsetMean computes the mean off-diagonal latency for the submatrix
// defined by the given row/col indices into lm.
func subsetMean(lm [][]time.Duration, indices []int) time.Duration {
	k := len(indices)
	if k < 2 {
		return 0
	}
	var total time.Duration
	for _, i := range indices {
		for _, j := range indices {
			if i != j {
				total += lm[i][j]
			}
		}
	}
	entries := k*k - k
	return total / time.Duration(entries)
}

// BestSubsetMatrix returns a Matrix containing only the k nodes with the
// lowest mean pairwise latency.
func (m Matrix) BestSubsetMatrix(k int) Matrix {
	indices, _ := m.BestSubset(k)
	locs := make([]string, len(indices))
	for i, idx := range indices {
		locs[i] = m.locs[idx]
	}
	return MatrixFrom(locs)
}

// Enabled returns true if the matrix was initialised with locations.
func (m Matrix) Enabled() bool {
	return m.enabled
}

// Delay sleeps for the one-way latency between nodes a and b.
func (m Matrix) Delay(a, b uint32) {
	if !m.Enabled() {
		return
	}
	time.Sleep(m.Latency(a, b))
}

// ----------------- STATISTICS OF MATRIX --------------

// offDiagonal calls f for every off-diagonal cell in the matrix.
func (m Matrix) offDiagonal(f func(d time.Duration)) {
	for i := range m.lm {
		for j := range m.lm[i] {
			if i == j {
				continue
			}
			f(m.lm[i][j])
		}
	}
}

func (m Matrix) Mean() time.Duration {
	rows := len(m.lm)
	if rows == 0 {
		return 0
	}
	var total time.Duration
	m.offDiagonal(func(d time.Duration) { total += d })
	return total / time.Duration((rows*rows)-rows)
}

func (m Matrix) Max() time.Duration {
	if len(m.lm) == 0 {
		return 0
	}
	var maximum time.Duration
	m.offDiagonal(func(d time.Duration) { maximum = max(maximum, d) })
	return maximum
}

func (m Matrix) Min() time.Duration {
	if len(m.lm) == 0 {
		return 0
	}
	minimum := time.Duration(math.MaxInt64)
	m.offDiagonal(func(d time.Duration) { minimum = min(minimum, d) })
	return minimum
}

func (m Matrix) StdDev() time.Duration {
	rows := len(m.lm)
	if rows == 0 {
		return 0
	}
	mean := m.Mean()
	var sumSquares float64
	m.offDiagonal(func(d time.Duration) {
		diff := float64(d - mean)
		sumSquares += diff * diff
	})
	entries := (rows * rows) - rows
	stddev := math.Sqrt(sumSquares / float64(entries))
	return time.Duration(stddev)
}

func (m Matrix) Print() {
	if len(m.lm) == 0 {
		return
	}
	// header row
	fmt.Printf("%10s", "")
	for _, loc := range m.locs {
		fmt.Printf("%10s", loc)
	}
	fmt.Println()

	for i, row := range m.lm {
		fmt.Printf("%10s", m.locs[i])
		for j, d := range row {
			if i == j {
				fmt.Printf("%10s", "-")
			} else {
				fmt.Printf("%10s", d.Round(time.Millisecond))
			}
		}
		fmt.Println()
	}

	fmt.Printf("\nMean:   %s\n", m.Mean().Round(time.Millisecond))
	fmt.Printf("Min:    %s\n", m.Min().Round(time.Millisecond))
	fmt.Printf("Max:    %s\n", m.Max().Round(time.Millisecond))
	fmt.Printf("StdDev: %s\n", m.StdDev().Round(time.Millisecond))
}
