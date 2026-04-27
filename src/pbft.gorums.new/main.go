package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/client"
	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/latency"
	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
)

var nodes = []server.NodeInfo{
	{ID: 1, Addr: "localhost:8080"},
	{ID: 2, Addr: "localhost:8081"},
	{ID: 3, Addr: "localhost:8082"},
	{ID: 4, Addr: "localhost:8083"},
	// {ID: 5, Addr: "localhost:8084"},
	// {ID: 6, Addr: "localhost:8085"},
	// {ID: 7, Addr: "localhost:8086"},
}

func main() {
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverID := serverCmd.Uint("id", 0, "Server node ID (1-7)")
	serverVerbose := serverCmd.Bool("verbose", false, "Enable debug logging")

	// benchCmd := flag.NewFlagSet("benchmark", flag.ExitOnError)
	// benchMode := benchCmd.String("mode", "latency", "Benchmark mode: latency or throughput")
	// benchN := benchCmd.Int("n", 100, "Number of requests (latency) or max req/s (throughput)")
	// benchSteps := benchCmd.Int("steps", 10, "Number of steps to sweep throughput")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "server":
		if err := serverCmd.Parse(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		if *serverID == 0 {
			fmt.Fprintln(os.Stderr, "Error: --id is required")
			os.Exit(1)
		}
		id := uint32(*serverID)
		if !validNodeID(id) {
			fmt.Fprintf(os.Stderr, "Error: invalid node ID %d\n", id)
			os.Exit(1)
		}
		server.RunServer(id, nodes, *serverVerbose)

	case "client":
		client.RunClient(nodes)

	case "latency":
		ids := make([]uint32, len(nodes))
		for i, n := range nodes {
			ids[i] = n.ID
		}
		m := latency.MatrixFromIDs(ids)
		fmt.Println("=== Full cluster ===")
		m.Print()

		best := m.BestSubsetMatrix(4)
		fmt.Println("\n=== Best 4-node subset ===")
		best.Print()

	default:
		printUsage()
		os.Exit(1)
	}
}

func validNodeID(id uint32) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  pbft server --id <1-4> [--verbose]")
	fmt.Println("  pbft client")
}
