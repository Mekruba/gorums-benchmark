package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/client"
	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
)

var nodes = []server.NodeInfo{
	{ID: 1, Addr: "localhost:8080"},
	{ID: 2, Addr: "localhost:8081"},
	{ID: 3, Addr: "localhost:8082"},
	{ID: 4, Addr: "localhost:8083"},
}

func main() {
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverID := serverCmd.Uint("id", 0, "Server node ID (1-4)")
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
		// --- PASTE THIS HERE ---
		go func() {
			// Use 6060 for Node 1, 6061 for Node 2, etc.
			pprofPort := 6060 + id
			pprofAddr := fmt.Sprintf("localhost:%d", pprofPort)
			log.Printf("Starting pprof server on %s", pprofAddr)
			log.Println(http.ListenAndServe(pprofAddr, nil))
		}()
		// -----------------------
		server.RunServer(id, nodes, *serverVerbose)

	case "client":
		client.RunClient(nodes)

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
	fmt.Println("  pbft benchmark --mode <latency|throughput> --n <requests> [--steps N]")
}
