package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Mekruba/gorums-benchmark/simplex.gorums/server"
)

var nodes = []server.NodeInfo{
	{ID: 1, Addr: "localhost:9090"},
	{ID: 2, Addr: "localhost:9091"},
	{ID: 3, Addr: "localhost:9092"},
	{ID: 4, Addr: "localhost:9093"},
}

func main() {
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverID := serverCmd.Uint("id", 0, "Server node ID (1-4)")
	serverVerbose := serverCmd.Bool("verbose", false, "Enable debug logging")

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
		server.InitKeys(len(nodes))
		server.RunServer(id, nodes, *serverVerbose)

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
	fmt.Println("  simplex server --id <1-4> [--verbose]")
}
