package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/Mekruba/gorums-benchmark/bft-smart.gorums/client"
	"github.com/Mekruba/gorums-benchmark/bft-smart.gorums/server"
)

var nodes = []server.NodeInfo{
	{ID: 1, Addr: "localhost:8080"},
	{ID: 2, Addr: "localhost:8081"},
	{ID: 3, Addr: "localhost:8082"},
	{ID: 4, Addr: "localhost:8083"},
}

var standbys = []server.NodeInfo{
	{ID: 5, Addr: "localhost:8084"},
}

func main() {
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	id := serverCmd.Uint("id", 0, "Node ID")
	verbose := serverCmd.Bool("verbose", false, "Debug logging")
	standby := serverCmd.Bool("standby", false, "Start as standby")

	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "server":
		if err := serverCmd.Parse(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		if *id == 0 {
			fmt.Fprintln(os.Stderr, "--id is required")
			os.Exit(1)
		}
		nid := uint32(*id)
		if *standby {
			if !inList(nid, standbys) {
				fmt.Fprintf(os.Stderr, "node %d is not a standby\n", nid)
				os.Exit(1)
			}
			server.RunServer(nid, append(slices.Clone(nodes), standbys...), *verbose, true)
		} else {
			if !inList(nid, nodes) {
				fmt.Fprintf(os.Stderr, "invalid node ID %d\n", nid)
				os.Exit(1)
			}
			server.RunServer(nid, nodes, *verbose, false)
		}

	case "client":
		client.RunClient(append(slices.Clone(nodes), standbys...))

	default:
		usage()
	}
}

func inList(id uint32, list []server.NodeInfo) bool {
	for _, n := range list {
		if n.ID == id {
			return true
		}
	}
	return false
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  bftsmart server --id <1-4> [--verbose]")
	fmt.Println("  bftsmart server --id 5 --standby [--verbose]")
	fmt.Println("  bftsmart client")
	os.Exit(1)
}
