package bench

import (
	"fmt"
	"time"
)

const (
	PaxosBroadcastCall             string = "Paxos.BroadcastCall"
	PaxosQuorumCall                string = "Paxos.QuorumCall"
	PaxosQuorumCallBroadcastOption string = "Paxos.QuorumCallBroadcastOption"
	PBFTWithGorums                 string = "PBFT.With.Gorums"
	PBFTWithoutGorums              string = "PBFT.Without.Gorums"
	PBFTGorumsNew                  string = "PBFT.Gorums.New"
	PaxosATA                       string = "Paxos.ATA"
	SimplexGorums                  string = "Simplex.Gorums"
	BFTSmartGorums                 string = "BFT.Smart.Gorums"
)

type initializable interface {
	Init(RunOptions)
}

type benchStruct struct {
	run  func(benchmarkOption, any) (ClientResult, []Result, error)
	init func() initializable
}

var benchTypes = map[string]benchStruct{
	PBFTGorumsNew: {
		run: func(opts benchmarkOption, bench any) (ClientResult, []Result, error) {
			return runBenchmark(opts, bench.(*PbftGorumsNewBenchmark))
		},
		init: func() initializable {
			return &PbftGorumsNewBenchmark{}
		},
	},
	BFTSmartGorums: {
		run: func(opts benchmarkOption, bench any) (ClientResult, []Result, error) {
			return runBenchmark(opts, bench.(*BFTSmartBenchmark))
		},
		init: func() initializable {
			return &BFTSmartBenchmark{}
		},
	},
	PaxosATA: {
		run: func(opts benchmarkOption, bench any) (ClientResult, []Result, error) {
			return runBenchmark(opts, bench.(*PaxosATABenchmark))
		},
		init: func() initializable {
			return &PaxosATABenchmark{}
		},
	},
	SimplexGorums: {
		run: func(opts benchmarkOption, bench any) (ClientResult, []Result, error) {
			return runBenchmark(opts, bench.(*SimplexGorumsBenchmark))
		},
		init: func() initializable {
			b := &SimplexGorumsBenchmark{}
			// Start with 4 active members (quorum=3); nodes 5–7 are standbys.
			// After 30 s a reconfiguration tx expands active membership to all
			// 7 nodes (quorum=5). The resulting .timed.csv shows the latency
			// spike caused by the reconfiguration round.
			b.SetInitialMembers([]uint32{1, 2, 3, 4})
			b.SetReconfig(30*time.Second, []uint32{1, 2, 3, 4, 5, 6, 7})
			return b
		},
	},
}

var threeServers = []string{
	"127.0.0.1:5000",
	"127.0.0.1:5001",
	"127.0.0.1:5002",
}

var benchmarks = []benchmarkOption{
	{
		srvAddrs:       threeServers,
		numClients:     1,
		clientBasePort: 8080,
		numRequests:    10000,
		local:          true,
		runType:        Async,
	},

	{
		srvAddrs:       threeServers,
		numClients:     1,
		clientBasePort: 8080,
		numRequests:    10000,
		local:          true,
		runType:        Sync,
	},

	{
		srvAddrs:       threeServers,
		numClients:     1,
		clientBasePort: 8080,
		numRequests:    10000,
		local:          true,
		runType:        Random,
		reqInterval: struct {
			start int
			end   int
		}{50, 400},
	},

	{
		srvAddrs:       threeServers,
		numClients:     10,
		clientBasePort: 8080,
		numRequests:    1000,
		local:          true,
		runType:        Async,
	},

	{
		srvAddrs:       threeServers,
		numClients:     10,
		clientBasePort: 8080,
		numRequests:    1000,
		local:          true,
		runType:        Sync,
	},

	{
		srvAddrs:       threeServers,
		numClients:     10,
		clientBasePort: 8080,
		numRequests:    1000,
		local:          true,
		runType:        Random,
		reqInterval: struct {
			start int
			end   int
		}{50, 400},
	},

	{
		srvAddrs:       threeServers,
		numClients:     100,
		clientBasePort: 8080,
		numRequests:    1000,
		local:          true,
		runType:        Async,
	},

	{
		srvAddrs:       threeServers,
		numClients:     100,
		clientBasePort: 8080,
		numRequests:    1000,
		local:          true,
		runType:        Sync,
	},

	{
		srvAddrs:       threeServers,
		numClients:     100,
		clientBasePort: 8080,
		numRequests:    1000,
		local:          true,
		runType:        Random,
		reqInterval: struct {
			start int
			end   int
		}{50, 400},
	},
}

func createClients[S, C any](bench Benchmark[S, C], opts RunOptions) {
	fmt.Println("creating clients...")
	for i := 0; i < opts.numClients; i++ {
		addr := fmt.Sprintf("127.0.0.1:%v", opts.clientBasePort+i)
		if opts.clients != nil {
			addr = opts.clients[i]
		}
		bench.AddClient(i, addr, opts.srvAddrs, opts.logger)
	}
}

func warmupFunc[C any](clients []*C, warmup func(*C)) {
	fmt.Println("warming up...")
	warmupChan := make(chan struct{}, len(clients))
	for _, client := range clients {
		go func(client *C) {
			warmup(client)
			warmupChan <- struct{}{}
		}(client)
	}
	for i := 0; i < len(clients); i++ {
		if i%2 == 0 {
			fmt.Print(".")
		}
		<-warmupChan
	}
	fmt.Println()
	fmt.Println()
}
