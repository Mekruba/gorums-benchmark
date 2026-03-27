package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"

	bench "github.com/Mekruba/gorums-benchmark/benchmark"
	pbftGorumsNew "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
	simplexGorumsNew "github.com/Mekruba/gorums-benchmark/simplex.gorums/server"
	"github.com/joho/godotenv"
)

func main() {
	id := flag.Int("id", -1, "nodeID")
	runSrv := flag.Bool("server", false, "default: false")
	benchTypeIndex := flag.Int("run", 0, "type of benchmark to run"+mapping.String())
	memProfile := flag.Bool("mem", false, "create memory and cpu profile")
	numClients := flag.Int("clients", 0, "number of clients to run")
	clientBasePort := flag.Int("port", 0, "which base port to use for clients")
	withLogger := flag.Bool("log", false, "run with structured logger. Default: false")
	local := flag.Bool("local", false, "run servers locally. Default: false")
	throughput := flag.Int("throughput", 0, "target throughput")
	runs := flag.Int("runs", 0, "number of runs of each benchmark")
	steps := flag.Int("steps", 0, "number of increments on throughput vs latency benchmarks")
	dur := flag.Int("dur", 0, "number of seconds throughput vs latency benchmarks should run")
	flag.Parse()

	godotenv.Load(".env")

	fmt.Println("--------")
	fmt.Println("Servers")
	servers, clients := getConfig()
	for id, srv := range servers {
		fmt.Printf("\tServer %v --> ID: %d, Address: %s, Port: %s\n", id, srv.ID, srv.Addr, srv.Port)
	}
	fmt.Println("--------")
	fmt.Println("Clients")
	for id, client := range clients {
		fmt.Printf("\tClient %v --> ID: %d, Address: %s, Port: %s\n", id, client.ID, client.Addr, client.Port)
	}
	fmt.Println()

	if os.Getenv("SERVER") == "1" {
		*runSrv = true
	}

	if os.Getenv("LOG") == "1" {
		*withLogger = true
	}

	if os.Getenv("THROUGHPUT") != "" {
		var err error
		*throughput, err = strconv.Atoi(os.Getenv("THROUGHPUT"))
		if err != nil {
			panic(err)
		}
	}

	if os.Getenv("STEPS") != "" {
		var err error
		*steps, err = strconv.Atoi(os.Getenv("STEPS"))
		if err != nil {
			panic(err)
		}
	}

	if os.Getenv("DUR") != "" {
		var err error
		*dur, err = strconv.Atoi(os.Getenv("DUR"))
		if err != nil {
			panic(err)
		}
	}

	if os.Getenv("RUNS") != "" {
		var err error
		*runs, err = strconv.Atoi(os.Getenv("RUNS"))
		if err != nil {
			panic(err)
		}
	}

	srvID := *id
	if srvID < 0 && *runSrv {
		var err error
		srvID, err = strconv.Atoi(os.Getenv("ID"))
		if err != nil {
			panic(err)
		}
	}

	runType := bench.Throughput
	if os.Getenv("TYPE") != "" {
		runTypeInt, err := strconv.Atoi(os.Getenv("TYPE"))
		if err != nil {
			panic(err)
		}
		runType = bench.RunType(runTypeInt)
	}

	bT := *benchTypeIndex
	if bT <= 0 {
		var err error
		bT, err = strconv.Atoi(os.Getenv("BENCH"))
		if err != nil {
			panic(err)
		}
	}
	benchType, ok := mapping[bT]
	if !ok {
		panic("invalid bench type")
	}

	if *runSrv {
		if srvID > len(servers) {
			// we start more docker containers than
			// what is specified in config. Hence,
			// we just return early.
			return
		}
		runServer(benchType, srvID, servers, *withLogger, *memProfile, *local)
	} else {
		runBenchmark(benchType, clients, *throughput, *numClients, *clientBasePort, *steps, *runs, *dur, *local, servers, *memProfile, *withLogger, runType)
	}
}

func runBenchmark(name string, clients ServerEntry, throughput, numClients, clientBasePort, steps, runs, dur int, local bool, srvAddrs map[int]Server, memProfile, withLogger bool, runType bench.RunType) {
	options := []bench.RunOption{bench.WithRunType(runType)}
	if withLogger {
		file, err := os.Create("./logs/log.Clients.json")
		if err != nil {
			panic(err)
		}
		loggerOpts := &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		}
		handler := slog.NewJSONHandler(file, loggerOpts)
		logger := slog.New(handler)
		options = append(options, bench.WithLogger(logger))
	}
	var srvAddresses []string
	if !local {
		options = append(options, bench.RunExternal())
		if srvAddrs == nil {
			panic("srvAddrs cannot be nil when not running locally")
		}
		srvAddresses = make([]string, len(srvAddrs)+1)
		for _, srv := range srvAddrs {
			srvAddresses[srv.ID] = fmt.Sprintf("%s:%s", srv.Addr, srv.Port)
		}
		options = append(options, bench.WithSrvAddrs(srvAddresses))
	}
	if numClients > 0 {
		options = append(options, bench.NumClients(numClients))
	}
	if clientBasePort > 0 {
		options = append(options, bench.ClientBasePort(clientBasePort))
	}
	if throughput > 0 {
		options = append(options, bench.MaxThroughput(throughput))
	}
	if steps > 0 {
		options = append(options, bench.Steps(steps))
	}
	if runs > 0 {
		options = append(options, bench.Runs(runs))
	}
	if dur > 0 {
		options = append(options, bench.Dur(dur))
	}
	if memProfile {
		options = append(options, bench.WithMemProfile())
	}
	// must be placed as the last option because it overwrites numClients
	if clients != nil {
		clientsMap := make(map[int]string, len(clients))
		for id, entry := range clients {
			clientsMap[id] = fmt.Sprintf("%s:%s", entry.Addr, entry.Port)
		}
		options = append(options, bench.WithClients(clientsMap))
	}
	// For simplex, always use the 4-node local addresses in local mode.
	if name == bench.SimplexGorums && local {
		options = append(options, bench.WithSrvAddrs(bench.SimplexAddrs))
	}
	bench.RunBenchmark(name, options...)
}

type BenchmarkServer interface {
	Start(local bool)
	Stop()
}

func runServer(benchType string, id int, srvAddrs map[int]Server, withLogger, memprofile, local bool) {
	fmt.Println("Running server:", benchType)
	// var logger *slog.Logger
	// if withLogger {
	// 	file, err := os.Create(fmt.Sprintf("./logs/log.%s:%s.json", srvAddrs[id].Addr, srvAddrs[id].Port))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	loggerOpts := &slog.HandlerOptions{
	// 		AddSource: true,
	// 		Level:     slog.LevelDebug,
	// 	}
	// 	handler := slog.NewJSONHandler(file, loggerOpts)
	// 	logger = slog.New(handler)
	// }
	srvAddresses := make(map[int]string, len(srvAddrs))
	for _, srv := range srvAddrs {
		srvAddresses[srv.ID] = fmt.Sprintf("%s:%s", srv.Addr, srv.Port)
	}
	var srv BenchmarkServer
	switch benchType {
	case bench.PBFTGorumsNew:
		srv = pbftGorumsNew.New(uint32(id), srvAddresses[id], srvAddresses)
	case bench.SimplexGorums:
		simplexGorumsNew.InitKeys(len(srvAddrs))
		srv = simplexGorumsNew.New(uint32(id), srvAddresses[id], srvAddresses)
	}
	if memprofile {
		runtime.GC()
		cpuProfile, err := os.Create(fmt.Sprintf("cpuprofile.%v", id))
		if err != nil {
			panic(err)
		}
		memProfile, err := os.Create(fmt.Sprintf("memprofile.%v", id))
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(cpuProfile)
		defer pprof.StopCPUProfile()
		defer pprof.WriteHeapProfile(memProfile)
	}
	if local {
		srv.Start(true)
		fmt.Println("Press any key to stop server")
		fmt.Scanln()
		//srv.Stop()
		return
	}
	srv.Start(false)
}
