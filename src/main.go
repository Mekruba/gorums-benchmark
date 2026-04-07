package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"syscall"

	bench "github.com/Mekruba/gorums-benchmark/benchmark"
	paxosataServer "github.com/Mekruba/gorums-benchmark/paxos.ata/server"
	pbftGorumsNew "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
	"github.com/joho/godotenv"
)

func printUsage() {
	fmt.Println("Usage: go run . [flags]")
	fmt.Println("")
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Println("")
	fmt.Println("Benchmark types (--run):")
	fmt.Println("  6  PBFT.Gorums.New")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  # Run benchmark")
	fmt.Println("  BENCH=6 go run . --run 6 --throughput 1000 --steps 10 --dur 10")
	fmt.Println("")
	fmt.Println("  # Run as server (node 2)")
	fmt.Println("  go run . --server --run 6 --id 2 --local")
	fmt.Println("")
	fmt.Println("Environment variables (override flags):")
	fmt.Println("  BENCH       benchmark type index")
	fmt.Println("  ID          server node ID")
	fmt.Println("  SERVER=1    run as server")
	fmt.Println("  LOG=1       enable structured logging")
	fmt.Println("  THROUGHPUT  target throughput (req/s)")
	fmt.Println("  STEPS       number of throughput steps")
	fmt.Println("  DUR         seconds per step")
	fmt.Println("  RUNS        number of benchmark runs")
	fmt.Println("  TYPE        run type (0=Throughput, 1=Performance)")
}

func main() {
	id := flag.Int("id", -1, "Server node ID")
	runSrv := flag.Bool("server", false, "Run as server node")
	benchTypeIndex := flag.Int("run", 0, "Benchmark type index (use --help to see options)")
	memProfile := flag.Bool("mem", false, "Create memory and CPU profiles")
	numClients := flag.Int("clients", 0, "Number of clients")
	clientBasePort := flag.Int("port", 0, "Base port for clients")
	withLogger := flag.Bool("log", false, "Enable structured JSON logger")
	local := flag.Bool("local", false, "Run servers locally")
	throughput := flag.Int("throughput", 0, "Target throughput (req/s)")
	runs := flag.Int("runs", 0, "Number of runs per benchmark")
	steps := flag.Int("steps", 0, "Number of throughput steps")
	dur := flag.Int("dur", 0, "Seconds per throughput step")
	help := flag.Bool("help", false, "Show help")

	flag.Usage = printUsage
	flag.Parse()

	if *help {
		printUsage()
		os.Exit(0)
	}

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
	if v := os.Getenv("THROUGHPUT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid THROUGHPUT=%q: %v\n", v, err)
			os.Exit(1)
		}
		*throughput = n
	}
	if v := os.Getenv("STEPS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid STEPS=%q: %v\n", v, err)
			os.Exit(1)
		}
		*steps = n
	}
	if v := os.Getenv("DUR"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid DUR=%q: %v\n", v, err)
			os.Exit(1)
		}
		*dur = n
	}
	if v := os.Getenv("RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid RUNS=%q: %v\n", v, err)
			os.Exit(1)
		}
		*runs = n
	}

	srvID := *id
	if srvID < 0 && *runSrv {
		n, err := strconv.Atoi(os.Getenv("ID"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error: server mode requires --id or ID env var")
			fmt.Fprintln(os.Stderr, "")
			printUsage()
			os.Exit(1)
		}
		srvID = n
	}

	runType := bench.Throughput
	if v := os.Getenv("TYPE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid TYPE=%q: %v\n", v, err)
			os.Exit(1)
		}
		runType = bench.RunType(n)
	}

	bT := *benchTypeIndex
	if bT <= 0 {
		v := os.Getenv("BENCH")
		if v == "" {
			fmt.Fprintln(os.Stderr, "Error: --run or BENCH env var is required")
			fmt.Fprintln(os.Stderr, "")
			printUsage()
			os.Exit(1)
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid BENCH=%q: %v\n", v, err)
			os.Exit(1)
		}
		bT = n
	}
	benchType, ok := mapping[bT]
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: unknown benchmark type %d\n", bT)
		fmt.Fprintln(os.Stderr, "")
		printUsage()
		os.Exit(1)
	}

	if *runSrv {
		if srvID > len(servers) {
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
			fmt.Fprintf(os.Stderr, "Error: failed to create log file: %v\n", err)
			os.Exit(1)
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
			fmt.Fprintln(os.Stderr, "Error: srvAddrs cannot be nil when not running locally")
			os.Exit(1)
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
	if clients != nil {
		clientsMap := make(map[int]string, len(clients))
		for id, entry := range clients {
			clientsMap[id] = fmt.Sprintf("%s:%s", entry.Addr, entry.Port)
		}
		options = append(options, bench.WithClients(clientsMap))
	}
	bench.RunBenchmark(name, options...)
}

type BenchmarkServer interface {
	Start(local bool)
	Stop()
}

func runServer(benchType string, id int, srvAddrs map[int]Server, withLogger, memprofile, local bool) {
	fmt.Println("Running server:", benchType)
	srvAddresses := make(map[int]string, len(srvAddrs))
	for _, srv := range srvAddrs {
		srvAddresses[srv.ID] = fmt.Sprintf("%s:%s", srv.Addr, srv.Port)
	}
	var srv BenchmarkServer
	switch benchType {
	case bench.PBFTGorumsNew:
		srv = pbftGorumsNew.New(uint32(id), srvAddresses)
	case bench.PaxosATA:
		addrs := make([]string, len(srvAddrs))
		for _, s := range srvAddrs {
			addrs[s.ID] = fmt.Sprintf("%s:%s", s.Addr, s.Port)
		}
		srv = paxosataServer.New(srvAddresses[id], addrs)
	default:
		fmt.Fprintf(os.Stderr, "Error: no server implementation for benchmark type %q\n", benchType)
		os.Exit(1)
	}
	if memprofile {
		runtime.GC()
		cpuProfile, err := os.Create(fmt.Sprintf("cpuprofile.%v", id))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to create cpu profile: %v\n", err)
			os.Exit(1)
		}
		memProfile, err := os.Create(fmt.Sprintf("memprofile.%v", id))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to create mem profile: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(cpuProfile)
		defer pprof.StopCPUProfile()
		defer pprof.WriteHeapProfile(memProfile)
	}
	if local {
		srv.Start(true)
		fmt.Println("Press any key to stop server")
		fmt.Scanln()
		return
	}
	// DOCKER MODE
	srv.Start(false)

	// Block the process so the container stays "Up"
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	slog.Info("Server is up and blocking", "id", id, "bind", srvAddresses[id])
	<-stop

	srv.Stop()
}
