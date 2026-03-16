package bench

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"time"
)

type ClientResult struct {
	Id              string
	Total           uint64
	Successes       uint64
	Errs            uint64
	LatencyAvg      time.Duration
	LatencyMedian   time.Duration
	LatencyMin      time.Duration
	LatencyMax      time.Duration
	LatencyStdev    float64
	TotalDur        time.Duration
	Throughput      float64
	ReqDistribution []uint64 // shows how many request in transit per unit of time
	BucketSize      int      // number of microseconds
	Durations       []time.Duration
}

type Result struct {
	Id                    string
	TotalNum              uint64
	FinishedReqsTotal     uint64
	FinishedReqsSuccesful uint64
	FinishedReqsFailed    uint64
	Processed             uint64
	Dropped               uint64
	Invalid               uint64
	AlreadyProcessed      uint64
	RoundTripLatencyAvg   time.Duration
	RoundTripLatencyMin   time.Duration
	RoundTripLatencyMax   time.Duration
	ReqLatencyAvg         time.Duration
	ReqLatencyMin         time.Duration
	ReqLatencyMax         time.Duration
	ShardDistribution     map[uint32]uint64
}

type RequestResult struct {
	err   error
	start time.Time
	end   time.Time
}

type Benchmark[S, C any] interface {
	CreateServer(addr string, peers []string) (*S, func(), error)
	Init(opts RunOptions)
	AddClient(id int, addr string, srvAddrs []string, logger *slog.Logger)
	Clients() []*C
	Config() *C
	Stop()
	StartBenchmark(config *C) []Result
	Run(client *C, ctx context.Context, payload int) error
	StopBenchmark(config *C) []Result
}

type RunType int

const (
	Throughput RunType = iota
	Performance
	Sync
	Async
	Random
)

func (r RunType) String() string {
	res := ""
	switch r {
	case Sync:
		res = "Sync"
	case Async:
		res = "Async"
	case Random:
		res = "Random"
	case Throughput:
		res = "Throughput"
	case Performance:
		res = "Performance"
	}
	return res
}

type benchmarkOption struct {
	name           string
	srvAddrs       []string
	numClients     int
	clientBasePort int
	quorumSize     int
	numRequests    int
	timeout        time.Duration
	reqInterval    struct{ start, end int } // reqs will be sent in the interval: [start, end] µs
	local          bool
	runType        RunType
	memProfile     bool
	dur            int // specifies how long the throughput benchmark should be run in seconds
	runs           int // number of times a benchmark should run. Will create separate run file for each benchmark
	steps          int // number of steps used in throughput vs latency benchmark: throughputIncrement
	logger         *slog.Logger
	clients        map[int]string
}

type RunOptions struct {
	local               bool
	throughputMax       int
	throughputIncrement int
	srvAddrs            []string
	numClients          int
	clientBasePort      int
	memProfile          bool
	dur                 int
	runs                int
	steps               int
	logger              *slog.Logger
	clients             map[int]string
	runType             RunType
}

func RunExternal() RunOption {
	return func(o *RunOptions) {
		o.local = false
	}
}

func MaxThroughput(max int) RunOption {
	return func(o *RunOptions) {
		o.throughputMax = max
	}
}

func ThroughputIncr(incr int) RunOption {
	return func(o *RunOptions) {
		o.throughputIncrement = incr
	}
}

func WithSrvAddrs(srvAddrs []string) RunOption {
	return func(o *RunOptions) {
		o.srvAddrs = srvAddrs
	}
}

func NumClients(numClients int) RunOption {
	return func(o *RunOptions) {
		o.numClients = numClients
	}
}

func ClientBasePort(basePort int) RunOption {
	return func(o *RunOptions) {
		o.clientBasePort = basePort
	}
}

func WithRunType(r RunType) RunOption {
	return func(o *RunOptions) {
		o.runType = r
	}
}

func WithClients(clients map[int]string) RunOption {
	return func(o *RunOptions) {
		o.numClients = len(clients)
		o.clients = clients
	}
}

func WithMemProfile() RunOption {
	return func(o *RunOptions) {
		o.memProfile = true
	}
}

func WithLogger(logger *slog.Logger) RunOption {
	return func(o *RunOptions) {
		o.logger = logger
	}
}

func Dur(dur int) RunOption {
	return func(o *RunOptions) {
		o.dur = dur
	}
}

func Steps(steps int) RunOption {
	return func(o *RunOptions) {
		o.steps = steps
	}
}

func Runs(runs int) RunOption {
	return func(o *RunOptions) {
		o.runs = runs
	}
}

type RunOption func(*RunOptions)

func RunBenchmark(name string, options ...RunOption) {
	opts := RunOptions{
		local:          true,
		srvAddrs:       threeServers,
		throughputMax:  15000,
		numClients:     10,
		clientBasePort: 8080,
		dur:            10,
		runs:           1,
		steps:          10,
		clients:        nil,
		runType:        Throughput,
	}
	for _, opt := range options {
		opt(&opts)
	}
	benchmark, ok := benchTypes[name]
	if !ok {
		return
	}
	if opts.clients != nil {
		opts.numClients = len(opts.clients)
	}
	state := benchmark.init()
	state.Init(opts)
	for i := 0; i < opts.runs; i++ {
		switch opts.runType {
		case Throughput:
			runThroughputVsLatencyBenchmark(benchmark, state, name, i, opts)
		case Performance:
			runPerformanceBenchmark(benchmark, state, name, i, opts)
		}
	}
}

func runThroughputVsLatencyBenchmark(benchmark benchStruct, benchmarkState initializable, name string, runNumber int, opts RunOptions) ([]Result, []error) {
	if opts.throughputIncrement <= 0 {
		opts.throughputIncrement = opts.throughputMax / opts.steps
	}
	fmt.Println("running benchmark:", name)
	results := make([]Result, len(benchmarks))
	errs := make([]error, len(benchmarks))
	throughputVsLatency := make([][]string, 0)
	for target := opts.throughputIncrement; target <= opts.throughputMax; target += opts.throughputIncrement {
		bench := benchmarkOption{
			name:           fmt.Sprintf("%s.S%v.C%v.R%v.Throughput.%v", name, len(opts.srvAddrs), opts.numClients, target, runNumber),
			srvAddrs:       opts.srvAddrs,
			numClients:     opts.numClients,
			clientBasePort: opts.clientBasePort,
			numRequests:    target,
			local:          opts.local,
			runType:        Throughput,
			memProfile:     opts.memProfile,
			dur:            opts.dur,
			runs:           opts.runs,
			steps:          opts.steps,
			logger:         opts.logger,
			clients:        opts.clients,
		}
		start := time.Now()
		clientResult, _, err := benchmark.run(bench, benchmarkState)
		if err != nil {
			panic(err)
		}
		fmt.Println("took:", time.Since(start))
		throughputVsLatency = append(throughputVsLatency, []string{strconv.Itoa(int(clientResult.Throughput)), strconv.Itoa(int(clientResult.LatencyAvg.Milliseconds())), strconv.Itoa(int(clientResult.LatencyMedian.Milliseconds()))})
		err = WriteDurations(fmt.Sprintf("%s.T%v.R%v.durations", name, target, runNumber), clientResult.Durations)
		if err != nil {
			panic(err)
		}
		fmt.Println("done")
		time.Sleep(1 * time.Second)
	}
	err := WriteThroughputVsLatency(fmt.Sprintf("%s.R%v", name, runNumber), throughputVsLatency)
	if err != nil {
		panic(err)
	}
	return results, errs
}

func runPerformanceBenchmark(benchmark benchStruct, benchmarkState initializable, name string, runNumber int, opts RunOptions) ([]Result, []error) {
	if opts.throughputIncrement <= 0 {
		opts.throughputIncrement = opts.throughputMax / opts.steps
	}
	fmt.Println("running benchmark:", name)
	bench := benchmarkOption{
		name:           fmt.Sprintf("%s.S%v.C%v.R%v.Performance.%v", name, len(opts.srvAddrs), opts.numClients, 1000, runNumber),
		srvAddrs:       opts.srvAddrs,
		numClients:     opts.numClients,
		clientBasePort: opts.clientBasePort,
		numRequests:    1000,
		local:          opts.local,
		memProfile:     opts.memProfile,
		dur:            1,
		runs:           opts.runs,
		steps:          opts.steps,
		logger:         opts.logger,
		clients:        opts.clients,
		runType:        Performance,
	}
	start := time.Now()
	clientResult, _, err := benchmark.run(bench, benchmarkState)
	if err != nil {
		panic(err)
	}
	fmt.Println("took:", time.Since(start))
	err = WritePerformance(bench.name, []string{
		strconv.Itoa(int(bench.numRequests)),
		strconv.Itoa(int(clientResult.LatencyAvg.Microseconds())),
		strconv.Itoa(int(clientResult.LatencyMedian.Microseconds())),
		strconv.Itoa(int(clientResult.LatencyStdev)),
		strconv.Itoa(int(clientResult.LatencyMin.Microseconds())),
		strconv.Itoa(int(clientResult.LatencyMax.Microseconds())),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("done")
	time.Sleep(1 * time.Second)
	return nil, nil
}

func runBenchmark[S, C any](opts benchmarkOption, benchmark Benchmark[S, C]) (ClientResult, []Result, error) {
	fmt.Printf("\nRunning benchmark: %s\n\n", opts.name)
	runtime.GC()
	var config *C
	totalNumReqs := opts.numClients * opts.numRequests
	switch opts.runType {
	case Throughput:
		totalNumReqs = opts.numRequests * opts.dur
	case Performance:
		totalNumReqs = opts.numRequests
	default:
	}
	durations := make([]time.Duration, totalNumReqs)
	clientResult := ClientResult{
		Id:    "clients",
		Total: uint64(totalNumReqs),
	}

	if opts.quorumSize <= 0 {
		opts.quorumSize = len(opts.srvAddrs)
	}
	if opts.timeout <= 0 {
		opts.timeout = 15 * time.Second
	}

	clients := benchmark.Clients()
	config = benchmark.Config()

	var servers []*S
	if opts.local {
		fmt.Println("creating servers...")
		servers = make([]*S, len(opts.srvAddrs))
		for i, addr := range opts.srvAddrs {
			var (
				err     error
				cleanup func()
			)
			servers[i], cleanup, err = benchmark.CreateServer(addr, opts.srvAddrs)
			defer cleanup()
			if err != nil {
				return clientResult, nil, err
			}
		}
	}

	resChan := make(chan RequestResult, totalNumReqs)
	fmt.Println("\nstarting benchmark...")

	// start the recording of metrics
	resultBefore := benchmark.StartBenchmark(config)
	// wait for start to finish up
	time.Sleep(1 * time.Second)
	if opts.memProfile {
		cpuProfile, _ := os.Create("cpuprofile")
		memProfile, _ := os.Create("memprofile")
		pprof.StartCPUProfile(cpuProfile)
		defer pprof.StopCPUProfile()
		defer pprof.WriteHeapProfile(memProfile)
	}

	switch opts.runType {
	case Sync:
		go runSync(opts, benchmark, resChan, clients)
	case Async:
		go runAsync(opts, benchmark, resChan, clients)
	case Random:
		go runRandom(opts, benchmark, resChan, clients)
	case Throughput:
		go runThroughput(opts, benchmark, resChan, clients, opts.dur)
	case Performance:
		go runThroughputSync(opts, benchmark, resChan, clients, opts.dur)
	}

	switch opts.runType {
	case Throughput:
		fmt.Println("collecting responses:")
		avgDur := time.Duration(0)
		numFailed := 0
		graceTime := 15 * time.Second
		for i := 0; i < totalNumReqs; i++ {
			if i%(opts.numRequests/2) == 0 {
				fmt.Printf("%v%s done\n", (100 * float64(i) / float64(totalNumReqs)), "%")
			}
			var res RequestResult
			// prevent deadlock
			select {
			case res = <-resChan:
			case <-time.After(graceTime):
				// set graceTime to something small because all reqs should have timed out by now.
				graceTime = 1 * time.Millisecond
				numFailed++
				continue
			}
			if res.err != nil {
				numFailed++
				slog.Info("benchmark:", "replies", i, "total", totalNumReqs, "successes", i-numFailed, "failures", numFailed, "err", res.err)
				continue
			}
			dur := res.end.Sub(res.start)
			avgDur += dur
			durations[i] = dur
		}
		// wait a short time to make the servers finish up before sending a "purge state msg" to them
		time.Sleep(2 * time.Second)
		fmt.Printf("100%s done, numFailed: %v out of %v\n", "%", numFailed, totalNumReqs)
		benchmark.StopBenchmark(config)
		fmt.Println("stopped benchmark...")

		avgDur /= time.Duration(totalNumReqs)
		// sort the durations to calculate median
		// [0, 1, 2, 3, 4, 5]
		slices.Sort(durations)
		var median time.Duration
		if len(durations)%2 == 0 {
			var medianIndex1 int = (len(durations) / 2) - 1 // integer division does floor operation
			var medianIndex2 int = len(durations) / 2       // integer division does floor operation
			median1 := durations[medianIndex1]
			median2 := durations[medianIndex2]
			median = (median1 + median2) / 2
		} else {
			var medianIndex int = len(durations) / 2 // integer division does floor operation
			median = durations[medianIndex]
		}
		clientResult.LatencyAvg = avgDur
		clientResult.LatencyMedian = median
		clientResult.Throughput = float64(opts.numRequests)
		clientResult.Durations = durations
		return clientResult, nil, nil
	case Performance:
		fmt.Println("collecting responses:")
		avgDur := time.Duration(0)
		numFailed := 0
		graceTime := 15 * time.Second
		for i := 0; i < totalNumReqs; i++ {
			if i%(opts.numRequests/2) == 0 {
				fmt.Printf("%v%s done\n", (100 * float64(i) / float64(totalNumReqs)), "%")
			}
			var res RequestResult
			// prevent deadlock
			select {
			case res = <-resChan:
			case <-time.After(graceTime):
				// set graceTime to something small because all reqs should have timed out by now.
				graceTime = 1 * time.Millisecond
				numFailed++
				continue
			}
			if res.err != nil {
				numFailed++
				slog.Info("benchmark:", "replies", i, "total", totalNumReqs, "successes", i-numFailed, "failures", numFailed, "err", res.err)
				continue
			}
			dur := res.end.Sub(res.start)
			avgDur += dur
			durations[i] = dur
		}
		// wait a short time to make the servers finish up before sending a "purge state msg" to them
		time.Sleep(2 * time.Second)
		fmt.Printf("100%s done, numFailed: %v out of %v\n", "%", numFailed, totalNumReqs)
		benchmark.StopBenchmark(config)
		fmt.Println("stopped benchmark...")

		avgDur /= time.Duration(totalNumReqs)
		// sort the durations to calculate median
		// [0, 1, 2, 3, 4, 5]
		slices.Sort(durations)
		var median time.Duration
		if len(durations)%2 == 0 {
			var medianIndex1 int = (len(durations) / 2) - 1 // integer division does floor operation
			var medianIndex2 int = len(durations) / 2       // integer division does floor operation
			median1 := durations[medianIndex1]
			median2 := durations[medianIndex2]
			median = (median1 + median2) / 2
		} else {
			var medianIndex int = len(durations) / 2 // integer division does floor operation
			median = durations[medianIndex]
		}
		clientResult.LatencyAvg = avgDur
		clientResult.LatencyMedian = median
		clientResult.LatencyMin = durations[0]
		clientResult.LatencyMax = durations[len(durations)-1]
		latencyVariance := 0.0
		for _, dur := range durations {
			latencyVariance += math.Pow(float64(dur.Microseconds())-float64(avgDur.Microseconds()), 2)
		}
		latencyVariance /= float64(len(durations) - 1)
		clientResult.LatencyStdev = math.Sqrt(latencyVariance)
		return clientResult, nil, nil
	default:
	}
	numBuckets := 30
	durDistribution := make([]uint64, numBuckets)
	maxDur := time.Duration(0)
	minDur := 100 * time.Hour
	avgDur := time.Duration(0)
	firstReqStart := time.Now().Add(24 * time.Hour)
	lastReqDone := time.Now()
	for i := 0; i < totalNumReqs; i++ {
		if i%(totalNumReqs/10) == 0 {
			fmt.Printf("%v%s done\n", (100 * float64(i) / float64(totalNumReqs)), "%")
		}
		var res RequestResult
		// prevent deadlock
		select {
		case res = <-resChan:
		case <-time.After(30 * time.Second):
			slog.Info("benchmark:", "replies", i, "total", totalNumReqs)
			return clientResult, nil, errors.New("could not collect all responses")
		}

		if res.err != nil {
			clientResult.Errs++
			continue
		} else {
			clientResult.Successes++
		}
		if firstReqStart.Sub(res.start) > 0 {
			firstReqStart = res.start
		}
		if res.end.Sub(lastReqDone) > 0 {
			lastReqDone = res.end
		}
		dur := res.end.Sub(res.start)
		durations[i] = dur
		if dur > maxDur {
			maxDur = dur
		}
		if dur < minDur {
			minDur = dur
		}
		avgDur += dur
	}
	clientResult.TotalDur = lastReqDone.Sub(firstReqStart)
	clientResult.ReqDistribution = durDistribution
	fmt.Printf("100%s done\n", "%")
	// stop the recording and return the metrics
	result := benchmark.StopBenchmark(config)
	fmt.Println("stopped benchmark...")

	fmt.Println("calculating histogram...")
	bucketSize := (maxDur.Milliseconds() - minDur.Milliseconds()) / int64(numBuckets)
	clientResult.BucketSize = int(bucketSize)
	for _, dur := range durations {
		bucket := int(math.Floor(float64(dur.Milliseconds()-minDur.Milliseconds()) / float64(bucketSize)))
		if bucket >= numBuckets {
			bucket = numBuckets - 1
		}
		if bucket <= 0 || bucket >= len(durDistribution) {
			continue
		}
		durDistribution[bucket]++
	}

	avgDur /= time.Duration(opts.numRequests * opts.numClients)
	clientResult.LatencyAvg = avgDur
	clientResult.LatencyMax = maxDur
	clientResult.LatencyMin = minDur
	if result != nil && resultBefore != nil {
		for i := range result {
			result[i].TotalNum -= resultBefore[i].TotalNum
		}
	}
	totalDur := clientResult.TotalDur.Seconds()
	if totalDur <= 0 {
		totalDur = 1
	}
	clientResult.Throughput = float64(clientResult.Total) / totalDur
	return clientResult, result, nil
}

// each client runs synchronously. I.e. waits for response before sending next msg.
func runSync[S, C any](opts benchmarkOption, benchmark Benchmark[S, C], resChan chan RequestResult, clients []*C) {
	for _, client := range clients {
		go func(client *C) {
			for i := 0; i < opts.numRequests; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
				defer cancel()
				start := time.Now()
				err := benchmark.Run(client, ctx, i)
				end := time.Now()
				resChan <- RequestResult{
					err:   err,
					start: start,
					end:   end,
				}
			}
		}(client)
	}
}

// each client runs asynchronously. I.e. sends all requests at once.
func runAsync[S, C any](opts benchmarkOption, benchmark Benchmark[S, C], resChan chan RequestResult, clients []*C) {
	for i := 0; i < opts.numRequests; i++ {
		for _, client := range clients {
			go func(client *C, i int) {
				ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
				defer cancel()
				start := time.Now()
				err := benchmark.Run(client, ctx, i)
				end := time.Now()
				resChan <- RequestResult{
					err:   err,
					start: start,
					end:   end,
				}
			}(client, i)
		}
	}
}

// each client runs synchronously but with an added delay between requests.
func runRandom[S, C any](opts benchmarkOption, benchmark Benchmark[S, C], resChan chan RequestResult, clients []*C) {
	for _, client := range clients {
		go func(client *C) {
			for i := 0; i < opts.numRequests; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
				defer cancel()
				start := time.Now()
				err := benchmark.Run(client, ctx, i)
				end := time.Now()
				resChan <- RequestResult{
					err:   err,
					start: start,
					end:   end,
				}
				t := rand.Intn(opts.reqInterval.end-opts.reqInterval.start) + opts.reqInterval.start
				time.Sleep(time.Duration(t) * time.Microsecond)
			}
		}(client)
	}
}

// each client runs asynchronously. I.e. sends all requests at once.
// runs for dur seconds and sends reqs at the target throughput.
func runThroughput[S, C any](opts benchmarkOption, benchmark Benchmark[S, C], resChan chan RequestResult, clients []*C, dur int) {
	// opts.numRequests denotes the target throughput in reqs/s. We thus
	// need to divide the number of reqs by the number of clients.
	numReqsPerClient := opts.numRequests / len(clients)
	// it will run for dur seconds
	for t := 0; t < dur; t++ {
		for _, client := range clients {
			for i := 0; i < numReqsPerClient; i++ {
				go func(client *C, i int) {
					ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
					defer cancel()
					start := time.Now()
					err := benchmark.Run(client, ctx, i)
					end := time.Now()
					resChan <- RequestResult{
						err:   err,
						start: start,
						end:   end,
					}
				}(client, i)
			}
		}
		time.Sleep(1 * time.Second)
	}
}

// each client runs synchronously. I.e. sends one request at a time.
// runs for dur seconds and sends reqs at target throughput.
func runThroughputSync[S, C any](opts benchmarkOption, benchmark Benchmark[S, C], resChan chan RequestResult, clients []*C, dur int) {
	// opts.numRequests denotes the target throughput in reqs/s. We thus
	// need to divide the number of reqs by the number of clients.
	numReqsPerClient := opts.numRequests / len(clients)
	// it will run for dur seconds
	for t := 0; t < dur; t++ {
		for _, client := range clients {
			go func(client *C) {
				for i := 0; i < numReqsPerClient; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
					defer cancel()
					start := time.Now()
					err := benchmark.Run(client, ctx, i)
					end := time.Now()
					resChan <- RequestResult{
						err:   err,
						start: start,
						end:   end,
					}
				}
			}(client)
		}
		time.Sleep(1 * time.Second)
	}
}
