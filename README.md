# PBFT Gorums Benchmark

Start by cd into src
```bash
cd ./src
```

## Setup

```bash
make proto     # regenerate protobuf files
make build     # build the pbft binary
```

## Start Servers
In a new terminal, move into pbft folder
```bash
cd pbft.gorums.new
```
Run start script for the servers
```bash
bash start.sh
```
This starts all 4 nodes. Add `--verbose` inside `start.sh` to enable debug logging.

Servers can also be started induvidually 
```bash
go run . server --id <1-4>
```


## Run Benchmark
From ./src
```bash
BENCH=6 go run . --run 6 --throughput 1000 --steps 10 --dur 10
```

| Flag | Description |
|------|-------------|
| `--throughput` | Target max req/s |
| `--steps` | Number of steps in the sweep |
| `--dur` | Seconds per step |

Results are written to `PBFT.Gorums.New.R0.csv` in the current directory.
