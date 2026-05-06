.PHONY: proto docker clean network \
        local-up local-down \
        dist-srv dist-client \
        pbft-srv pbft-client \
        paxos-srv paxos-client \
        simplex-srv simplex-client \
        gen-compose gen-srv gen-client \
        histogram

# --- NETWORK SETUP ---
# For local testing (bridge network)
network:
	docker network create --gateway 10.0.0.1 --subnet 10.0.0.0/16 MasterLab

# --- LOCAL EXECUTION (all-in-one, docker-compose.local.yml) ---
# Example: make local-up BENCH=6 THROUGHPUT=1000 STEPS=10 RUNS=1 DUR=10
local-up:
ifndef BENCH
	$(error BENCH is not set. Example: make local-up BENCH=6 THROUGHPUT=1000 STEPS=1 RUNS=1 DUR=10)
endif
	mkdir -p csv logs
	docker compose -f docker-compose.local.yml up --build

local-down:
	docker compose -f docker-compose.local.yml down

# --- GENERIC DISTRIBUTED EXECUTION (host network) ---
# Set BENCH, then run on each VM: make dist-srv ID=1
# Run on client node after all servers are up: make dist-client
dist-srv:
ifndef ID
	$(error ID is not set. Example: make dist-srv ID=1)
endif
	docker compose up srv$(ID) --build

dist-client:
	mkdir -p csv logs
	docker compose up client --build --no-deps

# --- PBFT (BENCH=6) ---
# On each node: make pbft-srv ID=<1..4>
# On client:    THROUGHPUT=1000 STEPS=10 RUNS=1 DUR=10 make pbft-client
pbft-srv:
ifndef ID
	$(error ID is not set. Example: make pbft-srv ID=1)
endif
	BENCH=6 docker compose up srv$(ID) --build

pbft-client:
	mkdir -p csv logs
	BENCH=6 docker compose up client --build --no-deps

# --- PAXOS ATA (BENCH=7) ---
# On each node: make paxos-srv ID=<1..7>
# On client:    THROUGHPUT=1000 STEPS=10 RUNS=1 DUR=10 make paxos-client
paxos-srv:
ifndef ID
	$(error ID is not set. Example: make paxos-srv ID=1)
endif
	BENCH=7 docker compose up srv$(ID) --build

paxos-client:
	mkdir -p csv logs
	BENCH=7 docker compose up client --build --no-deps

# --- SIMPLEX (BENCH=8) ---
# On each node: make simplex-srv ID=<1..7>
# On client:    THROUGHPUT=1000 STEPS=10 RUNS=1 DUR=10 make simplex-client
simplex-srv:
ifndef ID
	$(error ID is not set. Example: make simplex-srv ID=1)
endif
	BENCH=8 docker compose up srv$(ID) --build

simplex-client:
	mkdir -p csv logs
	BENCH=8 docker compose up client --build --no-deps

# --- DYNAMIC COMPOSE GENERATION ---
# Generate docker-compose with N servers:  make gen-compose N=7 MODE=paxos
# Then use: docker compose -f docker-compose.generated.yml up srv$(ID) --build
N ?= 7
MODE ?= paxos
gen-compose:
	./generate-compose.sh $(N) $(MODE)

gen-srv:
ifndef ID
	$(error ID is not set. Example: make gen-srv ID=1)
endif
	docker compose -f docker-compose.generated.yml up srv$(ID) --build

gen-client:
	mkdir -p csv logs
	docker compose -f docker-compose.generated.yml up client --build --no-deps

# --- UTILS ---
wd := $(shell pwd)
csv_path := $(wd)/csv

histogram:
	@cd src/util/; go run util.go -num=$(NUM) -bench=$(BENCH) -path=$(csv_path) -t=$(T)

clean:
	docker system prune -f
	rm -rf ./logs/*.log
	rm -rf ./csv/*.csv
