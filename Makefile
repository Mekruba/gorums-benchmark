.PHONY: proto docker clean eval setup local-up local-down simplex-server simplex-client simplex-down deploy stop


# --- NETWORK SETUP ---
# For local testing
network:
	docker network create --gateway 10.0.0.1 --subnet 10.0.0.0/16 MasterLab

# For Swarm testing (Run once on Manager)
setup:
	@if ! docker info | grep -q "Swarm: active"; then \
		docker swarm init --advertise-addr $(shell hostname -I | awk '{print $$1}'); \
	fi
	docker network create --driver overlay --attachable --subnet 10.0.0.0/16 MasterLab

# --- LOCAL EXECUTION ---
docker:
	docker compose up --build

local-up:
ifndef BENCH
	$(error BENCH is not set. Example: make local-up BENCH=7 THROUGHPUT=1000 STEPS=1 RUNS=1 DUR=10)
endif
	CONF=local docker compose up --build

local-down:
	docker compose down
# --- DISTRIBUTED EXECUTION (Swarm) ---
deploy:
	docker stack deploy -c docker-compose.yml bench_stack

stop:
	docker stack rm bench_stack

# --- SIMPLEX DISTRIBUTED EXECUTION (non-Swarm, host network) ---
# On each VM:    make simplex-server ID=<n>   (e.g. make simplex-server ID=1)
# On the client: make simplex-client
simplex-server:
ifndef ID
	$(error ID is not set. Example: make simplex-server ID=1)
endif
	docker compose -f docker-compose.simplex.yml up server --build
simplex-client:
	docker compose -f docker-compose.simplex.yml up client --build

simplex-down:
	docker compose -f docker-compose.simplex.yml down
eval:
	docker compose down
	docker compose up --build

# --- UTILS ---
wd := $(shell pwd)
csv_path := $(wd)/csv

histogram:
	@cd src/util/; go run util.go -num=$(NUM) -bench=$(BENCH) -path=$(csv_path) -t=$(T)

clean:
	docker system prune -f
	rm -rf ./logs/*.log
	rm -rf ./csv/*.csv
