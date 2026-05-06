#!/usr/bin/env bash
#
# Generate docker-compose.yml dynamically based on the number of servers.
#
# Usage:
#   ./generate-compose.sh              # uses NUM_SERVERS from .env (default 7)
#   ./generate-compose.sh 3            # override with 3 servers
#   ./generate-compose.sh 3 paxos      # 3 servers, compose for distributed (paxos) mode
#   ./generate-compose.sh 3 local      # 3 servers, compose for local mode
#
# Outputs:
#   docker-compose.generated.yml

set -euo pipefail

NUM_SERVERS=${1:-${NUM_SERVERS:-7}}
MODE=${2:-paxos}  # "paxos" (distributed/host network) or "local"
OUTPUT="docker-compose.generated.yml"

echo "Generating ${OUTPUT} with ${NUM_SERVERS} servers (mode: ${MODE})..."

cat > "${OUTPUT}" <<'HEADER'
# Auto-generated — do not edit manually.
# Regenerate with: ./generate-compose.sh <num_servers> [paxos|local]
services:
HEADER

for i in $(seq 1 "${NUM_SERVERS}"); do
  if [[ "${MODE}" == "local" ]]; then
    PORT=$((8079 + i))  # 8080, 8081, ...
    cat >> "${OUTPUT}" <<EOF
  srv${i}:
    build:
      context: .
      dockerfile: src/Dockerfile.Server
    network_mode: "host"
    environment:
      - ID=${i}
      - SERVER=1
      - CONF=local
      - PRODUCTION=\${PRODUCTION:-1}
      - BENCH=\${BENCH}
      - LOG=\${LOG:-0}
    volumes:
      - ./logs:/app/logs
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "${PORT}"]
      interval: 2s
      timeout: 2s
      retries: 15
      start_period: 5s

EOF
  else
    cat >> "${OUTPUT}" <<EOF
  srv${i}:
    image: my-bench-server:latest
    build:
      context: .
      dockerfile: src/Dockerfile.Server
    network_mode: "host"
    environment:
      - ID=${i}
      - SERVER=1
      - CONF=\${CONF}
      - BENCH=\${BENCH}
      - LOG=\${LOG}
    volumes:
      - ./logs:/app/logs

EOF
  fi
done

# --- Client service ---
if [[ "${MODE}" == "local" ]]; then
  cat >> "${OUTPUT}" <<'EOF'
  client:
    build:
      context: .
      dockerfile: src/Dockerfile.Client
    network_mode: "host"
    environment:
      - SERVER=0
      - CONF=local
      - PRODUCTION=${PRODUCTION:-1}
      - BENCH=${BENCH}
      - THROUGHPUT=${THROUGHPUT}
      - STEPS=${STEPS}
      - RUNS=${RUNS}
      - DUR=${DUR}
      - LOG=${LOG:-0}
    volumes:
      - ./logs:/app/logs
      - ./csv:/app/src/csv
    depends_on:
EOF
  for i in $(seq 1 "${NUM_SERVERS}"); do
    cat >> "${OUTPUT}" <<EOF
      srv${i}:
        condition: service_healthy
EOF
  done
else
  cat >> "${OUTPUT}" <<'EOF'
  client:
    image: my-bench-client:latest
    build:
      context: .
      dockerfile: src/Dockerfile.Client
    network_mode: "host"
    environment:
      - SERVER=0
      - CONF=${CONF}
      - BENCH=${BENCH}
      - THROUGHPUT=${THROUGHPUT}
      - STEPS=${STEPS}
      - RUNS=${RUNS}
      - DUR=${DUR}
      - LOG=${LOG}
    volumes:
      - ./logs:/app/logs
      - ./csv:/app/src/csv
EOF
fi

echo "Generated ${OUTPUT} with ${NUM_SERVERS} server(s) + 1 client."
