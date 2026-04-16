#!/bin/bash

# 1. Get configuration from user (Portable prompting)
printf "Number of faulty nodes to tolerate (1 or 2): "
read F_NODES

if [[ "$F_NODES" != "1" && "$F_NODES" != "2" ]]; then
    echo "Error: Only 1 or 2 faulty nodes supported for this setup."
    exit 1
fi

printf "Enable verbose logging? (y/n): "
read VERBOSE_ANS

VERBOSE_FLAG=""
if [[ "$VERBOSE_ANS" == "y" || "$VERBOSE_ANS" == "Y" ]]; then
    VERBOSE_FLAG="--verbose"
fi

# 2. Calculate total nodes (n = 3f + 1)
N_NODES=$((3 * F_NODES + 1))
echo "-------------------------------------------------------"
echo "Starting cluster with $N_NODES nodes (f=$F_NODES)..."

# 3. Start servers
PIDS=()
for i in $(seq 1 $N_NODES); do
    # Run the server in the background
    ./pbft server --id $i $VERBOSE_FLAG &
    PID=$!
    PIDS+=($PID)
    
    # Save the PID so we can target it later
    echo $PID > "/tmp/pbft$i.pid"
    echo "Started Node $i (PID: $PID) on localhost:$((8079 + i))"
done

echo "-------------------------------------------------------"
echo "Cluster is running."
echo "To kill node 2:   kill \$(cat /tmp/pbft2.pid)"
echo "To freeze node 2: kill -STOP \$(cat /tmp/pbft2.pid)"
echo "To resume node 2: kill -CONT \$(cat /tmp/pbft2.pid)"
echo "Press Ctrl+C to stop all nodes."
echo "-------------------------------------------------------"

# 4. Cleanup on exit
trap_cleanup() {
    echo -e "\n\nShutting down $N_NODES nodes..."
    for i in $(seq 1 $N_NODES); do
        PID_FILE="/tmp/pbft$i.pid"
        if [ -f "$PID_FILE" ]; then
            # kill the process group or the specific PID
            kill $(cat "$PID_FILE") 2>/dev/null
            rm "$PID_FILE"
        fi
    done
    exit 0
}

trap trap_cleanup SIGINT SIGTERM

# Wait for all background processes to keep the script alive
wait
