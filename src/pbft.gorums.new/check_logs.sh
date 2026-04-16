#!/bin/bash

# Usage: ./analyze_logs.sh [num_nodes]
# Default to 7 nodes if no argument is provided
NUM_NODES=${1:-7}

for i in $(seq 1 $NUM_NODES); do
    LOG_FILE="node-$i.log"
    
    if [ ! -f "$LOG_FILE" ]; then
        echo "Warning: $LOG_FILE not found, skipping..."
        continue
    fi

    echo "============================================================"
    echo "  NODE $i ANALYSIS"
    echo "============================================================"

    client_recv=$(grep -c 'CLIENT-REQUEST' "$LOG_FILE")

    # ── Pre-Prepare ────────────────────────────────────────────────
    pp_sent=$(grep -c 'preprepare sent' "$LOG_FILE")
    pp_recv=$(grep -c 'PRE-PREPARE' "$LOG_FILE")

    # ── Prepare ────────────────────────────────────────────────────
    prep_sent=$(grep -c 'prepare sent' "$LOG_FILE")
    prep_recv=$(grep -c 'prepare received' "$LOG_FILE")

    # ── Commit ─────────────────────────────────────────────────────
    commit_sent=$(grep -c 'commit sent' "$LOG_FILE")
    commit_recv=$(grep -c 'commit received' "$LOG_FILE")

    # ── Result & Errors ────────────────────────────────────────────
    executed=$(grep -c 'committed.*total_committed' "$LOG_FILE")
    send_errors=$(grep -c 'send error' "$LOG_FILE")
    ctx_timeouts=$(grep -c -E 'ctx\.Done|ClientRequest timeout' "$LOG_FILE")

    # ── Output Summary ─────────────────────────────────────────────
    printf "  Client requests received:           %6s\n" "$client_recv"
    echo   "  ------------------------------------------------------------"
    printf "  PrePrepare  sent (primary):         %6s\n" "$pp_sent"
    printf "  PrePrepare  received (backup):      %6s\n" "$pp_recv"
    echo   "  ------------------------------------------------------------"
    printf "  Prepare     sent:                   %6s\n" "$prep_sent"
    printf "  Prepare     received:               %6s (total)\n" "$prep_recv"
    
    # Dynamic "Prepare from" breakdown
    for j in $(seq 1 $NUM_NODES); do
        count=$(grep 'prepare received' "$LOG_FILE" | grep -c "from=$j")
        if [ "$count" -gt 0 ]; then
            printf "              from node %d:            %6s\n" "$j" "$count"
        fi
    done

    echo   "  ------------------------------------------------------------"
    printf "  Commit      sent:                   %6s\n" "$commit_sent"
    printf "  Commit      received:               %6s (total)\n" "$commit_recv"
    
    # Dynamic "Commit from" breakdown
    for j in $(seq 1 $NUM_NODES); do
        count=$(grep 'commit received' "$LOG_FILE" | grep -c "from=$j")
        if [ "$count" -gt 0 ]; then
            printf "              from node %d:            %6s\n" "$j" "$count"
        fi
    done

    echo   "  ============================================================"
    printf "  COMMITTED (executed):               %6s\n" "$executed"
    printf "  Send errors:                        %6s\n" "$send_errors"
    printf "  Context timeouts:                   %6s\n" "$ctx_timeouts"
    echo ""
done
