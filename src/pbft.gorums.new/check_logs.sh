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
    # ── Checkpoint ─────────────────────────────────────────────────
    ckpt_sent=$(grep -c 'checkpoint sent' "$LOG_FILE")
    ckpt_recv=$(grep -c 'checkpoint received' "$LOG_FILE")
    ckpt_stable=$(grep -c 'checkpoint stable' "$LOG_FILE")
    ckpt_self=$(grep -c 'checkpoint stable (self-vote)' "$LOG_FILE")
    # Water mark info (last seen values)
    last_hwm=$(grep -oP 'high_wm=\K[0-9]+' "$LOG_FILE" | tail -1)
    last_lwm=$(grep -oP 'low_wm=\K[0-9]+' "$LOG_FILE" | tail -1)
    last_gc=$(grep -oP 'gc_entries=\K[0-9]+' "$LOG_FILE" | tail -1)
    # Backoff (primary stalling on high water mark)
    backoff_count=$(grep -c 'above high water mark' "$LOG_FILE")
    wm_outside=$(grep -c 'outside water marks' "$LOG_FILE")
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
    echo   "  ------------------------------------------------------------"
    echo   "  CHECKPOINTS"
    echo   "  ------------------------------------------------------------"
    printf "  Checkpoint  sent:                   %6s\n" "$ckpt_sent"
    printf "  Checkpoint  received:               %6s\n" "$ckpt_recv"
    printf "  Checkpoint  stable (from peer):     %6s\n" "$ckpt_stable"
    printf "  Checkpoint  stable (self-vote):     %6s\n" "$ckpt_self"
    printf "  Last low water mark:                %6s\n" "${last_lwm:---}"
    printf "  Last high water mark:               %6s\n" "${last_hwm:---}"
    printf "  Last GC entries freed:              %6s\n" "${last_gc:---}"
    printf "  Primary backoff (hwm stalls):       %6s\n" "$backoff_count"
    printf "  Msgs rejected (outside wm):         %6s\n" "$wm_outside"

    # Show which seqs triggered checkpoints
    ckpt_seqs=$(grep -oP 'checkpoint sent.*?seq=\K[0-9]+' "$LOG_FILE" | sort -n | uniq | tr '\n' ' ')
    if [ -n "$ckpt_seqs" ]; then
        printf "  Checkpoint seqs sent:          %s\n" "$ckpt_seqs"
    fi
    stable_seqs=$(grep -oP 'checkpoint stable.*?seq=\K[0-9]+' "$LOG_FILE" | sort -n | uniq | tr '\n' ' ')
    if [ -n "$stable_seqs" ]; then
        printf "  Checkpoint seqs stable:        %s\n" "$stable_seqs"
    fi

    # Show per-checkpoint-seq vote counts (how many received per seq)
    ckpt_recv_seqs=$(grep -oP 'checkpoint received.*?seq=\K[0-9]+' "$LOG_FILE" | sort -n | uniq -c | sort -rn)
    if [ -n "$ckpt_recv_seqs" ]; then
        echo   "  Checkpoint votes received per seq:"
        echo "$ckpt_recv_seqs" | while read count seq; do
            printf "              seq %-6s:             %6s votes\n" "$seq" "$count"
        done
    fi

    echo   "  ============================================================"
    printf "  COMMITTED (executed):               %6s\n" "$executed"
    printf "  Send errors:                        %6s\n" "$send_errors"
    printf "  Context timeouts:                   %6s\n" "$ctx_timeouts"
    echo ""
done
