#!/bin/bash

# Maps to pbft.go log messages:
#   "CLIENT-REQUEST"    → ClientRequest handler entered (request received from client)
#   "preprepare sent"   → primary's runPrimary() multicasted a PrePrepare
#   "PRE-PREPARE"       → backup's PrePrepare handler invoked (received PrePrepare)
#   "prepare sent"      → node multicasted a Prepare  (inside PrePrepare handler)
#   "prepare received"  → node's Prepare handler invoked (received Prepare from peer)
#   "commit sent"       → node multicasted a Commit   (inside Prepare handler)
#   "commit received"   → node's Commit handler invoked (received Commit from peer)
#   "committed"         → deliver() executed the request

for i in 1 2 3 4; do
    echo "============================================================"
    echo "  NODE $i"
    echo "============================================================"

    client_recv=$(grep 'CLIENT-REQUEST' node-$i.log 2>/dev/null | wc -l | tr -d ' ')

    # ── Pre-Prepare ────────────────────────────────────────────────
    pp_sent=$(grep 'preprepare sent' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    pp_recv=$(grep 'PRE-PREPARE' node-$i.log 2>/dev/null | wc -l | tr -d ' ')

    # ── Prepare ────────────────────────────────────────────────────
    prep_sent=$(grep 'prepare sent' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    prep_recv=$(grep 'prepare received' node-$i.log 2>/dev/null | wc -l | tr -d ' ')

    prep_from1=$(grep 'prepare received' node-$i.log 2>/dev/null | grep 'from=1' | wc -l | tr -d ' ')
    prep_from2=$(grep 'prepare received' node-$i.log 2>/dev/null | grep 'from=2' | wc -l | tr -d ' ')
    prep_from3=$(grep 'prepare received' node-$i.log 2>/dev/null | grep 'from=3' | wc -l | tr -d ' ')
    prep_from4=$(grep 'prepare received' node-$i.log 2>/dev/null | grep 'from=4' | wc -l | tr -d ' ')

    # ── Commit ─────────────────────────────────────────────────────
    commit_sent=$(grep 'commit sent' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    commit_recv=$(grep 'commit received' node-$i.log 2>/dev/null | wc -l | tr -d ' ')

    commit_from1=$(grep 'commit received' node-$i.log 2>/dev/null | grep 'from=1' | wc -l | tr -d ' ')
    commit_from2=$(grep 'commit received' node-$i.log 2>/dev/null | grep 'from=2' | wc -l | tr -d ' ')
    commit_from3=$(grep 'commit received' node-$i.log 2>/dev/null | grep 'from=3' | wc -l | tr -d ' ')
    commit_from4=$(grep 'commit received' node-$i.log 2>/dev/null | grep 'from=4' | wc -l | tr -d ' ')

    # ── Result ─────────────────────────────────────────────────────
    executed=$(grep 'committed' node-$i.log 2>/dev/null | grep 'total_committed' | wc -l | tr -d ' ')

    # ── Errors ─────────────────────────────────────────────────────
    send_errors=$(grep 'send error' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    ctx_timeouts=$(grep -c -E 'ctx\.Done|ClientRequest timeout' node-$i.log 2>/dev/null | tr -d ' ')

    # ── Output ─────────────────────────────────────────────────────
    printf "  Client requests received:          %6s\n" "$client_recv"
    echo   "  ------------------------------------------------------------"
    printf "  PrePrepare  sent (multicast out):   %6s\n" "$pp_sent"
    printf "  PrePrepare  received (handler in):  %6s\n" "$pp_recv"
    echo   "  ------------------------------------------------------------"
    printf "  Prepare     sent (multicast out):   %6s\n" "$prep_sent"
    printf "  Prepare     received (handler in):  %6s  (total)\n" "$prep_recv"
    printf "              from node 1:            %6s\n" "$prep_from1"
    printf "              from node 2:            %6s\n" "$prep_from2"
    printf "              from node 3:            %6s\n" "$prep_from3"
    printf "              from node 4:            %6s\n" "$prep_from4"
    echo   "  ------------------------------------------------------------"
    printf "  Commit      sent (multicast out):   %6s\n" "$commit_sent"
    printf "  Commit      received (handler in):  %6s  (total)\n" "$commit_recv"
    printf "              from node 1:            %6s\n" "$commit_from1"
    printf "              from node 2:            %6s\n" "$commit_from2"
    printf "              from node 3:            %6s\n" "$commit_from3"
    printf "              from node 4:            %6s\n" "$commit_from4"
    echo   "  ============================================================"
    printf "  COMMITTED (executed):               %6s\n" "$executed"
    printf "  Send errors:                        %6s\n" "$send_errors"
    printf "  Context timeouts (ctx.Done):        %6s\n" "$ctx_timeouts"
    echo ""
done
