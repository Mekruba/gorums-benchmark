#!/bin/bash

echo "============================================================"
echo "  GORUMS DEBUG ANALYSIS"
echo "============================================================"
echo ""

for i in 1 2 3 4; do
    [ ! -f "node-$i.log" ] && continue

    echo "============================================================"
    echo "  NODE $i"
    echo "============================================================"

    # AfterFunc stream clears (the big one — context cancellation tearing down streams)
    afterfunc_count=$(grep 'GORUMS DEBUG: AfterFunc cleared' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    echo "  AfterFunc stream clears:          $afterfunc_count"

    if [ "$afterfunc_count" -gt 0 ]; then
        echo "  --- AfterFunc details ---"
        # Show which methods triggered it
        echo "    By method:"
        grep 'GORUMS DEBUG: AfterFunc cleared' node-$i.log 2>/dev/null \
            | sed 's/.*method=\([^ ,]*\).*/\1/' \
            | sort | uniq -c | sort -rn \
            | while read count method; do
                printf "      %-40s %6s\n" "$method" "$count"
            done
        # Show pending counts when it fired
        echo "    Pending counts at time of clear:"
        grep 'GORUMS DEBUG: AfterFunc cleared' node-$i.log 2>/dev/null \
            | sed 's/.*pending=\([0-9]*\).*/\1/' \
            | sort -n | uniq -c | sort -rn | head -10 \
            | while read count pending; do
                printf "      pending=%-10s occurred %6s times\n" "$pending" "$count"
            done
        echo ""
    fi

    # Send errors
    send_errors=$(grep 'GORUMS DEBUG: Send error' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    echo "  Send errors:                      $send_errors"

    if [ "$send_errors" -gt 0 ]; then
        echo "  --- Send error details ---"
        echo "    By method:"
        grep 'GORUMS DEBUG: Send error' node-$i.log 2>/dev/null \
            | sed 's/.*method=\([^ ,]*\).*/\1/' \
            | sort | uniq -c | sort -rn \
            | while read count method; do
                printf "      %-40s %6s\n" "$method" "$count"
            done
        echo "    Pending counts at time of error:"
        grep 'GORUMS DEBUG: Send error' node-$i.log 2>/dev/null \
            | sed 's/.*pending=\([0-9]*\).*/\1/' \
            | sort -n | uniq -c | sort -rn | head -10 \
            | while read count pending; do
                printf "      pending=%-10s occurred %6s times\n" "$pending" "$count"
            done
        echo ""
    fi

    # Router pending growth (logged every 1000 entries)
    pending_logs=$(grep 'GORUMS DEBUG: router pending' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    echo "  Router pending milestones:        $pending_logs"

    if [ "$pending_logs" -gt 0 ]; then
        echo "  --- Pending growth ---"
        echo "    By method:"
        grep 'GORUMS DEBUG: router pending' node-$i.log 2>/dev/null \
            | sed 's/.*method=\([^ ,]*\).*/\1/' \
            | sort | uniq -c | sort -rn \
            | while read count method; do
                printf "      %-40s %6s\n" "$method" "$count"
            done
        echo "    Max pending seen:"
        grep 'GORUMS DEBUG: router pending' node-$i.log 2>/dev/null \
            | sed 's/.*pending=\([0-9]*\).*/\1/' \
            | sort -n | tail -1 \
            | while read maxp; do
                printf "      %s\n" "$maxp"
            done
        echo "    Timeline (first and last 5):"
        total=$pending_logs
        grep 'GORUMS DEBUG: router pending' node-$i.log 2>/dev/null | head -5 \
            | while read line; do
                echo "      $line"
            done
        if [ "$total" -gt 10 ]; then
            echo "      ..."
        fi
        grep 'GORUMS DEBUG: router pending' node-$i.log 2>/dev/null | tail -5 \
            | while read line; do
                echo "      $line"
            done
        echo ""
    fi

    # Summary
    echo "  ---"
    total_gorums=$(grep 'GORUMS DEBUG' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    echo "  Total GORUMS DEBUG lines:         $total_gorums"
    echo ""
done

# Cross-node summary
echo "============================================================"
echo "  CROSS-NODE SUMMARY"
echo "============================================================"
total_afterfunc=0
total_send_err=0
total_pending=0
for i in 1 2 3 4; do
    [ ! -f "node-$i.log" ] && continue
    af=$(grep 'GORUMS DEBUG: AfterFunc cleared' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    se=$(grep 'GORUMS DEBUG: Send error' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    rp=$(grep 'GORUMS DEBUG: router pending' node-$i.log 2>/dev/null | wc -l | tr -d ' ')
    total_afterfunc=$((total_afterfunc + af))
    total_send_err=$((total_send_err + se))
    total_pending=$((total_pending + rp))
done
echo "  Total AfterFunc clears:           $total_afterfunc"
echo "  Total Send errors:                $total_send_err"
echo "  Total Pending milestones:         $total_pending"
echo ""
if [ "$total_afterfunc" -eq 0 ] && [ "$total_send_err" -eq 0 ] && [ "$total_pending" -eq 0 ]; then
    echo "  >>> No GORUMS DEBUG output detected."
    echo "  >>> Streams are NOT being torn down."
    echo "  >>> Pending map is NOT growing past 1000."
    echo "  >>> The issue is likely pure response pipeline delay."
elif [ "$total_afterfunc" -gt 0 ]; then
    echo "  >>> AfterFunc IS clearing streams during the benchmark."
    echo "  >>> This causes requeue storms and response delays."
elif [ "$total_pending" -gt 0 ]; then
    echo "  >>> Router pending map IS growing — orphaned entries accumulating."
fi
