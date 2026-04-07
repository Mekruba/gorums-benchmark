#!/bin/bash

for i in 1 2 3 4; do
    echo "=== NODE $i ==="
    pp_sent=$(grep 'preprepare sent' node-$i.log | wc -l | tr -d ' ')
    errors=$(grep 'send error' node-$i.log | wc -l | tr -d ' ')
    ctxdone=$(grep 'ctx.Done' node-$i.log | wc -l | tr -d ' ')
    pp_total=$(grep 'PRE-PREPARE' node-$i.log | wc -l | tr -d ' ')

    prep_total=$(grep 'prepare received' node-$i.log | wc -l | tr -d ' ')
    prep_f1=$(grep 'prepare received' node-$i.log | grep 'from=1' | wc -l | tr -d ' ')
    prep_f2=$(grep 'prepare received' node-$i.log | grep 'from=2' | wc -l | tr -d ' ')
    prep_f3=$(grep 'prepare received' node-$i.log | grep 'from=3' | wc -l | tr -d ' ')
    prep_f4=$(grep 'prepare received' node-$i.log | grep 'from=4' | wc -l | tr -d ' ')

    commit_total=$(grep 'commit received' node-$i.log | wc -l | tr -d ' ')
    commit_f1=$(grep 'commit received' node-$i.log | grep 'from=1' | wc -l | tr -d ' ')
    commit_f2=$(grep 'commit received' node-$i.log | grep 'from=2' | wc -l | tr -d ' ')
    commit_f3=$(grep 'commit received' node-$i.log | grep 'from=3' | wc -l | tr -d ' ')
    commit_f4=$(grep 'commit received' node-$i.log | grep 'from=4' | wc -l | tr -d ' ')

    printf "  preprepare sent:      %6s    preprepare received: %6s\n" $pp_sent $pp_total
    printf "  send errors:          %6s\n" $errors
    printf "  ctx.Done:             %6s\n" $ctxdone
    printf "  prepare received:     %6s    commit received:     %6s\n" $prep_total $commit_total
    printf "    from=1:             %6s      from=1:            %6s\n" $prep_f1 $commit_f1
    printf "    from=2:             %6s      from=2:            %6s\n" $prep_f2 $commit_f2
    printf "    from=3:             %6s      from=3:            %6s\n" $prep_f3 $commit_f3
    printf "    from=4:             %6s      from=4:            %6s\n" $prep_f4 $commit_f4
    echo ""
done
