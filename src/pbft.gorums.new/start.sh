./pbft server --id 1 --verbose  2> node-1.log &
PID1=$!
echo $PID1 > /tmp/pbft1.pid

./pbft server --id 2 --verbose  2> node-2.log &
PID2=$!
echo $PID2 > /tmp/pbft2.pid

./pbft server --id 3 --verbose  2> node-3.log &
PID3=$!

./pbft server --id 4 --verbose  2> node-4.log &
PID4=$!
echo $PID4 > /tmp/pbft4.pid

echo "PIDs: 1=$PID1 2=$PID2 3=$PID3 4=$PID4"
echo "To kill a replica:   kill \$(cat /tmp/pbft2.pid)"
echo "To freeze a replica: kill -STOP \$(cat /tmp/pbft2.pid)"
echo "To resume a replica: kill -CONT \$(cat /tmp/pbft2.pid)"

trap "kill $PID1 $PID2 $PID3 $PID4; rm -f /tmp/pbft{1,2,3,4}.pid" SIGINT SIGTERM

wait
