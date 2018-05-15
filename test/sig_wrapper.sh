#! /bin/bash

if [ -z $1 ]; then
	echo "usage: $0 <command>"
	exit

fi

function handle_signal {
  echo " received signal now sending SIGUSR1 to pid $PID"
  kill -s USR1 $PID
  wait $PID
  sleep 2
}

trap "handle_signal" INT TERM


$@ &
PID=$!
wait $PID


