#!/bin/bash
for file in $PWD/.gmt-*-*; do
    if [ ! -f $file ]; then
        continue
    fi
    var=${file##/*/.gmt-}
    pid=${var%%-*}
    hostname=${var#*-}
    CMD="/bin/kill -s USR1 $pid"
    echo ssh $hostname $CMD
    SSH_CMD=ssh
    SSH_CMD_OPTIONS="-Y $hostname"  # Note no whitespace to protect
    $SSH_CMD $SSH_CMD_OPTIONS "$CMD"
    if [ $? -eq 1 ]; then
        rm $file
    fi
done
