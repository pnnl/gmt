#!/bin/bash
if [ -z $2 ] || ( [ "$2" != '-v' ] && [ "$2" != '-t' ] ); then
    echo "usage: $0 <num_nodes> <-v (verify) /-t (time)>"
    exit
fi

NUM_WORKERS=9
NUM_HELPERS=9
MAX_TASK_PER_WORKER=1024
gmt_opt="--gmt_num_workers $NUM_WORKERS --gmt_num_helpers $NUM_HELPERS"
function do_test()
{
    LAUNCHER="mpirun -n $nodes -npernode 1"
    rm -f $output_file 
    touch .no_run
    for test_name in $test_names; do
        echo "Running test $test_name"
        for iter_per_node in $num_iterations; do
            for oper_per_iter  in $num_oper_per_iter; do
                for chunk_size in $chunk_sizes; do
                    for spawn_policy in $spawn_policies; do
                        for alloc_policy in $alloc_policies; do
                            for preempt_policy in $preempt_policies; do
                                cmd="$LAUNCHER ./gmttest -b $test_name -i $iter_per_node -n $oper_per_iter -c $chunk_size -s $spawn_policy -a $alloc_policy -p $preempt_policy $gmt_opt "$mode
                                echo -n $cmd
                                rm -f tmp.out
                                while true; do
                                    $cmd &> tmp.out
                                    return_code=$?
                                    diff tmp.out .no_run > /dev/null
                                    if [ $? -ne 0 ]; then
                                        break
                                    fi
                                    echo -n "..rep.."
                                done
                                if [ "$return_code" -ne "0" ]; then
                                    echo -e '\n\n' "Test binary returned an ERROR CODE!" '\n\n'
                                    cat tmp.out
                                    exit
                                fi
                                if [ "$mode" != '-v' ]; then
                                    timer=`grep "time " tmp.out`
                                    echo -n -e '\n' $timer
                                else
                                    passed=`grep PASSED tmp.out`
                                    if [ "$passed" == "" ]; then
                                        echo -e '\n\n' "Test NOT PASSED!" '\n\n'
                                        cat tmp.out
                                        exit
                                    else
                                        timer=`grep "Time " tmp.out`
                                        echo -n -e '\n' $timer
                                        #echo -n " ...PASSED"
                                    fi
                                fi
                                echo 
                            done
                        done
                    done
                done
            done
        done
        echo -e '\n\n' "Test PASSED!!!!" '\n\n'
    done
}

nodes=$1;

mode=""
if [ "$2" != '-t' ]; then
    mode=$2
fi
output_file="./test.out"

test_names="execute_on_node"
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="GMT_PREEMPTABLE GMT_NON_PREEMPTABLE"
num_iterations="$(($NUM_WORKERS*MAX_TASK_PER_WORKER-1))"
num_oper_per_iter="32"
chunk_sizes="8"
do_test

test_names="for_each" 
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE"
preempt_policies="NA"
num_iterations="$(($nodes))"
num_oper_per_iter="128"
chunk_sizes="8"
do_test

test_names="alloc" 
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="NA"
num_iterations="$(($NUM_WORKERS*MAX_TASK_PER_WORKER-1))"
num_oper_per_iter="128"
chunk_sizes="10240 64"
do_test

test_names="for_loop_whandle for_loop for_loop_nested"
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="NA"
num_iterations="$(($NUM_WORKERS*$nodes))"
num_oper_per_iter="32" 
chunk_sizes="8" 
do_test

test_names="execute_on_all execute"
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="GMT_PREEMPTABLE GMT_NON_PREEMPTABLE"
num_iterations="$(($NUM_WORKERS*MAX_TASK_PER_WORKER-1))"
num_oper_per_iter="32"
chunk_sizes="8"
do_test

test_names="yield putvalue atomic_add atomic_cas"
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="NA"
num_iterations="$(($NUM_WORKERS*MAX_TASK_PER_WORKER-1))" # 4096 1024"
num_oper_per_iter="32"
chunk_sizes="8"
do_test

test_names="memcpy" 
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="NA"
num_iterations="$(($NUM_WORKERS*$nodes))"
num_oper_per_iter="32"
chunk_sizes="1024 8"
do_test

test_names="get put" 
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="NA"
num_iterations="$(($NUM_WORKERS*MAX_TASK_PER_WORKER-1))"
num_oper_per_iter="32"
chunk_sizes="1024 8"
do_test

test_names="local_ptr" 
spawn_policies="GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION_FROM_ZERO GMT_SPAWN_PARTITION_FROM_RANDOM GMT_SPAWN_PARTITION_FROM_HERE GMT_SPAWN_SPREAD"
alloc_policies="GMT_ALLOC_PARTITION_FROM_ZERO GMT_ALLOC_PARTITION_FROM_RANDOM GMT_ALLOC_PARTITION_FROM_HERE GMT_ALLOC_REMOTE GMT_ALLOC_REPLICATE"
preempt_policies="NA"
num_iterations="$(($NUM_WORKERS*MAX_TASK_PER_WORKER))"
num_oper_per_iter="8"
chunk_sizes="128"
do_test

exit
#
