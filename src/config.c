/*
 * Global Memory and Threading (GMT)
 *
 * Copyright © 2018, Battelle Memorial Institute
 * All rights reserved.
 *
 * Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to
 * any person or entity lawfully obtaining a copy of this software and associated
 * documentation files (hereinafter “the Software”) to redistribute and use the
 * Software in source and binary forms, with or without modification.  Such
 * person or entity may use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and may permit others to do
 * so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name `Battelle Memorial Institute` or `Battelle` may be used in
 *    any form whatsoever without the express written consent of `Battelle`.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL `BATTELLE` OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include "gmt/config.h"
#include "gmt/utils.h"
#include "gmt/network.h"
#include "gmt/commands.h"

config_t config;

int gmt_get_comm_buffer_size()
{
    return COMM_BUFFER_SIZE;
}

void config_init()
{
    config.comm_buffer_size = 256 * 1024;
    config.num_cmd_blocks = 128;
    config.cmd_block_size = 4096;
    config.num_buffs_per_channel = 64;
    config.num_cores = get_num_cores();
#if ENABLE_SINGLE_NODE_ONLY
    config.num_workers = config.num_cores;
    config.num_helpers = 0;
#else
    config.num_workers = MAX(1,(config.num_cores-1)/2);
    config.num_helpers = MAX(1,(config.num_cores-1)/2);
#endif
    config.num_uthreads_per_worker = 1024;
    config.max_nesting = 2;
    config.mtasks_per_queue = 8 * 1024;    
    config.num_mtasks_queues = MAX(2,config.num_workers/2);    
#if !DTA
    config.mtasks_res_block_loc = 32;
#endif
    config.mtasks_res_block_rem = 1024;
    config.limit_parallelism = false;
    config.thread_pinning = false;
    config.release_uthread_stack = false;
    config.print_stack_break = false;
    config.print_gmt_mem_usage = false;
    config.print_sched_interv = 0;
    config.cmdb_check_interv = 100000;
    config.node_agg_check_interv = 2000000;
#if DTA
	config.dta_chunk_size = 1024;
	config.dta_prealloc_worker_chunks = 32;
	config.dta_prealloc_helper_chunks = 256;
#endif
    config.enable_usr_signal = false;
    config.state_name[0] = '\0';
    config.state_populate = 0;
    config.state_prot = PROT_READ;
    config.disk_path[0] = '\0';
    config.ssd_path[0] = '\0';
    config.max_handles_per_node = 256 * 1024;
    config.handle_check_interv = 1024;
    config.mtask_check_interv = 100000;
    config.stride_pinning = 1;
}

#define OPT_INT    0
#define OPT_STRING 1
#define OPT_BOOL   2
#define OPT_UINT32 3
#define OPT_UINT64 4

typedef union value_t {
    char *svalue;
    uint32_t uvalue;
    int ivalue;
    bool bvalue;
} value_t;

/* do not reorder */
typedef struct options_t {
    char *name;                 // name of the option
    int type;                   // OPT_STRING or OPT_INT
    bool has_arg;               // OPT_BOOL and has_args can be merged???
    void *ptr;                  // pointer where to store the value if found
    value_t val;                // value to assign (in case has_arg is false)
    bool is_dyn;
    char *help;                 // help message
} options_t;

options_t options[] = {

    {"--gmt_num_workers", OPT_UINT32, true, &config.num_workers,
     {NULL}, NUM_WORKERS_DYN, "Number of worker threads"},

    {"--gmt_num_helpers", OPT_UINT32, true, &config.num_helpers,
     {NULL}, NUM_HELPERS_DYN, "Number of helper threads"},

    {"--gmt_num_uthreads_per_worker", OPT_UINT32, true,
     &config.num_uthreads_per_worker,
     {NULL}, NUM_UTHREADS_PER_WORKER_DYN, "Number of uthreads per worker"},

    {"--gmt_max_nesting", OPT_UINT32, true, &config.max_nesting,
     {NULL}, MAX_NESTING_DYN,
     "maximum level of nesting each uthread can perform"},

    {"--gmt_comm_buffer_size", OPT_UINT32, true, &config.comm_buffer_size,
     {NULL}, COMM_BUFFER_SIZE_DYN,
     "Size of the communication buffers in the network in bytes"},

    {"--gmt_num_buffs_per_channel", OPT_UINT32, true,
     &config.num_buffs_per_channel,
     {NULL}, NUM_BUFFS_PER_CHANNEL_DYN,
     "Number of buffers per channel, a channel can be used only to send or to "
     "recv. A worker has 1 send channel while a helper has both a send and recv "
     "channel"},

    {"--gmt_num_cmd_blocks", OPT_UINT32, true, &config.num_cmd_blocks, {NULL},
     NUM_CMD_BLOCKS_DYN,
     "Number of command blocks per node that can be used to send message to a "
     "remote node (impacts aggregation)"},

    {"--gmt_cmd_block_size", OPT_UINT32, true, &config.cmd_block_size, {NULL},
     NUM_CMD_BLOCKS_DYN,
     "Size in bytes of the command block for aggregation (this will also define "
     "the maximum size of argument and return buffer to a task)"},

    {"--gmt_mtasks_per_queue", OPT_UINT32, true, &config.mtasks_per_queue,
     {NULL}, true,
     "Number of mtasks per queue "
     "(an mtask corresponds to one or more tasks that run on the uthreads)"},

     {"--gmt_num_mtasks_queues", OPT_UINT32, true, &config.num_mtasks_queues,
     {NULL}, true,
     "Number of mtasks queues "
     "(an mtask corresponds to one or more tasks that run on the uthreads)"},
     
     {"--gmt_mtasks_res_block_rem", OPT_UINT32, true, &config.mtasks_res_block_rem,
     {NULL}, true,
     "Block of mtasks each node will try to reserve form remote nodes"},

#if !DTA
     {"--gmt_mtasks_res_block_loc", OPT_UINT32, true, &config.mtasks_res_block_loc,
     {NULL}, true,
     "Block of mtasks each worker will try to reserve locally"},
#endif
     
    {"--gmt_max_handles_per_node", OPT_UINT32, true,
     &config.max_handles_per_node,
     {NULL}, true,
     "Max number of handles per node (with a handle can check the completion of "
     "the mtasks started by one or more nested operations)"},

    {"--gmt_handle_check_interv", OPT_UINT32, true, &config.handle_check_interv,
     {NULL}, true,
     "Ticks a task will wait before rechecking if a remote handle is completed"},

    {"--gmt_mtask_check_interv", OPT_UINT32, true, &config.mtask_check_interv,
     {NULL}, true,
     "Ticks a worker will wait before rechecking for available tasks to start "
     "from the mtask queue"},

    {"--gmt_cmdb_check_interv", OPT_UINT32, true, &config.cmdb_check_interv,
     {NULL}, true,
     "Ticks a worker or helper will wait before aggregating commands in its local "
     "command block in case a command block is never full"},

    {"--gmt_node_agg_check_interv", OPT_UINT32, true,
     &config.node_agg_check_interv,
     {NULL}, true,
     "Ticks a helper will wait before aggregating all the commands in a node "
     " in case a communication buffer is never full"},

#if DTA
	{"--gmt_dta_chunk_size", OPT_UINT32, true,
	 &config.dta_chunk_size,
	 {NULL}, true,
	 "Size of mtasks-chunks for mtasks allocators"},

	{"--gmt_dta_prealloc_worker_chunks", OPT_UINT32, true,
	 &config.dta_prealloc_worker_chunks,
	 {NULL}, true,
	 "Number of pre-allocated chunks for each worker-local allocator"},

	{"--gmt_dta_prealloc_helper_chunks", OPT_UINT32, true,
	 &config.dta_prealloc_helper_chunks,
	 {NULL}, true,
	 "Number of pre-allocated chunks for the helper-local allocator"},
#endif

    {"--gmt_state_name", OPT_STRING, true, &config.state_name, {NULL}, true,
     "State name to restore (or create if it does not exist)"},

    {"--gmt_state_rw", OPT_INT, false, &config.state_prot,
     {.ivalue = PROT_READ | PROT_WRITE}, true,
     "Set r/w permission on the state"},

    {"--gmt_state_populate", OPT_BOOL, false, &config.state_populate,
     {.bvalue = true}, true,
     "Populate state at initialization"},

    {"--gmt_ssd_path", OPT_STRING, true, &config.ssd_path, {NULL}, true,
     "SSD path to use"},

    {"--gmt_disk_path", OPT_STRING, true, &config.disk_path, {NULL}, true,
     "Disk path to use"},

    {"--gmt_limit_parallelism", OPT_BOOL, false, &config.limit_parallelism,
     {.bvalue = true}, true,
     "Limits the parallelism that a single task can create to "
     "at most NUM_WORKERS * NUM_UTHREADS_PER_WORKER_DYN * num_nodes "},

    {"--gmt_thread_pinning", OPT_BOOL, false, &config.thread_pinning,
     {.bvalue = true}, true,
     "Enable pinning of workers, helper and comm_server to the cores"},

    {"--gmt_num_cores", OPT_INT, true, &config.num_cores,
     {NULL}, true,
     "Sets the max number of cores to use when pinning pthreads "
     "(functional only if used with --gmt_thread_pinning)"},

    {"--gmt_stride_pinning", OPT_INT, true, &config.stride_pinning,
     {NULL}, true,
     "Sets the value for stride in the pinning pthreads "
     "(functional only if used with --gmt_thread_pinning)"},

    {"--gmt_release_uthread_stack", OPT_BOOL, false,
     &config.release_uthread_stack,
     {.bvalue = true}, true,
     "Enable reset of uthread stack size to default size (in case this stack "
     "was previously expanded by another task)"},

    {"--gmt_print_stack_break", OPT_BOOL, false, &config.print_stack_break,
     {.bvalue = true}, true,
     "Print info about stack break done by the uthread. This info could be "
     "used to extend the default stack size"},

    {"--gmt_print_gmt_mem_usage", OPT_BOOL, false, &config.print_gmt_mem_usage,
     {.bvalue = true}, true,
     "Print info about static and dynamic memory used by internal GMT memory "
     "structures (not data allocated with gmt_alloc)"},

    {"--gmt_print_sched_interv", OPT_UINT64, true, &config.print_sched_interv,
     {NULL}, true,
     "Print interval for worker scheduler status in ticks"},

    {"--gmt_enable_usr_signal", OPT_BOOL, false, &config.enable_usr_signal,
     {.bvalue = true}, true,
     "enable to receive \"/bin/kill -s USR1 pid\", a .gmt-pid-hostname file "
     "is created (utility sig_send.sh can send to all hostname and pids) "},
};

void config_print()
{
    int i;
    print_line(100);
    printf("%50s\n", "GMT config");
    print_line(100);
    printf("%25s%10s%16s\n", "Option", "Dynamic", "Value");
    putchar('\n');

    printf("%25s%10s%16d\n", "num_nodes", "yes", num_nodes);
    for (i = 0; i < (int)(sizeof(options) / sizeof(options_t)); i++) {
        printf("%25s%10s", options[i].name + strlen("--gmt_"),
               options[i].is_dyn ? "yes" : "no");
        switch (options[i].type) {
        case OPT_INT:
            printf("%16d", *((int *)options[i].ptr));
            break;
        case OPT_UINT32:
            printf("%16d", *((uint32_t *) options[i].ptr));
            break;
        case OPT_UINT64:
            printf("%16ld", *((uint64_t *) options[i].ptr));
            break;
        case OPT_BOOL:
            printf("%16d", *((bool *) options[i].ptr));
            break;
        case OPT_STRING:
            printf("%16s", (char *)options[i].ptr);
            break;
        }
        printf("\n");
    }

    printf("\n");
    PRINT_CONFIG_INT(ENABLE_SINGLE_NODE_ONLY);
    PRINT_CONFIG_INT(ENABLE_ASSERTS);
    PRINT_CONFIG_INT(ENABLE_PROFILING);
    PRINT_CONFIG_INT(ENABLE_TIMING);
    PRINT_CONFIG_INT(ENABLE_GMT_UCONTEXTS);
    PRINT_CONFIG_INT(ENABLE_EXPANDABLE_STACKS);
    PRINT_CONFIG_INT(ENABLE_AGGREGATION);
    PRINT_CONFIG_INT(ENABLE_HELPER_BUFF_COPY);
#ifdef BUILD_VERSION
    PRINT_CONFIG_STR(BUILD_VERSION);
#endif
#ifdef CFLAGS
    PRINT_CONFIG_STR(CFLAGS);
#endif
#if defined(COMPILER_VERSION) && defined(COMPILER_NAME)
    PRINT_CONFIG_STR(COMPILER_NAME);
    PRINT_CONFIG_STR(COMPILER_VERSION);
#endif
    print_line(100);
}

void config_help()
{
    int i;
    print_line(100);
    printf("%50s\n", "GMT Help");
    print_line(100);
    printf("%30s%9s%8s   %s\n", "Option", "Value", "Default", "Description");

    putchar('\n');
    for (i = 0; i < (int)(sizeof(options) / sizeof(options_t)); i++) {
        if (!options[i].is_dyn)
            continue;
        printf("%30s", options[i].name);

        switch (options[i].type) {
        case OPT_INT:
            if (options[i].has_arg)
                printf("%9s", "int");
            else
                printf("%9s", "");
            printf("%8d", *((int *)options[i].ptr));
            break;
        case OPT_UINT32:
            if (options[i].has_arg)
                printf("%9s", "uin32_t");
            else
                printf("%9s", "");
            printf("%8d", *((uint32_t *) options[i].ptr));
            break;
        case OPT_UINT64:
            if (options[i].has_arg)
                printf("%9s", "uint64_t");
            else
                printf("%9s", "");
            printf("%8ld", *((uint64_t *) options[i].ptr));
            break;
        case OPT_BOOL:
            printf("%9s", "");
            printf("%8d", *((bool *) options[i].ptr));
            break;
        case OPT_STRING:
            if (options[i].has_arg)
                printf("%9s", "string");
            else
                printf("%9s", "");
            printf("%8s", (char *)options[i].ptr);
            break;
        }

        printf("   ");
        int cnt = 0, cr = 0;
        while (options[i].help[cnt] != '\0') {
            putchar(options[i].help[cnt]);
            cnt++;
            if (cnt % 50 == 0)
                cr = 1;
            /* carriage return is ready but wait until a char with space is found */
            if (cr && options[i].help[cnt] == ' ') {
                printf("\n%30s%9s%8s   ", "", "", "");
                cr = 0;
                /* advance to the fist non space char */
                while (options[i].help[cnt] == ' ')
                    cnt++;
            }
        }
        printf("\n");
    }
    printf("%30s%9s%8s   %s\n", "--gmt_help", "", "", "This help message");
    printf("\n\n*long can have (k,K,m,M,g,G,t,T) postfix or (0x,x,X) prefix\n");
    print_line(100);
}

int config_parse(int argc, char *argv[])
{
    int i = 0, j = 0, cnt = 0;
    for (i = 0; i < argc; i++) {
        if ((strcmp(argv[i], "--gmt_help") == 0) ||
            (strcmp(argv[i], "--gmt-help") == 0))
            return -1;
        /* make sure we recognize everything that starts with --gmt */
        if (strncmp(argv[i], "--gmt", 5) == 0) {
            bool rec = false;
            for (j = 0; j < (int)(sizeof(options) / sizeof(options_t)); j++) {
                if (strcmp(argv[i], options[j].name) == 0) {
                    rec = true;
                }
            }
            if (rec == false) {
                if (node_id == 0)
                    printf("GMT ERROR: option %s not recognized\n", argv[i]);
                return -1;
            }
        }
    }

    char **nargv = (char **)_malloc(argc * sizeof(char *));
    i = 0;
    while (i < argc) {
        //printf("%d - checking:%s\n", i,argv[i]);
        bool match = false;
        for (j = 0; j < (int)(sizeof(options) / sizeof(options_t)); j++) {

            if (strcmp(argv[i], options[j].name) == 0) {
                //printf("%d - match:%s\n", i, argv[i]);
                i++;
                switch (options[j].type) {
                case OPT_UINT32:
                    if (options[j].has_arg) {
                        if (argv[i] == NULL)
                            return -1;
                        long val = strtol_suffix(argv[i]);
                        if (val == -1)
                            return -1;
                        i++;
                        *((uint32_t *) options[j].ptr) = val;
                    } else
                        *((uint32_t *) options[j].ptr) = options[j].val.uvalue;
                    break;
                case OPT_UINT64:
                    if (options[j].has_arg) {
                        if (argv[i] == NULL)
                            return -1;
                        long val = strtol_suffix(argv[i]);
                        if (val == -1)
                            return -1;
                        i++;
                        *((uint64_t *) options[j].ptr) = val;
                    } else
                        *((uint64_t *) options[j].ptr) = options[j].val.uvalue;
                    break;
                case OPT_INT:
                    if (options[j].has_arg) {
                        if (argv[i] == NULL)
                            return -1;
                        long val = strtol_suffix(argv[i]);
                        if (val == -1)
                            return -1;
                        i++;
                        *((int *)options[j].ptr) = val;
                    } else
                        *((int *)options[j].ptr) = options[j].val.ivalue;
                    break;
                case OPT_BOOL:
                    *((bool *) (options[j].ptr)) = options[j].val.bvalue;
                    break;
                case OPT_STRING:
                    if (options[j].has_arg) {
                        if (argv[i] == NULL)
                            return -1;
                        sscanf(argv[i], "%s\n", (char *)options[j].ptr);
                        i++;
                    } else
                        sprintf((char *)options[j].ptr, "%s\n",
                                (char *)options[j].val.svalue);
                    break;
                }
                match = true;
                break;
            }
        }

        if (!match && i < argc)
            nargv[cnt++] = argv[i++];
    }
    for (i = 0; i < cnt; i++)
        argv[i] = nargv[i];

    free(nargv);
    return cnt;
}

void config_check()
{
    _check(UTHREAD_MAX_STACK_SIZE >= UTHREAD_MAX_RET_SIZE);
    _check(UTHREAD_MAX_RET_SIZE <= CMD_BLOCK_SIZE);
    _check(config.num_mtasks_queues >= 1);
    _check(config.mtasks_per_queue >= 1);
    _check(CMD_BLOCK_SIZE <= COMM_BUFFER_SIZE);
    _check(MAX_NESTING >= 1 && MAX_NESTING < (1<< NESTING_BITS));
    _check(config.max_handles_per_node * num_nodes < UINT32_MAX);
    _check(CMD_BLOCK_SIZE <= (1 << ARGS_SIZE_BITS));
    _check(NUM_WORKERS * NUM_UTHREADS_PER_WORKER <= (1 << TID_BITS));
#if DTA
    _check(NUM_HELPERS <= 1);
#endif

//     /* if state is null un-protect */
//     if (config.state_name == NULL)
//         config.state_name = PROT_READ | PROT_WRITE;
}
