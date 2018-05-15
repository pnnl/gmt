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

#include <sys/types.h>
#include <sys/wait.h>
#include <getopt.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include "gmt/comm_server.h"
#include "gmt/aggregation.h"
#include "gmt/helper.h"
#include "gmt/memory.h"
#include "gmt/worker.h"
#include "gmt/profiling.h"
#include "gmt/utils.h"
#include "gmt/config.h"
#include "gmt/mtask.h"

/* bring these inside network.h or config_t ?? */
uint32_t num_nodes = 1;
uint32_t node_id = 0;
int realRet = EXIT_SUCCESS;

void generate_main_task_args(int argc, char *argv[],
                             uint32_t * args_bytes, uint8_t ** args)
{
    if (argv != NULL) {
        *args_bytes = argc * sizeof(uint8_t *); /* argv[] */
        int i;
        for (i = 0; i < argc; i++) {
            //printf ( "@@@@   %d - %s\n", argc, argv[i] );
            *args_bytes += strlen(argv[i]) + 1; /* +1 for "\0" */
            //printf ( "args_bytes %d\n", *args_bytes );
        }

        *args = _malloc(sizeof(uint8_t) * (*args_bytes));
        _assert(args != NULL);

        uint32_t offset_bytes = 0;
        uint8_t *base_addr = *args + argc * sizeof(uint8_t *);
        //printf ( "args_start %p - offset 0x%016lx - base_addr %p\n",
        //         *args, (long) (argc * sizeof ( uint8_t * )), base_addr );
        for (i = 0; i < argc; i++) {
            uint8_t *offset = base_addr + offset_bytes;
            //printf ( "offset 0x%016lx\n", (long) offset );            
            *((uint64_t *) (*args + i * sizeof(uint8_t *))) = (uint64_t) offset;
            strcpy((char *)offset, argv[i]);
            strcpy((char *)(offset + strlen(argv[i])), "\0");
            offset_bytes += strlen(argv[i]) + 1;
            //printf ( "offset_bytes %d\n", offset_bytes );
        }
    }
}

/*****************************************************************************/
/* args to the gmt_main task */
uint8_t *gm_args = NULL;
uint32_t gm_argc = 0;
uint32_t gm_args_bytes = 0;

char *prog_name = NULL;

void *pt_stacks = NULL;
uint64_t pt_stacks_size = 0;

int main(int argc, char *argv[])
{
    /* initialize configuration to default paramters */
    config_init();
    prog_name = argv[0];
#if !(ENABLE_SINGLE_NODE_ONLY)
    network_init(&argc, &argv);
#endif

    if ((argc = config_parse(argc, argv)) == -1) {
        if (node_id == 0)
            config_help();
#if !(ENABLE_SINGLE_NODE_ONLY)
        network_finalize();
#endif
        exit(EXIT_SUCCESS);
    }

    /* check configuration to see if everything is consistent, then print it */
    config_check();
    if (node_id == 0)
        config_print();

    if (config.enable_usr_signal)
        debug_init();

    /* make sure there is enough virtual address space to create all the stacks 
       for uthreads, workers, helpers and comm_server, */
//     set_res_limits(UTHREAD_MAX_STACK_SIZE * NUM_WORKERS *
//                    config.state_name + (NUM_WORKERS + NUM_HELPERS +
//                                               1) * PTHREAD_STACK_SIZE);
//     get_shmem_bytes(&mem.shmem_size_total, &mem.shmem_size_used,
//                     &mem.shmem_size_avail);
    
    pt_stacks_size = (NUM_WORKERS + NUM_HELPERS + 1) * PTHREAD_STACK_SIZE;
    pt_stacks = _malloc(pt_stacks_size);

    /* initialize profile and timing in case we compiled for it */
    timing_init();
    profile_init();    

#if !(ENABLE_SINGLE_NODE_ONLY)
    if (num_nodes != 1) {
        comm_server_init();
        aggreg_init();
        helper_team_init();
    }
#endif
    mem_init();
    mtm_init();
    worker_team_init();

    /* registering at exit */
    atexit(worker_exit);

    /* generate the args for gmt_main */
    if (node_id == 0) {
        gm_argc = argc;
        generate_main_task_args(argc, argv, &gm_args_bytes, &gm_args);
    }
#if !(ENABLE_SINGLE_NODE_ONLY)
    if (num_nodes != 1) {
        comm_server_run();
        helper_team_run();
    }
#endif

    worker_team_run();

    if (node_id == 0) {
        free(gm_args);
        print_line(100);
    }
#if !(ENABLE_SINGLE_NODE_ONLY)
    if (num_nodes != 1) {
        helper_team_stop();
        comm_server_stop();
        helper_team_destroy();
        aggreg_destroy();
        comm_server_destroy();
    }
#endif
    worker_team_destroy();
    mem_destroy();
    mtm_destroy();

    if (config.print_gmt_mem_usage) {
        printf("GMT internal structures usage - %ld MB\n",
               _total_malloc / 1024 / 1024);
    }
#if !(ENABLE_SINGLE_NODE_ONLY)
    network_finalize();
#endif

    profile_destroy();
    timing_destroy();
    instrumentation_destroy();
    free(pt_stacks);
    
    /* calling flush not called after _exit */
    fflush(stdout);
    fflush(stderr);        
    /* do not remove otherwise the worker_exit will be called again at 
       the end of main */
    _exit(realRet);
}
