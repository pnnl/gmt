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

#include <string.h>
#include "gmt/aggregation.h"
#include "gmt/profiling.h"

#if ENABLE_PROFILING

uint64_t *events;

uint64_t print_events(char *event_str, event_type_t type,
                      uint64_t * profile, uint32_t nid)
{
    int i;
    uint64_t count = 0;
    uint64_t tot = 0;

    for (i = 0; i < (int)NUM_ENTITIES; i++) {
        count = profile[i * PROFILE_COUNTERS + type];
        if (count > 0) {
            char name[4];
            uint32_t id = 0;
            if (i < (int)NUM_WORKERS) {
                sprintf(name, "W ");
                id = i;
            } else if (i >= (int)NUM_WORKERS
                       && i < (int)(NUM_WORKERS + NUM_HELPERS)) {
                sprintf(name, "H ");
                id = i - NUM_WORKERS;
            } else
                sprintf(name, "CS");
            printf("%30s\t\t%s %u\t\t%2d\t%12lu\n",
                   event_str, name, id, nid, count);
            tot += count;
        }
    }

    return tot;
}

void print_all_profile(uint64_t * profile, uint32_t nid)
{
    printf
        ("\n****************Profiling Node %d **********************\n",
         nid);
    printf("\t\tEvent\t\t\tEntity\t\tNode\t\tSamples\n");

    /* Add events here and in profiling.h */
    PRINT_EVENTS(COMM_SEND_COMPLETED,
                 COMM_RECV_COMPLETED,
                 COMM_SEND_BYTES,
                 COMM_RECV_BYTES,
                 WORKER_ITS_STARTED,
                 WORKER_ITS_SELF_EXECUTE,
                 WORKER_ITS_EXECUTE_LOCAL,
                 WORKER_ITS_ENQUEUE_LOCAL, WORKER_ITS_ENQUEUE_REMOTE,
                 /*        
                    WORKER_WAIT_DATA,
                    WORKER_WAIT_MTASKS,
                    WORKER_WAIT_HANDLE,
                    WORKER_GMT_FREE,
                    WORKER_GMT_ALLOC,
                    WORKER_GMT_PUT_LOCAL,
                    WORKER_GMT_PUT_REMOTE,
                    WORKER_GMT_PUTVALUE_LOCAL,
                    WORKER_GMT_PUTVALUE_REMOTE,
                    WORKER_GMT_GET_LOCAL,
                    WORKER_GMT_GET_REMOTE,*/
                    WORKER_GMT_GET_HANDLE,
                    /*WORKER_GMT_ATOMIC_ADD_LOCAL,
                    WORKER_GMT_ATOMIC_ADD_REMOTE,
                    WORKER_GMT_ATOMIC_CAS_LOCAL,
                    WORKER_GMT_ATOMIC_CAS_REMOTE,
                    HELPER_CMD_FINALIZE,
                    HELPER_CMD_ALLOC,
                    HELPER_CMD_FREE,
                    HELPER_CMD_ATOMIC_ADD,
                    HELPER_CMD_ATOMIC_CAS,
                    HELPER_CMD_PUT,
                    HELPER_CMD_PUT_VALUE,
                    HELPER_CMD_GET,
                    HELPER_CMD_EXEC_PREEMPT, */
                 HELPER_CMD_EXEC_NON_PREEMPT,
                 /*HELPER_CMD_FOR_LOOP,
                    HELPER_CMD_FOR_EACH,
                    HELPER_CMD_MTASKS_RES_REQ,
                    HELPER_CMD_MTASKS_RES_REPLY,
                    HELPER_CMD_CHECK_HANDLE_REQ,
                    HELPER_CMD_CHECK_HANDLE_REPLY,
                    HELPER_CMD_FOR_COMPL,
                    HELPER_CMD_EXEC_COMPL,
                    HELPER_CMD_REPLY_ACK,
                    HELPER_CMD_REPLY_VALUE,
                    HELPER_CMD_REPLY_GET,
                    AGGREGATION_CMD_BYTES,
                    AGGREGATION_DATA_BYTES,
                    AGGREGATION_BLOCK_INFO_BYTES,
                    AGGREGATION_ON_MISS_CMDB,
                    AGGREGATION_ON_TIMEOUT,
                    AGGREGATION_BYTE_WASTE, 
                    AGGREGATION_ON_FULLBLOCK
                  */ );

}

void profile_init()
{
    int size = PROFILE_COUNTERS * NUM_ENTITIES * sizeof(uint64_t);
    events = _malloc(size);
    memset(events, 0, size);
}

void profile_destroy()
{
    free(events);
}


typedef struct profile_args_t {
    gmt_data_t g_ret;
    int flag;
} profile_args_t;

uint32_t collect_profile(void *args)
{
    int size = PROFILE_COUNTERS * NUM_ENTITIES * sizeof(uint64_t);
    profile_args_t *largs = (profile_args_t *) args;
    if (largs->flag != -1)
        gmt_put(largs->g_ret, node_id, (char *)events, 1);

    if (largs->flag == -1 || largs->flag == 1)
        memset(events, 0, size);
    return 0;
}

/* flag :  0 = print ; 1 = print and reset ; -1 = reset */
void gmt_profile(int flag)
{
    uint32_t size = NUM_ENTITIES * PROFILE_COUNTERS * sizeof(uint64_t);
    profile_args_t args;
    args.g_ret = gmt_alloc(size * num_nodes, GMT_ALLOC_LOCAL);
    args.flag = flag;
    uint32_t i = 0;
    for (i = 0; i < num_nodes; i++){
        gmt_execute_on_node(i, (gmt_execute_func_t) collect_profile,
                            &args, sizeof(profile_args_t),
                            NULL, NULL, GMT_PREEMPTABLE);
    }

    if (flag != -1) {
        uint64_t *array = (uint64_t *) gmt_get_local_ptr(args.g_ret, 0);
        uint32_t i;
        for (i = 0; i < num_nodes; i++) {
            print_all_profile(&array[i * NUM_ENTITIES * PROFILE_COUNTERS], i);
        }
    }
    gmt_free(args.g_ret);
}

void incr_event(event_type_t type, uint64_t incr)
{
    uint32_t thid = get_thread_id();
    _assert(thid < NUM_ENTITIES);
    events[thid * PROFILE_COUNTERS + type] += incr;
}

void set_max_event(event_type_t type, int64_t value)
{
    uint32_t thid = get_thread_id();
    _assert(thid < NUM_ENTITIES);
    if (value > 0 && events[thid * PROFILE_COUNTERS + type] < (uint64_t) value)
        events[thid * PROFILE_COUNTERS + type] = (uint64_t) value;
}

void reset_event(event_type_t type)
{
    uint32_t thid = get_thread_id();
    _assert(thid < NUM_ENTITIES);
    events[thid * PROFILE_COUNTERS + type] = 0;
}

#else

void gmt_profile(int reset)
{
    (void)reset;
}

void profile_init()
{
}

void profile_destroy()
{

}

#endif


#if ENABLE_INSTRUMENTATION
instrumentation_t instr;
bool is_instrumentation_init = false;

void instrumentation_init()
{
    memset(instr.funcs, 0, sizeof(func_node_t) * MAX_FUNCS * MAX_ENTITIES);
    printf("Node %d - Allocation of %ld MB for instrumentation\n", node_id,
           sizeof(func_node_t) * MAX_FUNCS * MAX_ENTITIES / (1 << 20));
    is_instrumentation_init = true;
}

int cmpfunc(const void *a, const void *b) NO_INSTRUMENT;
int cmpfunc(const void *a, const void *b)
{
    long va = ((func_node_t *) a)->tick_total;
    long vb = ((func_node_t *) b)->tick_total;
    if (vb > va)
        return 1;
    else if (va < vb)
        return -1;
    return 0;
}

void instrumentation_destroy()
{
    if (is_instrumentation_init == false)
        return;
    char filename[64];
    char func[256];
    char cmd[256];

    sprintf(filename, "profile-%d", node_id);
    FILE *fp = fopen(filename, "w");
    if (fp == NULL) {
        printf("ERROR opening profile file\n");
        exit(1);
    }

    int i, j;
    for (i = 0; i < (int)NUM_ENTITIES; i++) {
        bool first = true;
        qsort(&instr.funcs[i][0], MAX_FUNCS, sizeof(func_node_t), cmpfunc);
        for (j = 0; j < MAX_FUNCS; j++) {
            if (instr.funcs[i][j].ptr != NULL) {
                if (first) {
                    fprintf(fp, "n %d - e %d - collisions %ld/%ld\n",
                            node_id, i,
                            instr.collisions[i], instr.collisions_tot[i]);
                    first = false;
                }

                FILE *fp2;
                sprintf(cmd, "addr2line -e %s -i %p -f | head -n 1",
                        prog_name, instr.funcs[i][j].ptr);

                fp2 = popen((const char *)cmd, "r");
                if (fp2 == NULL) {
                    printf("command not executed\n");
                    exit(1);
                }

                char * tmp = fgets(func, 256, fp2);
                _unused(tmp);
                func[strlen(func) - 1] = '\0';
                pclose(fp2);
                fprintf(fp, "n %d - e %d - %p (%40s) ", node_id, i,
                        instr.funcs[i][j].ptr, func);

                if (instr.funcs[i][j].tick_total != 0)
                    fprintf(fp, "%ld %s", instr.funcs[i][j].tick_total,
                        instr.funcs[i][j].nest_flag == true ? "*\n":"\n");
            }
        }
    }
    fclose(fp);
}

void __cyg_profile_func_enter(void *this_fn, void *call_site)
{
    _unused(call_site);
    if (is_instrumentation_init == false)
        instrumentation_init();
    
    long tick = rdtsc();
    long val = (long)this_fn;
    int thid = get_thread_id();
    if (thid > MAX_ENTITIES) {
        printf("ERROR MAX_ENTITIES %d/%d \n", thid, MAX_ENTITIES);
        _exit(1);
    }
    mix_fasthash(val);
    int id = val % MAX_FUNCS;
    int cnt = 0;
    bool flag = false;
    while (instr.funcs[thid][id].ptr != NULL &&
           instr.funcs[thid][id].ptr != this_fn) {
        id = (id > MAX_FUNCS) ? 0 : id + 1;
        instr.collisions_tot[thid]++;
        if (!flag) {
            instr.collisions[thid]++;
            flag = true;
        }
        if (cnt++ >= MAX_FUNCS) {
            printf("increase MAX_FUNCS - too many functions to profile\n");
            exit(1);
        }
    }

    if (instr.funcs[thid][id].ptr == NULL) {
        instr.funcs[thid][id].ptr = this_fn;
        instr.funcs[thid][id].nesting = 0;
        instr.funcs[thid][id].nest_flag = false;
    }

    if (instr.funcs[thid][id].nesting++ == 0)
        instr.funcs[thid][id].tick_start = tick;
    else 
        instr.funcs[thid][id].nest_flag = true;
}

void __cyg_profile_func_exit(void *this_fn, void *call_site)
{
    long tick = rdtsc();
    long val = (long)this_fn;
    int thid = get_thread_id();
    if (thid > MAX_ENTITIES) {
        printf("ERROR MAX_ENTITIES %d/%d \n", thid, MAX_ENTITIES);
        _exit(1);
    }
    mix_fasthash(val);
    int id = val % MAX_FUNCS;
    if (instr.funcs[thid][id].ptr == NULL) {
        printf
            ("ERROR function not recorded in entry but now in exit  NULL?? %d - %p - %p\n",
             id, this_fn, call_site);
        _exit(1);
    }
    int cnt = 0;
    while (instr.funcs[thid][id].ptr != this_fn) {
        id = (id > MAX_FUNCS) ? 0 : id + 1;
        if (cnt++ >= MAX_FUNCS) {
            printf
                ("ERROR function not recorded in entry but now in exit NOT FOUND??\n");
            _exit(1);
        }
    }

    if (--instr.funcs[thid][id].nesting == 0)
        instr.funcs[thid][id].tick_total +=
            tick - instr.funcs[thid][id].tick_start;
}


#else

void instrumentation_destroy()
{

}

#endif


