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

#ifndef __PROFILING_H__
#define __PROFILING_H__

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include "gmt/config.h"
#include "gmt/debug.h"

#define NUM_ENTITIES (NUM_WORKERS+NUM_HELPERS+1)

void profile_init();
void profile_destroy();

void instrumentation_init() NO_INSTRUMENT;
void instrumentation_destroy() NO_INSTRUMENT;

#ifdef __cplusplus
extern "C" {
#endif
void __cyg_profile_func_enter(void *this_fn, void *call_site) NO_INSTRUMENT;
void __cyg_profile_func_exit(void *this_fn, void *call_site) NO_INSTRUMENT;
#ifdef __cplusplus
}
#endif

#if ENABLE_PROFILING
/* Add events here and in profiling.c*/
typedef enum event_type_t {
    COMM_SEND_COMPLETED,
    COMM_RECV_COMPLETED,
    COMM_SEND_BYTES,
    COMM_RECV_BYTES,

    WORKER_ITS_STARTED,
    WORKER_ITS_SELF_EXECUTE,
    WORKER_ITS_EXECUTE_LOCAL,
    WORKER_ITS_ENQUEUE_LOCAL,
    WORKER_ITS_ENQUEUE_REMOTE,

    WORKER_WAIT_DATA,
    WORKER_WAIT_MTASKS,
    WORKER_WAIT_HANDLE,

    WORKER_GMT_FREE,
    WORKER_GMT_ALLOC,
    WORKER_GMT_PUT_LOCAL,
    WORKER_GMT_PUT_REMOTE,
    WORKER_GMT_MEM_PUT_REMOTE,
    WORKER_GMT_MEM_STRIDED_PUT_REMOTE,
    WORKER_GMT_PUTVALUE_LOCAL,
    WORKER_GMT_PUTVALUE_REMOTE,
    WORKER_GMT_GET_LOCAL,
    WORKER_GMT_GET_REMOTE,
    WORKER_GMT_MEM_GET_REMOTE,
    WORKER_GMT_ATOMIC_ADD_LOCAL,
    WORKER_GMT_ATOMIC_ADD_REMOTE,
    WORKER_GMT_ATOMIC_CAS_LOCAL,
    WORKER_GMT_ATOMIC_CAS_REMOTE,
    WORKER_GMT_GET_HANDLE,

    HELPER_CMD_FINALIZE,
    HELPER_CMD_ALLOC,
    HELPER_CMD_FREE,
    HELPER_CMD_ATOMIC_ADD,
    HELPER_CMD_ATOMIC_CAS,
    HELPER_CMD_PUT,
    HELPER_CMD_MEM_PUT,
    HELPER_CMD_MEM_STRIDED_PUT,
    HELPER_CMD_PUT_VALUE,
    HELPER_CMD_GET,
    HELPER_CMD_MEM_GET,
    HELPER_CMD_EXEC_PREEMPT,
    HELPER_CMD_EXEC_NON_PREEMPT,
    HELPER_CMD_FOR_LOOP,
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
    AGGREGATION_ON_FULLBLOCK,

    PROFILE_COUNTERS
} event_type_t;

extern uint64_t *events;

/* Do not call functions directly, use defines instead */
uint64_t print_events(char *eventStr, event_type_t type,
                      uint64_t * profile, uint32_t nid);
void print_all_profile(uint64_t * profile, uint32_t nid);

#if defined(__cplusplus)
extern "C" {
#endif
    void incr_event(event_type_t type, uint64_t incr);
    void set_max_event(event_type_t type, int64_t value);
    void reset_event(event_type_t type);
#if defined(__cplusplus)
}
#endif
#define PRINT_EVENTS( ... ) do {\
    char list[] = #__VA_ARGS__;\
    int  events[] = { __VA_ARGS__ };\
    int i = 0;\
    char name[256]; \
    char *str = list; \
    while(*str != '\0') { \
        while(*str == ' ' && *str != '\0') str++; \
        char *ptr = name; \
        while(*str != ',' && *str != '\0') \
            *ptr++ = *str++;\
        if (*str != '\0') str++;\
        *ptr='\0';\
        print_events(name, events[i++], profile, nid);\
    }\
} while(0);
#define COUNT_EVENT(TYPE)         incr_event(TYPE,1)
#define INCR_EVENT(TYPE,INCR)     incr_event(TYPE,INCR)
#define SET_MAX_EVENT(TYPE,VALUE) set_max_event(TYPE,VALUE)
#define RESET_EVENT(TYPE)         reset_event(TYPE)
#else

#define COUNT_EVENT(TYPE)
#define INCR_EVENT(TYPE,INCR)
#define SET_MAX_EVENT(TYPE,VALUE)
#define RESET_EVENT(TYPE)

#endif


#if ENABLE_INSTRUMENTATION

#define MAX_FUNCS      (32*1024)
#define MAX_ENTITIES   32

#define mix_fasthash(h) ({                      \
                (h) ^= (h) >> 23;               \
                (h) *= 0x2127599bf4325c37ULL;   \
                (h) ^= (h) >> 47; })

typedef struct func_node_t {
    void *ptr;
    int nesting;
    bool nest_flag;
    long tick_start;
    long tick_total;
} func_node_t;

typedef struct instrumentation_t {
    func_node_t funcs[MAX_ENTITIES][MAX_FUNCS];
    long collisions[MAX_ENTITIES];
    long collisions_tot[MAX_ENTITIES];
} instrumentation_t;

extern instrumentation_t instr;
#endif


#endif                          /* __PROFILING_H__ */
