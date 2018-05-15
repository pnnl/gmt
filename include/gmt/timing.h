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

#ifndef __TIMING_H__
#define __TIMING_H__

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/gmt.h"

void timing_init();
void timing_destroy();
#if ENABLE_TIMING

/* Add events here and remember to add 
 * each event to print_all_timing() */
enum timeType_t {
    WORKER_TIME,
    WORKER_BUSY,
    WORKER_SERVICE_ITB,
    WORKER_SERVICE_ITB_REAL,
    WORKER_SERVICE_CMDB,
    HELPER_SERVICE_AGGR,
    HELPER_SERVICE_BUF,
    HELPER_SERVICE_BUF_REAL,
    HELPER_SERVICE_CMDB,
    AGGREG_CHECK,
    AGGREG_REAL,
    AGGREG_CMDB_REAL,
    TIMING_COUNTERS
};

typedef struct timing_args_t {
    gmt_data_t g_ret;
    int reset;
} timing_args_t;

extern double _timing[MAX_ENTITIES * TIMING_COUNTERS];

/* Do not call functions directly, use defines instead */

void _print_timing(char **print_str, char *typeStr, enum timeType_t type,
                   double *timing, uint32_t nid);
void _print_all_timing(double *timing, uint32_t nid);

/* Use the following defines in the code */
#define PRINT_TIMING(STR,TYPE,TIMINGS,NID) _print_timing(STR,#TYPE,TYPE,TIMINGS,NID)

#define START_TIME(TIMESTAMP)  \
    double TIMESTAMP = rdtsc();
#define START_TIME2(TIMESTAMP)  \
    TIMESTAMP = rdtsc();
#define END_TIME(TIMESTAMP,TYPE)  \
_timing[get_thread_id * TIMING_COUNTERS + TYPE] += rdtsc()-TIMESTAMP;

#else                           /* ENABLE_TIMING not defined */

#define PRINT_TIMING(STR,TYPE,TIMING,NID)
#define START_TIME(TIMESTAMP)
#define START_TIME2(TIMESTAMP)
#define END_TIME(TIMESTAMP,TYPE)

#endif

#endif                          /* __TIMING_H__ */
