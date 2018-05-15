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

#include "gmt/gmt.h"
#include "gmt/utils.h"
#include "gmt/timing.h"
#include "gmt/network.h"

#if ENABLE_TIMING

#define FREQ ( 2100 * 1000 * 1000)

double _timing[MAX_ENTITIES * TIMING_COUNTERS];

void _print_timing ( char **print_str, char * typeStr,enum timeType_t type, double * timing, uint32_t nid) {
    int entity;
    double time;
    double time_pernode = 0;
    char *p=*print_str;

    for ( entity = 0; entity < MAX_ENTITIES; entity++ ) {
        time = timing[entity * TIMING_COUNTERS + type];
        time_pernode += time;
#if !PRINT_TIMING_PERNODE
        if ( time >0 )
            p += sprintf ( p,"[%3d] %s %d %lf sec\n",nid,typeStr,entity,time / FREQ );
#endif
    }

#if PRINT_TIMING_PERNODE
    if(time_pernode > 0)
        p += sprintf ( p,"[%3d] %s %lf sec\n",nid,typeStr,time_pernode / FREQ); 
#endif

    *print_str=p;
}

#define STR_SIZE 16384
void _print_all_timing (double * timing, uint32_t nid) {
    char _str[STR_SIZE];
    memset(_str,0,STR_SIZE);
    char * _strp=_str;

    _strp+=sprintf(_strp,"\n************** Timing Node %d (Assuming Freq %d MHz.) **************\n", nid, FREQ/1000/1000);
    _strp+=sprintf(_strp,"\t\tEvent\t\t\tEntity\t\tTime\n");

    /* Add new timing here */
    PRINT_TIMING ( &_strp,WORKER_TIME,timing,nid);
    PRINT_TIMING ( &_strp,WORKER_BUSY,timing,nid);
    PRINT_TIMING ( &_strp,WORKER_SERVICE_CMDB,timing,nid);
    PRINT_TIMING ( &_strp,WORKER_SERVICE_ITB,timing,nid);
    PRINT_TIMING ( &_strp,WORKER_SERVICE_ITB_REAL,timing,nid);
    PRINT_TIMING ( &_strp,HELPER_SERVICE_CMDB,timing,nid);
    PRINT_TIMING ( &_strp,HELPER_SERVICE_BUF,timing,nid);
    PRINT_TIMING ( &_strp,HELPER_SERVICE_BUF_REAL,timing,nid);
    PRINT_TIMING ( &_strp,HELPER_SERVICE_AGGR,timing,nid);
    PRINT_TIMING ( &_strp,AGGREG_CHECK,timing,nid);
    PRINT_TIMING ( &_strp,AGGREG_REAL,timing,nid);
    PRINT_TIMING ( &_strp,AGGREG_CMDB_REAL,timing,nid);

    printf ( "%s",_str );
}

void timing_init() {
    int size = MAX_ENTITIES * TIMING_COUNTERS * sizeof(double);
    memset(_timing, 0, size); 
}

uint32_t collect_timing(void *args, void * ret ) {
    (void) ret;
    int size = MAX_ENTITIES * TIMING_COUNTERS * sizeof(double);
    timing_args_t * largs = (timing_args_t *) args;
    if (largs->reset != -1) 
        gmt_put(largs->g_ret, node_id * size, (char*) _timing, size);
    if (largs->reset == -1 || largs->reset == 1)
        memset(_timing, 0, size); 
    return 0;
}

void gmt_timing(int reset) {
    uint32_t size = MAX_ENTITIES*TIMING_COUNTERS*sizeof(double);
    timing_args_t args;
    args.g_ret = gmt_alloc(size * num_nodes, GMT_ALLOC_LOCAL);
    args.reset = reset;
    gmt_execute_pernode(collect_timing, &args, sizeof(timing_args_t), 
            0, NULL, NULL, GMT_PREEMPTABLE);
    if ( reset != -1 ) {
        double * array = (double * ) gmt_local_ptr(args.g_ret);
        uint32_t i;
        for ( i = 0; i < num_nodes; i++) {
            _print_all_timing(&array[i * MAX_ENTITIES * TIMING_COUNTERS],i);
        }
    }
    gmt_free(args.g_ret);
}
#else
void gmt_timing(int reset) {(void) reset;}
void timing_init() {}
void timing_destroy(){}
#endif
