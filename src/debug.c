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

#include <stdarg.h>
#include <signal.h>
#include "gmt/uthread.h"
#include "gmt/worker.h"
#include "gmt/helper.h"
#include "gmt/comm_server.h"
#include "gmt/debug.h"

  /*  This function can be called by both workers and helpers and can be used
   *    in any code that requires to identify the thread id.
   *    Workers have thread id from 0 to NUM_WORKERS-1, helpers have thread id from
   *    NUM_WORKERS to NUM_WORKERS + NUM_HELPERS - 1 */
uint32_t get_thread_id()
{
    int32_t thid = 0;
//     if ( pt_stacks == NULL || ut_stacks == NULL)
//         return thid;
    uint64_t sp = (uint64_t) arch_get_sp();
    /* this is pthread not running in any uthread */
    if (sp - (uint64_t) pt_stacks < pt_stacks_size)
        thid = (uint32_t) ((sp - (uint64_t) pt_stacks) / PTHREAD_STACK_SIZE);
    else if (sp - (uint64_t) ut_stacks < ut_stacks_size)
        thid = (sp - (uint64_t) ut_stacks) / (UTHREAD_MAX_STACK_SIZE *
                                              NUM_UTHREADS_PER_WORKER);
   
    return thid;
}

void _debug(const char *func, const char *file, int line, char *fmt, ...)
{
    const size_t size = 4096;
    char string[size];
    va_list argptr;
    va_start(argptr, fmt);
    vsnprintf(string, size, fmt, argptr);
    va_end(argptr);

    if (uthread_get_tid() < (uint32_t) (NUM_UTHREADS_PER_WORKER * NUM_WORKERS))
        printf("[n%3d-w%3d-t%4d] <%s:%d> %s(): %s", node_id, get_thread_id(),
               uthread_get_tid(), file, line, func, string);
    else if (get_thread_id() < NUM_WORKERS)
        printf("[n%3d-w%3d] <%s:%d> %s(): %s", node_id, get_thread_id(), file,
               line, func, string);
    else if (get_thread_id() >= NUM_WORKERS
             && get_thread_id() < NUM_HELPERS + NUM_WORKERS)
        printf("[n%3d-h%3d] <%s:%d> %s(): %s", node_id,
               get_thread_id() - NUM_WORKERS, file, line, func, string);
    else if (get_thread_id() == NUM_HELPERS + NUM_WORKERS)
        printf("[n%3d-CS] <%s:%d> %s(): %s", node_id, file, line, func, string);
    else
        printf("[n%3d] <%s:%d> %s(): %s", node_id, file, line, func, string);
}


void btrace()
{
    void *array[1024];
    size_t size;
    char **strings;
    size_t i;
    size = backtrace(array, 1024);
    strings = backtrace_symbols(array, size);
    _debug("--", "", 0, "Backtrace (if not done remember to compile with "
           "\"-g -rdynamic\" and to use: \"addr2line -e %s -i addr\" for more info)\n",
           prog_name);
    for (i = 0; i < size; i++)
        _debug(strings[i], "", 0, "\n");
    free(strings);
    fflush(stderr);
    fflush(stdout);
}

uint32_t sig_cnt_enter = 0;
uint32_t sig_cnt_exit = 0;

void sig_func(int sig)
{
    _unused(sig);   
    uint32_t cnt = __sync_fetch_and_add(&sig_cnt_enter, 1);
    if (cnt == 0) {
        printf("\n\n \t\t\t NODE %d %d\n\n", node_id, get_thread_id());
         btrace();
        uint32_t i;
        for (i = 0; i < NUM_WORKERS; i++)
            if ( i != get_thread_id())
                pthread_kill(workers[i].pthread, SIGUSR1);
        if (num_nodes != 1) {
            for (i = 0; i < NUM_HELPERS; i++) {
                if ( i + NUM_WORKERS != get_thread_id())
                    pthread_kill(helpers[i].pthread, SIGUSR1);                
            }
            if ( get_thread_id() != NUM_WORKERS + NUM_HELPERS)
                pthread_kill(cs.pthread, SIGUSR1);
        }
    } else {
         btrace();
    }

    if (num_nodes != 1) {
        if (__sync_fetch_and_add(&sig_cnt_exit, 1) ==
            NUM_WORKERS + NUM_HELPERS + 1) {
            sig_cnt_exit = 0;
            sig_cnt_enter = 0;
        }
    } else {
        if (__sync_fetch_and_add(&sig_cnt_exit, 1) == NUM_WORKERS) {
            sig_cnt_exit = 0;
            sig_cnt_enter = 0;
        }
    }
}

void debug_init()
{
    char hostname[256], filename[256];
    gethostname(hostname, 256);
    snprintf(filename, 256, ".gmt-%d-%s", getpid(), hostname);
    FILE *fp = fopen(filename, "a");
    if (fp == NULL) {
        printf("error debug_init\n");
        fflush(stdout);
        fflush(stderr);
        _exit(EXIT_FAILURE);
    }
    signal(SIGUSR1, sig_func);  // Register signal handler before going multithread
}
