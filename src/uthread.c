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

#define _GNU_SOURCE
#include <unistd.h>
#include <sys/mman.h>
#include <math.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "gmt/uthread.h"

uthread_t *uthreads;
uint64_t ut_stacks_size = 0;;
void *ut_stacks = NULL;

/* support variables we don't want to recalculate them
   every time we get into the segmentation fault handler */
int log2pagesize = 0;
int pagesize = 0;

void uthread_set_stack(uint32_t tid, uint64_t size)
{
#if !ENABLE_EXPANDABLE_STACKS
    _assert(0);
#endif
    /* get the protection address of this uthread */
    _assert(log2pagesize != 0 && pagesize != 0);
    _assert(size % pagesize == 0);
    void *prot_addr = (void *)(UTHREADS_STACK_ENTRY_ADDRESS +
                               UTHREAD_MAX_STACK_SIZE * tid);

    /* in this case stack did not grow in previous execution */
    if (size == uthreads[tid].stack_size)
        return;

    /* we are shrinking the stack, this is a termination or final cleanup,
       no need to preserve anything */
    if (size < uthreads[tid].stack_size) {
        int ret = munmap(prot_addr, UTHREAD_MAX_STACK_SIZE);
        if (ret != 0)
            perror("FAILED TO UNMAP PROTECTION"), exit(EXIT_FAILURE);
        uthreads[tid].stack_size = 0;

        /* this is final cleanup just return */
        if (size == 0)
            return;
    }

    /* check is there was a previous protection and in case
       remove it */
    long old_prot_size = UTHREAD_MAX_STACK_SIZE - uthreads[tid].stack_size;
    if (old_prot_size != UTHREAD_MAX_STACK_SIZE) {
        int ret = munmap(prot_addr, old_prot_size);
        if (ret != 0)
            perror("FAILED TO UNMAP PROTECTION"), exit(EXIT_FAILURE);
    }

    /* set the new protection */
    long new_prot_size = UTHREAD_MAX_STACK_SIZE - size;
    _assert(new_prot_size % pagesize == 0);
    if (new_prot_size != 0) {
        void *ret = mmap(prot_addr, new_prot_size, PROT_NONE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
                         -1, (off_t) 0);
        if (ret == MAP_FAILED) {
            if (errno == ENOMEM) {
                struct rlimit rlim;
                if (getrlimit(RLIMIT_AS, &rlim) != 0) {
                    ERRORMSG("%s error in getrlimit(): %s", __func__,
                             strerror(errno));
                }
                ERRORMSG("Error ENOMEM doing mmap():"
                         " virtual address space limit reached (%llu) or too"
                         " many mmap() called."
                         "NOTE: Verify virtual address space limits for the "
                         "current process (check \"ulimit -a\" or"
                         " \"cat /proc/PID/status\").\n",
                         (long long)rlim.rlim_cur);
            } else {
                perror("mmap():FAILED TO MMAP PROTECTION"), exit(EXIT_FAILURE);
            }
        }
    }

    /* create a new page */
    void *page_addr = prot_addr + UTHREAD_MAX_STACK_SIZE - size;
    uint32_t page_size = size - uthreads[tid].stack_size;
    void *ret = mmap(page_addr, page_size,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
                     -1, (off_t) 0);
    if (ret == MAP_FAILED)
        perror("FAILED TO MMAP STACKS"), exit(EXIT_FAILURE);

    /* set the new page size */
    uthreads[tid].stack_size = size;

    /* collect statistics */
    if (config.print_stack_break) {
        if (size > uthreads[tid].max_stack_size)
            uthreads[tid].max_stack_size = size;
        if (size != UTHREAD_INITIAL_STACK_SIZE)
            uthreads[tid].num_breaks++;
    }

}

void uthread_touch_stack(uint32_t tid)
{
    uint64_t *ptr = (uint64_t *) (UTHREADS_STACK_ENTRY_ADDRESS + (tid + 1) *
                                  UTHREAD_MAX_STACK_SIZE -
                                  UTHREAD_INITIAL_STACK_SIZE);
    while (ptr <
           (uint64_t *) (UTHREADS_STACK_ENTRY_ADDRESS +
                         (tid + 1) * UTHREAD_MAX_STACK_SIZE)) {
        *((uint64_t *) ptr++) = 0xFFFFFFFFFFFFFFFF;
    }
}

void uthread_init_all()
{

    pagesize = getpagesize();
#if !defined(__USE_ISOC99)
    double log2(double);
#endif
    log2pagesize = (int)log2(pagesize);

    uthreads = (uthread_t *)_malloc(sizeof(uthread_t) *
                       NUM_UTHREADS_PER_WORKER * NUM_WORKERS);

    ut_stacks_size =
        NUM_UTHREADS_PER_WORKER * NUM_WORKERS * UTHREAD_MAX_STACK_SIZE;

#if ENABLE_EXPANDABLE_STACKS
    ut_stacks = (void *)UTHREADS_STACK_ENTRY_ADDRESS;
#else
    ut_stacks = _malloc(ut_stacks_size);
#endif

    uint32_t wid, j;
    /* set the initial stack for the uthreads */
    for (wid = 0; wid < NUM_WORKERS; wid++) {
        for (j = 0; j < NUM_UTHREADS_PER_WORKER; j++) {
            uint32_t tid = wid * NUM_UTHREADS_PER_WORKER + j;
            uthread_init(tid, wid);
#if ENABLE_EXPANDABLE_STACKS
            uthread_set_stack(tid, UTHREAD_INITIAL_STACK_SIZE);
            uthread_touch_stack(tid);
#endif
        }
    }
}

void uthread_destroy_all()
{
    uint32_t i = 0;
    uint32_t node_breaks = 0;
    uint32_t max_stack_size = 0;

    for (i = 0; i < NUM_WORKERS * NUM_UTHREADS_PER_WORKER; i++) {
#if ENABLE_EXPANDABLE_STACKS
        uthread_set_stack(i, 0);
#endif
        if (config.print_stack_break) {
            if (uthreads[i].num_breaks > 0) {
                node_breaks += uthreads[i].num_breaks;
                if (uthreads[i].max_stack_size > max_stack_size)
                    max_stack_size = uthreads[i].max_stack_size;
                DEBUG0(printf
                       ("GMT_WARNING:n %d - tid %d - broke his stack %d times "
                        " - max stack used %ld bytes\n", node_id, i,
                        uthreads[i].num_breaks - 1, uthreads[i].max_stack_size);
                    );
            }
        }
        uthread_destroy(i);
    }

    if (config.print_stack_break) {
        if (node_breaks > 0)
            printf("GMT_WARNING:n %d - Total stack breaks %d - max stack size "
                   "%d bytes\n", node_id, node_breaks, max_stack_size);
    }
#if !ENABLE_EXPANDABLE_STACKS
    free(ut_stacks);
#endif
    free(uthreads);
}

void uthread_init(int tid, int wid)
{
    uthreads[tid].tid = tid;
    uthreads[tid].wid = wid;
    uthreads[tid].req_nbytes = 0;
    uthreads[tid].recv_nbytes = 0;
    uthreads[tid].created_mtasks = (uint64_t *)_malloc(MAX_NESTING * sizeof(uint64_t));
    uthreads[tid].terminated_mtasks = (uint64_t *)_malloc(MAX_NESTING * sizeof(uint64_t));
    uthreads[tid].mt = NULL;
    uthreads[tid].tstatus = TASK_NOT_INIT;
    uthreads[tid].stack_size = 0;
    uthreads[tid].max_stack_size = 0;
    uthreads[tid].num_breaks = 0;
    uthreads[tid].nest_lev = 0;
    uint32_t i;
    for (i = 0; i < MAX_NESTING; i++) {
        uthreads[tid].created_mtasks[i] = 0;
        uthreads[tid].terminated_mtasks[i] = 0;
    }
}

void uthread_destroy(int tid)
{
    free(uthreads[tid].created_mtasks);
    free((void *)uthreads[tid].terminated_mtasks);
}
