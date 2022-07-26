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

#ifndef __UTHREAD_H__
#define __UTHREAD_H__

#include <stdbool.h>
#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/queue.h"
#include "gmt/mtask.h"
#include "gmt/gmt_ucontext.h"

/* state of a task running on the uthread */
typedef enum {
    TASK_NOT_INIT,
    TASK_NOT_STARTED,           /* this is also equivalent to TASK_COMPLETED */
    TASK_RUNNING,
    TASK_WAITING_DATA,
    TASK_WAITING_MTASKS,
    TASK_WAITING_HANLDE,
    TASK_THROTTLING
} task_status_t;

typedef struct uthread_t {
    /* local uthread identifier (unique per node) */
    uint32_t tid;

    /* worker assigned to this uthread (fixed) */
    uint32_t wid;

    /* pointer to the macro-task that started this uthread */
    mtask_t *mt;

    /* context for this uthread */
    ucontext_t ucontext;

    /* requested and received bytes for this uthread */
    uint64_t req_nbytes;
    volatile uint64_t recv_nbytes;

    /* created and terminated  mtasks */
    uint64_t *created_mtasks;
    uint64_t volatile *terminated_mtasks;

    /* status of the task running on this uthread */
    task_status_t tstatus;

    /* "real" stack size for this uthread */
    uint64_t stack_size;

    /* max "real" stack space ever used by this uthread */
    uint64_t max_stack_size;

    /* number of times this uthread broke the stack and
       needed expansion */
    uint32_t num_breaks;

    /* nesting level of execution for this uthread */
    uint32_t nest_lev;
} uthread_t;

extern uthread_t *uthreads;
extern uint64_t ut_stacks_size;
extern void *ut_stacks;

DEFINE_QUEUE(uthread_queue, uthread_t *, NUM_UTHREADS_PER_WORKER);

void uthread_init(int tid, int wid);
void uthread_destroy(int tid);
#if defined(__cplusplus)
extern "C" {
#endif
void uthread_set_stack(uint32_t tid, uint64_t size);
#if defined(__cplusplus)
}
#endif
void uthread_init_all();
void uthread_destroy_all();

/* uthread methods */

/* the uthread id is obtained looking at the stack pointer */
INLINE uint32_t uthread_get_tid()
{
    return ((uint64_t) arch_get_sp() -
            (uint64_t) ut_stacks) / UTHREAD_MAX_STACK_SIZE;
}

INLINE void uthread_incr_nesting(uint32_t tid)
{
    uthreads[tid].nest_lev++;
    if (uthreads[tid].nest_lev >= MAX_NESTING)
        ERRORMSG("max nesting reached");
}

INLINE void uthread_dec_nesting(uint32_t tid)
{
    uthreads[tid].nest_lev--;
}

INLINE bool uthread_check_terminated_mtasks(uthread_t * ut)
{
    uint32_t l = ut->nest_lev;
    _assert(l < MAX_NESTING);    
    return (ut->created_mtasks[l] == ut->terminated_mtasks[l]);
}

INLINE bool uthread_check_recv_all_data(uthread_t * ut)
{
    return (ut->recv_nbytes == ut->req_nbytes);
}


INLINE bool uthread_can_run(uthread_t * ut)
{
    _assert(ut->tstatus != TASK_NOT_INIT);
    switch (ut->tstatus) {
    case TASK_RUNNING:
    case TASK_NOT_STARTED:
    case TASK_THROTTLING:
    case TASK_WAITING_HANLDE:
        return true;
    case TASK_WAITING_DATA:
        if (ut->nest_lev < (MAX_NESTING - 1) ||
            uthread_check_recv_all_data(ut))
            return true;
        return false;
    case TASK_WAITING_MTASKS:
        if (ut->nest_lev < (MAX_NESTING - 1) ||
            uthread_check_terminated_mtasks(ut))
            return true;
        return false;
    default:
        ERRORMSG("TASK STATUS unknown\n");
    }
}
INLINE void uthread_tid_check(uint32_t tid){
     if(tid >= NUM_UTHREADS_PER_WORKER * NUM_WORKERS){
      fprintf(stderr,"%s error: tid value incorrect. Make sure this is not being "
      "called from a GMT_NON_PREEMPTABLE task.\n", __func__);
      btrace();
      _exit(EXIT_FAILURE);
    } 
}

INLINE uint32_t uthread_get_gtid(uint32_t tid, uint32_t node_id)
{
  uthread_tid_check(tid);
  return (node_id * NUM_UTHREADS_PER_WORKER * NUM_WORKERS) + tid;
}

INLINE uint32_t uthread_get_tid_from_gtid(uint32_t gtid, uint32_t node_id)
{
  return gtid - (node_id * NUM_UTHREADS_PER_WORKER * NUM_WORKERS);
}

INLINE uint32_t uthread_get_nest_lev(uint32_t tid)
{
  uthread_tid_check(tid);
  return uthreads[tid].nest_lev;
}

INLINE uint32_t uthread_get_wid(uint32_t tid)
{
  uthread_tid_check(tid);
  uint32_t wid = uthreads[tid].wid;
  _assert(wid < NUM_WORKERS);
  return wid;
}

INLINE uint32_t uthread_get_node(uint32_t gtid)
{
    _assert((int32_t) gtid != -1);
    return gtid / (NUM_UTHREADS_PER_WORKER * NUM_WORKERS);
}

INLINE void uthread_incr_req_nbytes(uint32_t tid, uint64_t nbytes)
{
    uthread_tid_check(tid);
    uthreads[tid].req_nbytes += nbytes;
}

INLINE void uthread_incr_created_mtasks(uint32_t tid)
{
    uthread_tid_check(tid);
    uint32_t l = uthreads[tid].nest_lev;
    _assert(l < MAX_NESTING);    
    uthreads[tid].created_mtasks[l]++;
}

INLINE void uthread_incr_recv_nbytes(uint32_t tid, uint64_t nbytes)
{
    uthread_tid_check(tid);
    __sync_add_and_fetch(&uthreads[tid].recv_nbytes, nbytes);
}

INLINE void uthread_incr_terminated_mtasks(uint32_t tid, uint32_t nl)
{
    uthread_tid_check(tid);
    _assert(nl < MAX_NESTING);    
    __sync_add_and_fetch(&uthreads[tid].terminated_mtasks[nl], 1);
}

INLINE void uthread_makecontext(ucontext_t * ucp,
                                void *wrapper, uint64_t taskid, uint32_t tidx)
{
    /* To maintain compatibility with the libc version of make context
       we split 64 bit value in two 32 bits variables (Low and High) and
       join them again into the wrapper function */

    uint32_t taskid_H32 = (uint32_t) ((uint64_t) (taskid) >> 32);
    uint32_t taskid_L32 = (uint32_t) ((uint64_t) (taskid) & 0xffffffffUL);

    gmt_makecontext(ucp, (void (*)())wrapper, 3, taskid_L32, taskid_H32, tidx);
}

#endif
