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

#ifndef __MTASK_H__
#define __MTASK_H__

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/utils.h"
#include "gmt/gmt.h"
#include "gmt/queue.h"
#include "gmt/network.h"
#include "gmt/profiling.h"
#include "gmt/commands.h"

/**
  A 'task' is the basic unit of execution for a 'uthread'. 'uthreads' are 
  software threads that each 'worker' context switches to achieve latency 
  tolerance. Each time a high level work creation operation (i.e.gmt_for_loop)
  is executed it results in the creation of one or more 'tasks'. One or more tasks
  (belonging to the same high level work creation) that are sent for execution 
  on the same physical node are grouped in a 'mtask' (macro-task). 

  For instance a gmt_par_loop with 80 iterations where iterations are blocked in 2
  will result in the creation of 40 tasks. If the execution is partitioned 
  across 4 nodes it results in the creation of 4 'mtasks' (multi-tasks) each
  of size 10.

  A 'mtask' is the basic unit of work requested to a node. Empty 'mtasks' are 
  stored in the queue 'mtasks_pool' and pushed on queue 'mtasks_queue' when a 
  work   request is made for a node.

  When a 'worker' grabs a 'mtask' from the queue 'mtasks_queue' it breaks into 
  single 'tasks', a 'task' is assigned to one and only one 'uthreads' and it 
  holds the 'uthreads' until it is not completed.

  A 'worker' may not be able to assign all the tasks in a 'mtask' to all its 
  'uthreads' (each worked has only MAX_UTHREAD_PER_WORKER available). In this 
  case the 'mtask' is repositioned in the queue 'mtasks_queue' to  allow another 
  'worker' (or the same 'worker' later if some of uthread completed their 
  'task') to start the execution of the remaining tasks int the 'mtask'. 

 */

typedef enum {
    MTASK_EXECUTE,
    MTASK_FOR,
    MTASK_GMT_MAIN
} mtask_type_t;

//TODO make this packed and use bit-fields
typedef struct PACKED_STR {
    /* information for the number of the number of iterations to start */
    uint64_t end_it:ITER_BITS __align(CACHE_LINE);
    uint64_t start_it:ITER_BITS;
    uint32_t step_it;
    /** generic function pointer */
    void *func;
    /* actual array with the args */
    void *largs;
    /** pointer to the args array */
    void *args;
    /** number of bytes in the args */
    uint32_t args_bytes;
    /** max size of the args bytes */
    uint32_t max_args_bytes;
    /** global thread id of the uthread that created this mtask 
     * (i.e. the global id of the parent) */
    int32_t gpid;
    /** nesting level of the uthread that created this mtask 
     * (i.e. the global id of the parent) */
    uint8_t nest_lev:NESTING_BITS;
    /** handle id associated with this mtask, if it is not GMT_HANDLE_NULL 
       when this mtask completes marks it the handle */
    gmt_handle_t handle;
    /** type of mtask EXECUTE, FOR_EACH, FOR_LOOP */
    mtask_type_t type:CMD_TYPE_BITS;

    uint32_t qid;
    /* return buffer pointer */
    void *ret_buf;
    /* pointer where to write the buffer size */
    uint32_t *ret_buf_size_ptr;
    /* gmt array used by this mtask */
    gmt_data_t gmt_array;
    /* this is at the bottom of this structure so we don't interfere with
     *other information above when doing fetch and add */
    uint64_t executed_it;

#if DTA
    uint32_t allocator_id;
#endif
} mtask_t;

typedef enum {
    HANDLE_NOT_USED,
    HANDLE_USED,
    HANDLE_CHECK_PENDING,
    HANDLE_RESET,
    HANDLE_COMPLETED
} handle_status_t;

typedef struct g_handle_t {
    uint32_t gtid;
    volatile int32_t mtasks_created;
    volatile int32_t mtasks_terminated;
    volatile handle_status_t status;
    volatile bool has_left_node;
} g_handle_t;

/** DEFINE for handleid_queue multiple producers and multiple consumers */
DEFINE_QUEUE_MPMC(handleid_queue, uint64_t, config.max_handles_per_node);

typedef struct mtasks_manager_t {
    /* structures for task scheduling */
#if ALL_TO_ALL
	spsc_t **mtasks_queues;
#elif !SCHEDULER
	qmpmc_t *mtasks_queue;
#else
	spsc_t *mtasks_sched_in_queues, *mtasks_sched_out_queues;
#endif
	uint32_t worker_in_degree;

	/* structures for task allocation */
	uint32_t pool_size;

#if !DTA
	/** mtasks pool */
    qmpmc_t mtasks_pool;

    /** actual array of the mtasks of size MAX_MTASKS_PER_THREAD */
    mtask_t *mtasks;

#if !NO_RESERVE
    /** number of mtasks available on this node */
    volatile int64_t num_mtasks_avail;
#endif
#endif

#if !NO_RESERVE
    /** Array of mtasks that this node has reserved on each remote node */
    int64_t volatile *num_mtasks_res_array;

    /** Array of mtask reservation pending flags one per node */
    bool *mtasks_res_pending;
#endif

    /**  total iterations (mtasks expanded) present on this node 
      used for load balancing */
    volatile int64_t total_its; //  __align ( CACHE_LINE );    

    /** Array of handles of size MAX_HANDLES * num_nodes */
    g_handle_t *handles;

    uint32_t num_used_handles;

    /** pool for handle ids each node will have only
     * MAX_HANDLES ids available with ids from (node_id * MAX_HANDLES) 
     * to ((node_id + 1) * MAX_HANDLES). 
     * We need only a pool because the handleid "is around" until not returned */
    handleid_queue_t handleid_pool;
} mtask_manager_t;

extern mtask_manager_t mtm;

void mtm_init();
void mtm_destroy();
void mtm_mtask_init(mtask_t *, uint32_t, uint32_t *);
void mtm_mtask_destroy(mtask_t *);

#if DTA
INLINE void mtm_mtask_bind_allocator(mtask_t *mt, uint32_t aid) {
       mt->allocator_id = aid;
}
#endif

#if !NO_RESERVE
/** aquire a reservation slot (if a reservation exists) for the remote node rnid */
INLINE bool mtm_acquire_reservation(uint32_t rnid)
{
    int64_t ret = __sync_sub_and_fetch(&mtm.num_mtasks_res_array[rnid], 1);    
    if (ret < 0) {
        __sync_add_and_fetch(&mtm.num_mtasks_res_array[rnid], 1);
        return false;
    }
    return true;
}

/** mark that a reservation block was made on remote node rnid */
INLINE void mtm_mark_reservation_block(uint32_t rnid, uint64_t value)
{
    __sync_add_and_fetch(&mtm.num_mtasks_res_array[rnid],  value);
}

#if !DTA
/** reserve a block of mtask locally */
INLINE uint64_t mtm_reserve_mtask_block(uint32_t res_size)
{
    int64_t rt = __sync_sub_and_fetch(&mtm.num_mtasks_avail, res_size);
    if (rt < 0) {
        uint64_t res = 0;
        /* we managed to get something even if less than res_size */
        if (rt + res_size > 0) {
            res = rt + res_size;
            __sync_sub_and_fetch(&mtm.num_mtasks_avail, rt);
        } else
            __sync_add_and_fetch(&mtm.num_mtasks_avail, res_size);
        return res;
    }
    return res_size;
}
#endif

/** lock reservation of mtasks on a given remote node, this prevents multiple
 * uthreads to attempt concurrent requests of a block of MTASKS */
INLINE bool mtm_lock_reservation(uint32_t rnid)
{
    if (__sync_bool_compare_and_swap(&mtm.mtasks_res_pending[rnid],
                                     false, true))
        return true;
    return false;
}

INLINE void mtm_unlock_reservation(uint32_t rnid)
{
    bool ret = __sync_bool_compare_and_swap(&mtm.mtasks_res_pending[rnid],
                                            true, false);
    _unused(ret);
    _assert(ret);
}
#endif

/*
 * functions for (re)schedule/get work to/from mtasks queues
 */
INLINE void mtm_return_mtask_queue(mtask_t * mt, uint32_t src_id)
{
#if ALL_TO_ALL
	spsc_push(&mtm.mtasks_queues[src_id][mt->qid], mt);
#elif !SCHEDULER
	_unused(src_id);
    qmpmc_push(&mtm.mtasks_queue[mt->qid], mt);
#else
    spsc_push(&mtm.mtasks_sched_in_queues[src_id], mt);
#endif
}

INLINE bool mtm_pop_mtask_queue(uint32_t cnt, mtask_t ** mt, uint32_t dst_id)
{
#if ALL_TO_ALL
	return spsc_pop(&mtm.mtasks_queues[cnt][dst_id], (void **) mt);
#elif !SCHEDULER
	_unused(dst_id);
    return qmpmc_pop(&mtm.mtasks_queue[cnt], (void **) mt);
#else
    _unused(cnt);
    return spsc_pop(&mtm.mtasks_sched_out_queues[dst_id], (void **) mt);
#endif
}

INLINE void mtm_fill_mtask(mtask_t * mt, void *func, uint32_t args_bytes,
                                 const void *args, int32_t gpid,
                                 uint64_t nest_lev, mtask_type_t type,
                                 uint64_t start_it, uint64_t end_it,
                                 uint64_t step_it, gmt_data_t gmt_array,
                                 uint32_t * ret_buf_size_ptr, void *ret_buf,
                                 gmt_handle_t handle)
{
    mt->func = func;
    mt->type = type;
    mt->handle = handle;
    mt->gpid = gpid;
    mt->nest_lev = nest_lev;
    mt->start_it = start_it;
    mt->end_it = end_it;
    mt->step_it = step_it;
    mt->executed_it = start_it;
    mt->ret_buf_size_ptr = ret_buf_size_ptr;
    mt->ret_buf = ret_buf;
    mt->gmt_array = gmt_array;

    if (mt->max_args_bytes < args_bytes) {
        if (mt->largs != NULL)
            free(mt->largs);
        mt->max_args_bytes = args_bytes;
        mt->largs = _malloc(args_bytes);
    }
    if (args_bytes > 0 && args != NULL) {
        memcpy(mt->largs, args, args_bytes);
        mt->args = mt->largs;
        mt->args_bytes = args_bytes;
    } else {
        mt->args = NULL;
        mt->args_bytes = 0;
    }
}

INLINE void mtm_push_mtask(mtask_t * mt, uint32_t src_id)
{
    __sync_fetch_and_add(&mtm.total_its, mt->end_it - mt->start_it);
#if ALL_TO_ALL
	spsc_push(&mtm.mtasks_queues[src_id][mt->qid], mt);
#elif !SCHEDULER
    _unused(src_id);
    qmpmc_push(&mtm.mtasks_queue[mt->qid], mt);
#else
    spsc_push(&mtm.mtasks_sched_in_queues[src_id], mt);
#endif
    INCR_EVENT(WORKER_ITS_ENQUEUE_LOCAL, mt->end_it - mt->start_it);
}

INLINE void mtm_schedule_mtask(mtask_t * mt, void *func, uint32_t args_bytes,
                                 const void *args, int32_t gpid,
                                 uint64_t nest_lev, mtask_type_t type,
                                 uint64_t start_it, uint64_t end_it,
                                 uint64_t step_it, gmt_data_t gmt_array,
                                 uint32_t * ret_buf_size_ptr, void *ret_buf,
                                 gmt_handle_t handle, uint32_t src_id)
{
    mtm_fill_mtask(mt, func, args_bytes, args, gpid, nest_lev, type, start_it,
        end_it, step_it, gmt_array, ret_buf_size_ptr, ret_buf, handle);
    mtm_push_mtask(mt, src_id);
}

INLINE void mtm_copy_mtask(mtask_t *dst, mtask_t *src) {
	mtm_fill_mtask(dst, src->func, src->args_bytes, src->args, src->gpid,
			src->nest_lev, src->type, src->start_it, src->end_it, src->step_it,
			src->gmt_array, src->ret_buf_size_ptr, src->ret_buf, src->handle);
}

INLINE int64_t mtm_total_its()
{
    return mtm.total_its;
}

INLINE void mtm_decrease_total_its(int64_t its)
{
    if (its != 0) {
        int64_t ret = __sync_sub_and_fetch(&mtm.total_its, its);
        _assert(ret >= 0);
    }
}

/*******************************************************/
/*             handle management functions             */
/*******************************************************/
INLINE gmt_handle_t mtm_handle_pop(uint32_t gtid)
{
    uint64_t handle;

    if (__sync_add_and_fetch(&mtm.num_used_handles, 1) >=
        config.max_handles_per_node) {
        ERRORMSG("maximum number of global handles "
                 " supported reached - MAX_HANDLES %d\n",
                 config.max_handles_per_node);

    }
    while (!handleid_queue_pop(&mtm.handleid_pool, &handle)) ;

    /* preserve terminated and created. DO NOT reset them */
    mtm.handles[handle].has_left_node = false;
    mtm.handles[handle].gtid = gtid;
    mtm.handles[handle].status = HANDLE_USED;
    mtm.handles[handle].mtasks_created = 0;
    mtm.handles[handle].mtasks_terminated = 0;
    return (gmt_handle_t) handle;
}

INLINE void mtm_handle_isvalid(gmt_handle_t handle, mtask_t * mt, uint32_t gtid)
{
    if (handle != GMT_HANDLE_NULL) {
        if (mtm.handles[handle].gtid != gtid)
            _check(handle == mt->handle);
    }
}

INLINE void mtm_handle_push(gmt_handle_t handle, uint32_t gtid)
{
    _assert(mtm.handles[handle].gtid == gtid);
    mtm.handles[handle].gtid = (uint32_t) - 1;
    mtm.handles[handle].status = HANDLE_NOT_USED;
    handleid_queue_push(&mtm.handleid_pool, (uint64_t) handle);
    __sync_sub_and_fetch(&mtm.num_used_handles, 1);
}

INLINE void mtm_handle_icr_mtasks_created(gmt_handle_t handle, uint64_t num)
{
    if (handle != GMT_HANDLE_NULL) {
        _assert(&mtm.handles[handle].mtasks_created != NULL);
        __sync_add_and_fetch(&mtm.handles[handle].mtasks_created, num);
    }
}

INLINE void mtm_handle_icr_mtasks_terminated(gmt_handle_t handle, uint64_t num)
{
    _assert(handle != GMT_HANDLE_NULL);
    _assert(&mtm.handles[handle].mtasks_terminated != NULL);
    __sync_add_and_fetch(&mtm.handles[handle].mtasks_terminated, num);
}

INLINE void mtm_handle_set_has_left_node(gmt_handle_t handle)
{
    mtm.handles[handle].has_left_node = true;
}

#endif
