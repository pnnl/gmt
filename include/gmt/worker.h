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

#ifndef __WORKER_H__
#define __WORKER_H__

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/queue.h"
#include "gmt/commands.h"
#include "gmt/aggregation.h"
#include "gmt/gmt_ucontext.h"
#include "gmt/uthread.h"
#include "gmt/helper.h"

typedef struct worker_t {
  /* queues of uthreads and pool */
  uthread_queue_t uthread_queue;
  uthread_queue_t uthread_pool;

  /* "Return address" within the worker for context switch */
  ucontext_t worker_ctxt;

  /* counter used to check on the mtask queue for new work */
  uint64_t cnt_mtasks_check;

  /* counter used to perform periodic check on the scheduler */
  uint64_t cnt_print_sched;

  /* timer used to check on the cmd blocks timeout */
  uint64_t tick_cmdb_timeout;

  /* seed for the random number generator provided by GMT */
  uint64_t rand_seed;

  /* pthread used by this worker */
  pthread_t pthread;

  mtask_t **mt_res;
  uint32_t num_mt_res;

  mtask_t **mt_ret;
  uint32_t num_mt_ret;

  uint32_t rr_cnt;

#if TRACE_QUEUES
  uint64_t pop_misses = 0, pop_hits = 0;
#endif
} worker_t;

extern volatile bool workers_stop_flag;
extern worker_t *workers;

void worker_team_init();
void worker_team_destroy();
void worker_team_run();
void worker_team_stop();
void worker_exit();

#if defined(__cplusplus)
extern "C" {
#endif
  void worker_task_wrapper(uint32_t taskid_L32, uint32_t taskid_H32);
#if defined(__cplusplus)
}
#endif

INLINE void worker_push_task(uint32_t wid, mtask_t * mt, uint64_t start_it)
{
  /* find a uthread where to start this task */
  uthread_t *ut = NULL;
  if (!uthread_queue_pop(&workers[wid].uthread_pool, &ut)) {
    ERRORMSG("uthread available not found\n");
  }
  _assert(ut != NULL);
  _assert(ut->tstatus == TASK_NOT_INIT);
#if ENABLE_EXPANDABLE_STACKS
  if (config.release_uthread_stack)
    uthread_set_stack(ut->tid, UTHREAD_INITIAL_STACK_SIZE);
#endif
  uthread_makecontext(&ut->ucontext, (void *)worker_task_wrapper, start_it);
  ut->mt = mt;

  uint32_t i;
  for (i = 0; i < MAX_NESTING; i++) {
    ut->created_mtasks[i] = 0;
    ut->terminated_mtasks[i] = 0;
  }

  _assert(ut->nest_lev == 0);
  ut->req_nbytes = 0;
  ut->recv_nbytes = 0;
  ut->tstatus = TASK_NOT_STARTED;
  uthread_queue_push(&workers[wid].uthread_queue, ut);
}

INLINE void worker_self_execute(uint32_t tid, uint32_t wid)
{
  // worker body can't self execute 
  if (tid == (uint32_t) - 1)
    return;

  uthread_t *ut = &uthreads[tid];
  if (ut->nest_lev < (MAX_NESTING - 1)
      && mtm_total_its() != 0) {
    /* find available work */
    mtask_t *mt = NULL;
    if (!mtm_pop_mtask_queue(workers[wid].rr_cnt, &mt, wid)) {
      if (++workers[wid].rr_cnt >= mtm.worker_in_degree)
        workers[wid].rr_cnt = 0;
#if TRACE_QUEUES
      ++workers[wid].pop_misses;
#endif
      return;
    }
#if TRACE_QUEUES
      ++workers[wid].pop_hits;
#endif
    _assert(mt != NULL);
    _assert(mt->start_it < mt->end_it);
    uint64_t start_it = mt->start_it;
    mt->start_it += mt->step_it;
    mtm_decrease_total_its(MIN(mt->step_it, mt->end_it - start_it));
    if (mt->start_it < mt->end_it)
      mtm_return_mtask_queue(mt, wid);

    /* save uthread information */
    uthread_t *ut = &uthreads[tid];
    mtask_t *bmt = ut->mt;
    task_status_t tstatus = ut->tstatus;
    ut->mt = mt;

    /* increase nesting level */
    uthread_incr_nesting(tid);

    /* execute locally */
    uint32_t start_it_L32 = start_it & ((1l << 32) - 1);
    uint32_t start_it_H32 = (start_it >> 32) & ((1l << 32) - 1);
    worker_task_wrapper(start_it_L32, start_it_H32);

    /* decrease nesting level */
    uthread_dec_nesting(tid);

    /* restore uthread information */
    ut->mt = bmt;
    ut->tstatus = tstatus;
    INCR_EVENT(WORKER_ITS_SELF_EXECUTE, 1);
  }
}

INLINE void worker_check_mtask_queue(uint32_t tid, uint32_t wid)
{
  /* check timeout on mtask queue  */
  if (workers[wid].cnt_mtasks_check++ > config.mtask_check_interv) {

    /* check if there are uthreads available, if not return */
    uint32_t ut_avail = uthread_queue_size(&workers[wid].uthread_pool);
    if (ut_avail == 0) {
      _unused(tid);
      worker_self_execute(tid, wid);
      return;
    }
    /* check if there are iteration to execute, if not return */
    uint64_t est_total_its = mtm_total_its();
    if (est_total_its == 0)
      return;

    /* estimate a max number of iterations for load balancing,
     * this is not exact because this variable is modified by 
     * multiple workers and helpers without atomics
     * so we read only a snapshot of it*/
    int64_t max_enqueue = CEILING(est_total_its, NUM_WORKERS);
    int64_t enqueued = 0;
    uint32_t ut_used = 0;
    mtask_t *mt = NULL;
    while (enqueued < max_enqueue) {
      /* check if we used all the ut available */
      int64_t nt_lim_space = ut_avail - ut_used;
      _assert(nt_lim_space >= 0);
      if (nt_lim_space == 0)
        break;
      /* check if there is a mt to execute */
      if (!mtm_pop_mtask_queue(workers[wid].rr_cnt, &mt, wid)) {
#if TRACE_QUEUES
          ++workers[wid].pop_misses;
#endif
        if (++workers[wid].rr_cnt >= mtm.worker_in_degree)
          workers[wid].rr_cnt = 0;
        break;
      }
#if TRACE_QUEUES
      ++workers[wid].pop_hits;
#endif
      _assert(mt != NULL);
      /* record start, end, step iteration for this mtask */
      uint64_t start_it = mt->start_it;
      uint64_t end_it = mt->end_it;
      uint64_t step_it = mt->step_it;
      /* calculate limits */
      uint64_t nt_mt = CEILING((end_it - start_it), step_it);
      uint64_t nt_lim_bal = CEILING(max_enqueue - enqueued, step_it);
      /* the tasks we can start are the min of nt_lim_bal, nt_lim_space and
       * nt_mt */
      uint64_t nt = MIN(MIN(nt_mt, nt_lim_bal), (uint64_t) nt_lim_space);
      uint64_t its = MIN(nt * step_it, end_it - start_it);
      _assert(its > 0);

      mt->start_it += its;
      /* if this mt is not completed return it on the queue */
      if (mt->start_it < end_it) {
        mtm_return_mtask_queue(mt, wid);
      }

      enqueued += its;
      ut_used += nt;
      _assert(ut_used <= ut_avail);

      /* start actual tasks */
      uint64_t i = 0;
      for (i = 0; i < nt; i++)
        worker_push_task(wid, mt, start_it + i * step_it);
    }
    /* decrease estimation counter */
    mtm_decrease_total_its(enqueued);
    INCR_EVENT(WORKER_ITS_STARTED, enqueued);
    workers[wid].cnt_mtasks_check = 0;
  }
}

INLINE void worker_scheduler_state(uint32_t wid)
{
  if (config.print_sched_interv != 0 &&
      workers[wid].cnt_print_sched++ > config.print_sched_interv) {
    uint32_t i;
    uint32_t waiting_data = 0;
    uint32_t waiting_mtasks = 0;
    uint32_t waiting_handles = 0;
    uint32_t not_started = 0;
    uint32_t not_init = 0;
    uint32_t running = 0;
    uint32_t throttling = 0;
    uint64_t tot_nest_lev = 0;

    for (i = wid * NUM_UTHREADS_PER_WORKER;
        i < (wid + 1) * NUM_UTHREADS_PER_WORKER; i++) {
      switch (uthreads[i].tstatus) {
        case TASK_NOT_INIT:
          not_init++;
          break;
        case TASK_RUNNING:
          running++;
          break;
        case TASK_NOT_STARTED:
          not_started++;
          break;
        case TASK_WAITING_DATA:
          waiting_data++;
          break;
        case TASK_WAITING_MTASKS:
          waiting_mtasks++;
          break;
        case TASK_WAITING_HANLDE:
          waiting_handles++;
          break;
        case TASK_THROTTLING:
          throttling++;
          break;
        default:
          ERRORMSG("%s task status not recognized\n", __func__);
      }
      tot_nest_lev += uthreads[i].nest_lev;
    }

    uint64_t mtask_avail = mtm.num_mtasks_avail;
    char str[2048] = "\0";
    char *pstr = str;
    for (i = 0; i < num_nodes; i++) {
      char tmp[256];
      sprintf(tmp, "r%d*=%ld ", i, mtm.num_mtasks_res_array[i]);
      pstr = strcat(pstr, tmp);
    }

    _DEBUG(" Tasks: not_init %d, %u run, %u not_start, %u wait_data, "
        "%u wait_mtasks, %u wait_handles, %u throt, "
        "avg nest %.2f/%d, iters_todo* %ld "
        "- mtask_avail* %ld - %s (*=estimate)\n",
        not_init, running, not_started, waiting_data,
        waiting_mtasks, waiting_handles, throttling,
        ((float)tot_nest_lev / NUM_UTHREADS_PER_WORKER) + 1, MAX_NESTING,
        mtm_total_its(), mtask_avail, str);

    _assert(not_init + running + not_started + waiting_data +
        waiting_mtasks + waiting_handles + throttling ==
        NUM_UTHREADS_PER_WORKER);

    workers[wid].cnt_print_sched = 0;
  }
}

INLINE void worker_schedule(uint32_t tid, uint32_t wid) NO_INSTRUMENT;
INLINE void worker_schedule(uint32_t tid, uint32_t wid)
{
  _assert(wid < NUM_WORKERS);

  uthread_t *ut = NULL;
  if (uthread_queue_pop(&workers[wid].uthread_queue, &ut)) {
    _assert(ut != NULL);
    if (uthread_can_run(ut)) {
      /* if this code is run by a uthread, push it back on the queue 
       * and swap with the uthread from the queue (this code jumps 
       * from uthread to another uthread) */
      if (tid != (uint32_t) - 1) {
        uthread_queue_push(&workers[wid].uthread_queue, &uthreads[tid]);
        gmt_swapcontext(&uthreads[tid].ucontext, &ut->ucontext);
      } else {
        /* this is the worker itself scheduling a uthread 
         * (this piece of code jumps from worker context to a uthread) */
        gmt_swapcontext(&workers[wid].worker_ctxt, &ut->ucontext);
      }
    } else
      uthread_queue_push(&workers[wid].uthread_queue, ut);
  }

  /* check if a task can be started from the mtask queue */
  worker_check_mtask_queue(tid, wid);

  worker_scheduler_state(wid);

  /* only if we are running on multinode we have 
   * to flush cmd block at timeout */
  if (num_nodes > 1)
    agm_check_cmdb_timeout(wid, &workers[wid].tick_cmdb_timeout);
}

INLINE void worker_wait_data(uint32_t tid, uint32_t wid)
{
  if (num_nodes > 1) {
    uthread_t *ut = &uthreads[tid];
    _assert(ut->tstatus == TASK_RUNNING);
    ut->tstatus = TASK_WAITING_DATA;
    uint64_t timeout = 0;
    while (!uthread_check_recv_all_data(ut)) {
      timeout++;
      worker_schedule(tid, wid);
      if(timeout % 1000000000 == 0)
        GMT_DEBUG_PRINTF("waiting for data\n");

    }
    ut->tstatus = TASK_RUNNING;
    COUNT_EVENT(WORKER_WAIT_DATA);
  }
}

INLINE void worker_wait_mtasks(uint32_t tid, uint32_t wid)
{
  uthread_t *ut = &uthreads[tid];
  _assert(ut->tstatus == TASK_RUNNING);
  ut->tstatus = TASK_WAITING_MTASKS;
  while (!uthread_check_terminated_mtasks(ut))
    worker_schedule(tid, wid);
  ut->tstatus = TASK_RUNNING;
  COUNT_EVENT(WORKER_WAIT_MTASKS);
}

INLINE void worker_wait_handle(uint32_t tid, uint32_t wid, gmt_handle_t handle)
{
  _assert(handle != GMT_HANDLE_NULL);
  uthread_t *ut = &uthreads[tid];
  _assert(ut->tstatus == TASK_RUNNING);
  ut->tstatus = TASK_WAITING_HANLDE;

  uint64_t stick = rdtsc();
  while (mtm.handles[handle].status != HANDLE_COMPLETED) {
    if (num_nodes == 1 || !mtm.handles[handle].has_left_node) {
      if (mtm.handles[handle].mtasks_terminated ==
          mtm.handles[handle].mtasks_created)
        mtm.handles[handle].status = HANDLE_COMPLETED;
    } else {
      uint64_t tick = rdtsc();
      if ((tick - stick > config.handle_check_interv)
          && __sync_bool_compare_and_swap(&mtm.handles[handle].status,
            HANDLE_USED,
            HANDLE_CHECK_PENDING)
         ) {
        /*  Start the spawn termination check (phase 1 - check terminated) */
        uint32_t snode = (node_id == num_nodes - 1) ? 0 : node_id + 1;

        cmd_check_handle_t *cmd =
          (cmd_check_handle_t *) agm_get_cmd(snode, wid,
              sizeof
              (cmd_check_handle_t),
              0,
              NULL);

        cmd->type = GMT_CMD_HANDLE_CHECK_TERM;
        cmd->node_counter = 1;
        cmd->mtasks_terminated = mtm.handles[handle].mtasks_terminated;
        cmd->mtasks_created = 0;
        cmd->handle = handle;

        agm_set_cmd_data(snode, wid, NULL, 0);

        COUNT_EVENT(WORKER_WAIT_HANDLE);
        stick = tick;
      }
    }
    worker_schedule(tid, wid);
  }
  COUNT_EVENT(WORKER_WAIT_HANDLE);
  ut->tstatus = TASK_RUNNING;
  /* return handle */
  uint32_t gtid = uthread_get_gtid(tid, node_id);
  mtm_handle_push(handle, gtid);
}

INLINE bool worker_reserve_mtasks(uint32_t tid, uint32_t wid, uint32_t rnid)
{
  _assert(rnid != node_id);
  if (!mtm_acquire_reservation(rnid)) {
    if (mtm_lock_reservation(rnid)) {
      cmd_gen_t *cmd =
        (cmd_gen_t *) agm_get_cmd(rnid, wid, sizeof(cmd_gen_t), 0,
            NULL);
      cmd->type = GMT_CMD_MTASKS_RES_REQ;
      agm_set_cmd_data(rnid, wid, NULL, 0);

    }
    _assert(uthreads[tid].tstatus == TASK_RUNNING);
    uthreads[tid].tstatus = TASK_THROTTLING;
    worker_schedule(tid, wid);
    uthreads[tid].tstatus = TASK_RUNNING;
    return false;
  }
  return true;
}

INLINE mtask_t *worker_pop_mtask_pool(uint32_t wid)
{
  if (workers[wid].num_mt_res == 0) {
    uint32_t cnt = config.mtasks_res_block_loc;
    if (num_nodes > 1)
      cnt = mtm_reserve_mtask_block(cnt);

    workers[wid].num_mt_res = qmpmc_pop_n(&mtm.mtasks_pool,
        (void **)workers[wid].mt_res,
        cnt);
    if (num_nodes > 1 && workers[wid].num_mt_res != cnt)
      __sync_add_and_fetch(&mtm.num_mtasks_avail,
          cnt - workers[wid].num_mt_res);
  }

  if (workers[wid].num_mt_res == 0)
    return NULL;
  else
    return workers[wid].mt_res[--workers[wid].num_mt_res];
}

INLINE void worker_push_mtask_pool(uint32_t wid, mtask_t * mt)
{
  workers[wid].mt_ret[workers[wid].num_mt_ret++] = mt;
  if (workers[wid].num_mt_ret == config.mtasks_res_block_loc) {
    qmpmc_push_n(&mtm.mtasks_pool, (void **)workers[wid].mt_ret,
        workers[wid].num_mt_ret);
    workers[wid].num_mt_ret = 0;
    if (num_nodes > 1) {
      int64_t avail = __sync_add_and_fetch(&mtm.num_mtasks_avail,
          config.mtasks_res_block_loc);
      _assert(avail <= mtm.pool_size);
      _unused(avail);
    }
  }

}

INLINE void worker_do_for(void *func, uint64_t start_it, uint64_t step_it,
    const void *args, gmt_data_t gmt_array,
    gmt_handle_t handle)
{

  if (gmt_array != GMT_DATA_NULL) {
    ((gmt_for_each_func_t) func) (gmt_array, start_it, step_it, args,
      handle);
  } else {
    ((gmt_for_loop_func_t) func) (start_it, step_it, args, handle);
  }
}

INLINE void worker_do_execute(void *func,
    const void *args, uint32_t args_size,
    void *ret_buf, uint32_t * ret_size_ptr, gmt_handle_t handle)
{
  ((gmt_execute_func_t) func) (args, args_size, ret_buf, ret_size_ptr, handle);
}
#endif
