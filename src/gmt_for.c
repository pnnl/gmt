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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdbool.h>

#include "gmt/gmt.h"
#include "gmt/worker.h"
#include "gmt/mtask.h"
#include "gmt/memory.h"
#include "gmt/utils.h"

/**********************************************************************
  Primary API implementations 
 ************************************************************************/

GMT_INLINE gmt_handle_t gmt_get_handle()
{
  uint32_t tid = uthread_get_tid();
  uint32_t gtid = uthread_get_gtid(tid, node_id);
  INCR_EVENT(WORKER_GMT_GET_HANDLE,1);
  return mtm_handle_pop(gtid);    
}

GMT_INLINE void gmt_wait_handle(gmt_handle_t handle)
{
  if (handle == GMT_HANDLE_NULL)
    return;
  uint32_t tid = uthread_get_tid();
  uint32_t wid = uthread_get_wid(tid);
  worker_wait_handle(tid, wid, handle);
}

static inline void for_at(uint32_t tid, uint32_t wid, uint32_t rnid,
    gmt_data_t gmt_array, uint64_t it_start,
    uint64_t it_end, uint32_t it_per_task,
    void *func, const void *args,
    uint64_t args_bytes, gmt_handle_t handle)
{
  if (args == NULL)
    _assert(args_bytes == 0);
  _assert(((uint64_t) func) >> VIRT_ADDR_PTR_BITS == 0);
  _assert(it_start >> ITER_BITS == 0);
  _assert(it_end >> ITER_BITS == 0);

  if (rnid == node_id) {
    mtask_t *mt = NULL;
    while (it_start < it_end && 
        ((mt = worker_mtask_alloc(wid)) == NULL)) {
      uint64_t its = MIN(it_per_task, it_end - it_start);
      //TODO:increase decrease nesting level before and after??               
      worker_do_for(func, it_start, its, args, gmt_array, handle);
      it_start += its;
      INCR_EVENT(WORKER_ITS_SELF_EXECUTE, its);
    }

    if (it_start < it_end) {
      const uint32_t gtid = uthread_get_gtid(tid, node_id);
      if (handle == GMT_HANDLE_NULL)
        uthread_incr_created_mtasks(tid);
      else {
        mtm_handle_isvalid(handle, uthreads[tid].mt, gtid);
        mtm_handle_icr_mtasks_created(handle, 1);
      }
      mtm_schedule_mtask(mt, func, args_bytes, args, gtid,
          uthread_get_nest_lev(tid), MTASK_FOR, 
          it_start, it_end, it_per_task, gmt_array,
          NULL, NULL,
          handle, wid);
    }
  } else {
#if !NO_RESERVE
    /* if we can't reserve execute a step locally */
    while (it_start < it_end && !worker_reserve_mtasks(tid, wid, rnid)) {
      uint64_t its = MIN(it_per_task, it_end - it_start);
      //TODO:increase decrease nesting level before and after??          
      worker_do_for(func, it_start, its, args, gmt_array, handle);
      it_start += its;
      INCR_EVENT(WORKER_ITS_SELF_EXECUTE, its);
    }
#endif

    if (it_start < it_end) {
      const uint32_t gtid = uthread_get_gtid(tid, node_id);
      if (handle == GMT_HANDLE_NULL) {
        uthread_incr_created_mtasks(tid);
      } else {
        mtm_handle_isvalid(handle, uthreads[tid].mt, gtid);
        mtm_handle_icr_mtasks_created(handle, 1);
        mtm_handle_set_has_left_node(handle);
      }
      cmd_for_t *cmd = (cmd_for_t *) agm_get_cmd(rnid, wid,
          sizeof(cmd_for_t) +
          args_bytes,
          0, NULL);
      cmd->handle = handle;
      cmd->func_ptr = (uint64_t) func;
      cmd->args_bytes = args_bytes;
      cmd->it_start = it_start;
      cmd->it_end = it_end;
      cmd->it_per_task = it_per_task;
      cmd->pid = tid;
      cmd->nest_lev = uthread_get_nest_lev(tid);
      cmd->type = GMT_CMD_FOR;
      cmd->gmt_array = gmt_array;
      /* copy args at the end of the cmd */
      memcpy(cmd + 1, args, args_bytes);
      agm_set_cmd_data(rnid, wid, NULL, 0);
      INCR_EVENT(WORKER_ITS_ENQUEUE_REMOTE, it_end - it_start);
    }
  }
}

GMT_INLINE void gmt_for_each_with_handle(gmt_data_t gmt_array,
    uint64_t el_per_task, uint64_t elems_offset, uint64_t num_elems,
    gmt_for_each_func_t func, const void *args,
    uint64_t args_bytes, gmt_handle_t handle)
{
  const uint32_t tid = uthread_get_tid();
  const uint32_t wid = uthread_get_wid(tid);
  gentry_t *const ga = mem_get_gentry(gmt_array);
  if (ga == NULL) {
    // TODO this should be supported
    ERRORMSG("FOR_EACH not supported on gmt_array allocated with"
        "GMT_ALLOC_LOCAL");
  }

  if(num_elems > ga->nbytes_tot / ga->nbytes_elem){
    GMT_DEBUG_PRINTF("Error num_elems %lu array elem %lu\n",
        num_elems,  ga->nbytes_tot / ga->nbytes_elem);
    ERRORMSG("num_elems exceeds array size\n");
  }

  if(num_elems ==  0)
    return;

  if (args != NULL && args_bytes == 0)
    ERRORMSG("Cannot be args != NULL and args_bytes == 0\n");

  if (args == NULL && args_bytes > 0)
    ERRORMSG("Cannot be args == NULL && args_bytes > 0\n");

  uint32_t i;
  for (i = 0; i < num_nodes; i++) {
    uint64_t nbytes_loc, nbytes_block, goffset_bytes;
    /* understand how many bytes node "i" has */
    block_partition(i, ga->nbytes_tot/ga->nbytes_elem,
        ga->nbytes_elem, gmt_array,
        &nbytes_loc, &nbytes_block, &goffset_bytes);

    if (nbytes_loc == 0)
      continue;

    _assert(goffset_bytes % ga->nbytes_elem == 0);
    _assert(nbytes_loc % ga->nbytes_elem == 0);
    uint64_t el_start = goffset_bytes / ga->nbytes_elem;
    uint64_t el_end = el_start + nbytes_loc / ga->nbytes_elem;
    _assert(el_start < el_end);
     //  _DEBUG("node %u el_start %ld - el_end %ld \n", i, el_start,el_end);
    if(elems_offset < el_end && elems_offset + num_elems > el_start) {
      uint64_t it_start = MAX(el_start, elems_offset);
      uint64_t it_end = MIN(el_end, elems_offset + num_elems);
      //  _DEBUG("node %u it_start %ld - it_end %ld \n", i, it_start,it_end);
      for_at(tid, wid, i, gmt_array, it_start, it_end, el_per_task,
          (void *)func, args, args_bytes, handle);
    }
  }
}

GMT_INLINE void gmt_for_each(gmt_data_t gmt_array, uint64_t elems_per_task,
    uint64_t elems_offset, uint64_t num_elems,
    gmt_for_each_func_t func, const void *args,
    uint64_t args_bytes)
{
  gmt_for_each_with_handle(gmt_array, elems_per_task, 
      elems_offset, num_elems, func, args, 
      args_bytes, GMT_HANDLE_NULL);
  gmt_wait_for_nb();
}

GMT_INLINE void gmt_for_each_nb(gmt_data_t gmt_array, uint64_t elems_per_task,
    uint64_t elems_offset, uint64_t num_elems,
    gmt_for_each_func_t func, const void *args,
    uint64_t args_bytes)
{
  gmt_for_each_with_handle(gmt_array, elems_per_task, 
      elems_offset, num_elems, func, args,
      args_bytes, GMT_HANDLE_NULL);
}

static inline uint32_t get_num_nodes_used(spawn_policy_t policy)
{
  if (num_nodes == 1)
    return 1;

  switch (policy) {
    case GMT_SPAWN_REMOTE:
      return num_nodes - 1;
    case GMT_SPAWN_LOCAL:
      return 1;
    case GMT_SPAWN_PARTITION_FROM_ZERO:
    case GMT_SPAWN_PARTITION_FROM_RANDOM:
    case GMT_SPAWN_PARTITION_FROM_HERE:
    case GMT_SPAWN_SPREAD:
      return num_nodes;
    default:
      ERRORMSG(" spawn policy not recognized\n");
  }
  return (uint32_t) - 1;
}

static inline uint32_t get_start_node(spawn_policy_t policy)
{
  if (num_nodes == 1)
    return node_id;

  switch (policy) {
    case GMT_SPAWN_REMOTE:
    case GMT_SPAWN_LOCAL:
    case GMT_SPAWN_SPREAD:
    case GMT_SPAWN_PARTITION_FROM_ZERO:
      return 0;
    case GMT_SPAWN_PARTITION_FROM_RANDOM:
      return gmt_rand() % num_nodes;
    case GMT_SPAWN_PARTITION_FROM_HERE:
      return node_id;
    default:
      ERRORMSG(" spawn policy not recognized\n");
  }
}

static inline uint32_t get_next_node(uint32_t start_node, uint64_t idx,
    uint32_t num_nodes_used,
    spawn_policy_t policy)
{
  uint32_t node;
  switch (policy) {
    case GMT_SPAWN_REMOTE:
      if (idx >= node_id)
        node = (idx + 1) % num_nodes_used;
      else
        node = idx;
      break;
    case GMT_SPAWN_LOCAL:
      node = node_id;
      break;
    case GMT_SPAWN_PARTITION_FROM_ZERO:
    case GMT_SPAWN_PARTITION_FROM_RANDOM:
    case GMT_SPAWN_PARTITION_FROM_HERE:
      node = (start_node + idx) % num_nodes;
      break;
    case GMT_SPAWN_SPREAD:
      {
        uint32_t stride = num_nodes / num_nodes_used;
        uint32_t extra = num_nodes % num_nodes_used;
        if (idx < extra)
          node = (start_node + idx * stride + idx) % num_nodes;
        else
          node = (start_node + idx * stride + extra) % num_nodes;
        break;
      }
    default:
      ERRORMSG(" spawn policy not recognized\n");
  }
  return node;
}

GMT_INLINE void gmt_for_loop_with_handle(uint64_t num_it, uint32_t it_per_task,
    gmt_for_loop_func_t func,
    const void *args, uint32_t args_bytes,
    spawn_policy_t policy, const gmt_handle_t handle)
{
  if (num_it == 0 || it_per_task == 0)
    return;
  const uint32_t tid = uthread_get_tid();
  const uint32_t wid = uthread_get_wid(tid);

  uint32_t n_nodes = get_num_nodes_used(policy);
  uint32_t s_node = get_start_node(policy);
  uint64_t n_tasks = CEILING(num_it, it_per_task);

  if (config.limit_parallelism) {
    /* Limit parallelism to create no more tasks than maximum dimension of 
       parallelism in the system */
    if (n_tasks > NUM_WORKERS * NUM_UTHREADS_PER_WORKER * n_nodes) {
      n_tasks = NUM_WORKERS * NUM_UTHREADS_PER_WORKER * n_nodes;
      /* adjust it_per_task to account for actual n_tasks */
      it_per_task = CEILING(num_it, n_tasks);
    }
  }
  n_nodes = MIN(n_tasks, n_nodes);
  uint32_t tpn = CEILING(n_tasks, n_nodes);   //tasks per node

  //     _DEBUG
  //         ("nit %ld - itpt %ld - n_tasks %ld - n_nodes %d - s_node %d - tpn %d - handle %d\n",
  //          num_it, it_per_task, n_tasks, n_nodes, s_node, tpn, handle);
  _assert(tpn * n_nodes >= n_tasks);
  uint32_t i;
  for (i = 0; i < n_nodes; i++) {
    uint32_t n = get_next_node(s_node, i, n_nodes, policy);
    /* get start and end iteration of this mtask */
    uint64_t it_start = i * tpn * it_per_task;
    uint64_t it_end = MIN(num_it, (i + 1) * tpn * it_per_task);
    for_at(tid, wid, n, GMT_DATA_NULL, it_start, it_end, it_per_task,
        (void *)func, args, args_bytes, handle);
  }
}

GMT_INLINE void gmt_for_loop(uint64_t num_it, uint32_t step_it,
    gmt_for_loop_func_t func, const void *args,
    uint32_t args_bytes, spawn_policy_t policy)
{
  gmt_for_loop_with_handle(num_it, step_it, func, args, args_bytes,
      policy, GMT_HANDLE_NULL);
  gmt_wait_for_nb();
}

GMT_INLINE void gmt_for_loop_nb(uint64_t num_it, uint32_t step_it,
    gmt_for_loop_func_t func, const void *args,
    uint32_t args_bytes, spawn_policy_t policy)
{
  gmt_for_loop_with_handle(num_it, step_it, func, args, args_bytes,
      policy, GMT_HANDLE_NULL);
}

GMT_INLINE void gmt_for_loop_on_node_with_handle(
    uint32_t rnid, uint64_t num_it, uint32_t it_per_task,
    gmt_for_loop_func_t func, const void *args, uint32_t args_bytes,
    const gmt_handle_t handle)
{
  if (num_it == 0 || it_per_task == 0)
    return;

  const uint32_t tid = uthread_get_tid();
  const uint32_t wid = uthread_get_wid(tid);

  for_at(tid, wid, rnid, GMT_DATA_NULL, 0, num_it, it_per_task,
         (void *)func, args, args_bytes, handle);
}

GMT_INLINE void gmt_for_loop_on_node(
    uint32_t rnid, uint64_t num_it, uint32_t it_per_task,
    gmt_for_loop_func_t func, const void *args, uint32_t args_bytes)
{
  gmt_for_loop_on_node_with_handle(
      rnid, num_it, it_per_task, func, args, args_bytes, GMT_HANDLE_NULL);
  gmt_wait_for_nb();
}

GMT_INLINE void gmt_for_loop_on_node_nb(
    uint32_t rnid, uint64_t num_it, uint32_t it_per_task,
    gmt_for_loop_func_t func, const void *args, uint32_t args_bytes)
{
  gmt_for_loop_on_node_with_handle(
      rnid, num_it, it_per_task, func, args, args_bytes, GMT_HANDLE_NULL);
}

GMT_INLINE void gmt_wait_for_nb()
{
  uint32_t tid = uthread_get_tid();
  uint32_t wid = uthread_get_wid(tid);
  worker_wait_mtasks(tid, wid);
}


