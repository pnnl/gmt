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

/**********************************************************************
                 Primary API implementation of gmt_execute *
 ************************************************************************/

GMT_INLINE void execute_loop_warning(const char *str, long *start)
{
  long now = rdtsc(); 
  if( now - *start > 10000000000) {
    _DEBUG("WARNING: looping in %s\n", str);
    *start = now;
  }
}

GMT_INLINE void gmt_execute_on_node_with_handle(uint32_t rnid, 
   gmt_execute_func_t func,
   const void *args, uint32_t args_bytes,
   void *ret_buf_ptr,
   uint32_t * ret_size_ptr,
   preempt_policy_t policy,
   gmt_handle_t handle)
{
  long start = rdtsc();
  while(!gmt_try_execute_on_node_with_handle(rnid, func, args, args_bytes,
   ret_buf_ptr, ret_size_ptr, policy, handle)){
    execute_loop_warning(__func__, &start);
    gmt_yield();
  }
}

GMT_INLINE bool gmt_try_execute_on_node_with_handle(uint32_t rnid, 
   gmt_execute_func_t func,
   const void *args, uint32_t args_bytes,
   void *ret_buf_ptr,
   uint32_t * ret_size_ptr,
   preempt_policy_t policy,
   gmt_handle_t handle)
{
    if (args == NULL)
        _assert(args_bytes == 0);

    _assert(((uint64_t) func) >> VIRT_ADDR_PTR_BITS == 0);
    _assert(((uint64_t) ret_buf_ptr) >> VIRT_ADDR_PTR_BITS == 0);
    _assert(((uint64_t) ret_size_ptr) >> VIRT_ADDR_PTR_BITS == 0);

    if (rnid >= num_nodes)
        ERRORMSG("Remote node %d/%d not present", rnid, num_nodes);

    uint64_t max_args = gmt_max_args_per_task();
    if (args_bytes > max_args)
        ERRORMSG("Maximum size of arguments is", max_args);

    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);
    uint32_t gtid = uthread_get_gtid(tid, node_id);
    if (rnid == node_id) {
      mtask_t *mt = worker_pop_mtask_pool(wid);
      if (mt == NULL) { // Execute here
        //TODO:increase decrease nesting level before and after??
        worker_do_execute((void *)func, args, args_bytes,
            ret_buf_ptr, ret_size_ptr, handle);
        INCR_EVENT(WORKER_ITS_EXECUTE_LOCAL, 1);
      } else {
        if (handle == GMT_HANDLE_NULL)
          uthread_incr_created_mtasks(tid);
        else {
          mtm_handle_isvalid(handle, uthreads[tid].mt, gtid);
          mtm_handle_icr_mtasks_created(handle, 1);
        }
        mtm_push_mtask_queue(mt, func, args_bytes, args, gtid,
            uthread_get_nest_lev(tid), MTASK_EXECUTE, 
            0, 1, 1, GMT_DATA_NULL,
            ret_size_ptr, ret_buf_ptr,
            handle);
      }
    } else {
      if (policy == GMT_PREEMPTABLE) {
            /* if we can't reserve do not execute */
            if (!worker_reserve_mtasks(tid, wid, rnid)) {
                //TODO:increase decrease nesting level before and after??
                /*worker_do_execute((void *)func, args, ret_buf_ptr,
                                  ret_size_ptr, handle);*/
                return false;
            }
        }

        if (handle == GMT_HANDLE_NULL) {
            uthread_incr_created_mtasks(tid);
        } else {
            mtm_handle_isvalid(handle, uthreads[tid].mt, gtid);
            mtm_handle_icr_mtasks_created(handle, 1);
            mtm_handle_set_has_left_node(handle);
        }

        cmd_exec_t *cmd = (cmd_exec_t *) agm_get_cmd(rnid, wid,
                                                     sizeof(cmd_exec_t)
                                                     + args_bytes, 0, NULL);
        cmd->args_bytes = args_bytes;
        cmd->func_ptr = (uint64_t) func;
        cmd->ret_buf_ptr = (uint64_t) ret_buf_ptr;
        cmd->ret_size_ptr = (uint64_t) ret_size_ptr;
        cmd->pid = tid;
        cmd->nest_lev = uthread_get_nest_lev(tid);
        cmd->handle = handle;
        if (policy == GMT_PREEMPTABLE)
            cmd->type = GMT_CMD_EXEC_PREEMPT;
        else
            cmd->type = GMT_CMD_EXEC_NON_PREEMPT;
        /* copy args at the end of the cmd */
        memcpy(cmd + 1, args, args_bytes);
        agm_set_cmd_data(rnid, wid, NULL, 0);
        INCR_EVENT(WORKER_ITS_ENQUEUE_REMOTE, 1);
    }

    return true;
}

GMT_INLINE void gmt_execute_on_data_with_handle(gmt_data_t gmt_array, 
    uint64_t elem_offset,
    gmt_execute_func_t func, const void *args,
    uint32_t args_bytes, void *ret_buf,
    uint32_t * ret_size,
    preempt_policy_t policy,
    gmt_handle_t handle)
{
  long start = rdtsc();
  while(!gmt_try_execute_on_data_with_handle(gmt_array, elem_offset, func, 
        args, args_bytes, ret_buf, ret_size, policy, handle)){
    execute_loop_warning(__func__, &start);
    gmt_yield();
  }
}

GMT_INLINE bool gmt_try_execute_on_data_with_handle(gmt_data_t gmt_array, 
    uint64_t elem_offset,
    gmt_execute_func_t func, const void *args,
    uint32_t args_bytes, void *ret_buf,
    uint32_t * ret_size,
    preempt_policy_t policy,
    gmt_handle_t handle)
{
  uint32_t rnid = 0;
  gentry_t *const ga = mem_get_gentry(gmt_array);
  _assert(ga != NULL);
  uint64_t goffset_bytes = ga->nbytes_elem * elem_offset;
  if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, NULL))
    rnid = node_id;
  else
    mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, NULL);

  return gmt_try_execute_on_node_with_handle(rnid, 
      func, args, args_bytes, ret_buf,
      ret_size, policy, handle);
}

GMT_INLINE void gmt_wait_execute_nb()
{
    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);
    worker_wait_mtasks(tid, wid);
}

GMT_INLINE void gmt_execute_on_node_nb(uint32_t rnode_id,
    gmt_execute_func_t func,
    const void *args, uint32_t args_bytes,
    void *ret_buf, uint32_t * ret_size,
    preempt_policy_t policy)
{
  long start = rdtsc();
  while(!gmt_try_execute_on_node_nb( rnode_id, func, args, args_bytes,
    ret_buf, ret_size, policy)){
    execute_loop_warning(__func__, &start);
    gmt_yield();
  }
}

GMT_INLINE bool gmt_try_execute_on_node_nb(uint32_t rnode_id,
    gmt_execute_func_t func,
    const void *args, uint32_t args_bytes,
    void *ret_buf, uint32_t * ret_size,
    preempt_policy_t policy)
{
    return gmt_try_execute_on_node_with_handle(rnode_id, func, args, 
        args_bytes, ret_buf, ret_size, policy, GMT_HANDLE_NULL);
}

GMT_INLINE void gmt_execute_on_node(uint32_t rnode_id, 
    gmt_execute_func_t func,
    const void *args, uint32_t args_bytes,
    void *ret_buf, uint32_t * ret_size,
    preempt_policy_t policy)
{
  long start = rdtsc();
  while(!gmt_try_execute_on_node_nb(rnode_id, func, args, args_bytes,
        ret_buf, ret_size, policy)){
    execute_loop_warning(__func__, &start);
    gmt_yield();
  }
  gmt_wait_execute_nb();
}

GMT_INLINE bool gmt_try_execute_on_node(uint32_t rnode_id, 
      gmt_execute_func_t func,
      const void *args, uint32_t args_bytes,
      void *ret_buf, uint32_t * ret_size,
      preempt_policy_t policy)
{
    bool success = gmt_try_execute_on_node_nb(rnode_id, func, args, args_bytes,
        ret_buf, ret_size, policy);
    if(success)
      gmt_wait_execute_nb();
    return success;
}

GMT_INLINE void gmt_execute_on_data_nb(gmt_data_t gmt_array,
    uint64_t elem_offset,
    gmt_execute_func_t func, const void *args,
    uint32_t args_bytes, void *ret_buf,
    uint32_t * ret_size, preempt_policy_t policy)
{
  long start = rdtsc();
  while(!gmt_try_execute_on_data_nb(gmt_array, elem_offset, func,
        args, args_bytes, ret_buf, ret_size, policy)){
    execute_loop_warning(__func__, &start);
    gmt_yield();
  }
}

GMT_INLINE bool gmt_try_execute_on_data_nb(gmt_data_t gmt_array,
  uint64_t elem_offset,
   gmt_execute_func_t func, const void *args,
   uint32_t args_bytes, void *ret_buf,
   uint32_t * ret_size, preempt_policy_t policy)
{
    return gmt_try_execute_on_data_with_handle(gmt_array, elem_offset,
        func, args, args_bytes, ret_buf, ret_size, policy, GMT_HANDLE_NULL);
}

GMT_INLINE void gmt_execute_on_data(gmt_data_t gmt_array, 
    uint64_t elem_offset,
    gmt_execute_func_t func, const void *args,
    uint32_t args_bytes, void *ret_buf,
    uint32_t * ret_size, preempt_policy_t policy)
{
  long start = rdtsc();
  while(!gmt_try_execute_on_data_nb(gmt_array, elem_offset, func, args,
        args_bytes, ret_buf, ret_size, policy)){
    execute_loop_warning(__func__, &start);
    gmt_yield();
  }
  gmt_wait_execute_nb();
}

GMT_INLINE bool gmt_try_execute_on_data(gmt_data_t gmt_array, 
                         uint64_t elem_offset,
                         gmt_execute_func_t func, const void *args,
                         uint32_t args_bytes, void *ret_buf,
                         uint32_t * ret_size, preempt_policy_t policy)
{
    bool success = gmt_try_execute_on_data_nb(gmt_array,
        elem_offset, func, args, args_bytes, ret_buf, ret_size, policy);
    if(success)
      gmt_wait_execute_nb();
    return success;
}

GMT_INLINE void gmt_execute_on_all_with_handle(gmt_execute_func_t func,
    const void *args, uint32_t args_bytes,
    preempt_policy_t policy, gmt_handle_t handle) {
  uint32_t n;
  for( n = 0; n < gmt_num_nodes(); n++) {
    gmt_execute_on_node_with_handle(n, func, args, args_bytes, NULL,
        NULL, policy, handle);
  }
}

GMT_INLINE void gmt_execute_on_all_nb(gmt_execute_func_t func,
                         const void *args, uint32_t args_bytes,
                         preempt_policy_t policy) {
  uint32_t n;
  for( n = 0; n < gmt_num_nodes(); n++) {
    gmt_execute_on_node_nb(n, func, args, args_bytes, NULL, NULL, policy);
  }
}

GMT_INLINE void gmt_execute_on_all(gmt_execute_func_t func,
                         const void *args, uint32_t args_bytes,
                         preempt_policy_t policy) {
  gmt_execute_on_all_nb(func, args, args_bytes, policy);
  gmt_wait_execute_nb();
}
