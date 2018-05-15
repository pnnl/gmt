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

#include "gmt/gmt.h"
#include "gmt/config.h"
#include "gmt/memory.h"
#include "gmt/debug.h"
#include "gmt/worker.h"
#include "gmt/aggregation.h"
#include "gmt/uthread.h"

#define GMT_TO_INITIALIZE UINT32_MAX

/**********************************************************************
  Primary API implementations 

  gmt_alloc
  gmt_free
  gmt_memcpy
  
  ************************************************************************/

GMT_INLINE gmt_data_t gmt_alloc(uint64_t num_elems, uint64_t bytes_per_elem,
                     alloc_type_t alloc_type, const char *array_name){
  gmt_data_t gmt_array = gmt_alloc_nb(num_elems, bytes_per_elem,
      alloc_type, array_name);
  gmt_wait_data();
  return gmt_array;
}

GMT_INLINE gmt_data_t gmt_alloc_nb(uint64_t num_elems, uint64_t bytes_per_elem,
                     alloc_type_t alloc_type, const char *array_name)
{

    if (num_elems == 0 || bytes_per_elem == 0) {
        _DEBUG("WARNING: trying to allocate an empty array.\n");
        return GMT_DATA_NULL;
    }

    uint32_t id = mem_get_alloc_id();
    gmt_data_t gmt_array = GMT_DATA_NULL;
    GD_SET_ID(gmt_array, id);
    GD_SET_NODE(gmt_array, node_id);
    GD_SET_TYPE(gmt_array, alloc_type);
    uint32_t name_len = 0;
    if (array_name != NULL)
        name_len = strlen(array_name);

//     _DEBUG("alloc id %ld - nbytes %ld  - type %d\n", 
//            GD_GET_GID(gmt_array), num_elems * bytes_per_elem, (int) alloc_type);

    switch (GD_GET_TYPE_DISTR(gmt_array)) {
      case GMT_ALLOC_PARTITION_FROM_ZERO:
        GD_SET_SNODE(gmt_array, 0);
        break;
      case GMT_ALLOC_LOCAL:
      case GMT_ALLOC_PARTITION_FROM_HERE:
        GD_SET_SNODE(gmt_array, node_id);
        break;
      case GMT_ALLOC_PARTITION_FROM_RANDOM:
        GD_SET_SNODE(gmt_array, gmt_rand() % num_nodes);
        break;
      default:
        break;
    }

    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);

    uint32_t i;
    for (i = 0; i < num_nodes; i++) {
      if (node_id != i) {
        uint32_t granted_bytes = 0;
        cmd_alloc_t *cmd;
        cmd =
          (cmd_alloc_t *) agm_get_cmd(i, wid,
              sizeof(cmd_alloc_t) + name_len,
              0, &granted_bytes);
        cmd->type = GMT_CMD_ALLOC;
        cmd->tid = tid;
        cmd->gmt_array = gmt_array;
        cmd->num_elems = num_elems;
        cmd->bytes_per_elem = bytes_per_elem;
        cmd->name_len = name_len;
        memcpy(cmd + 1, array_name, name_len);
        uthread_incr_req_nbytes(tid, sizeof(uint64_t));
        agm_set_cmd_data(i, wid, NULL, 0);
      }
    }
    mem_alloc(gmt_array, num_elems, bytes_per_elem, array_name, name_len);
    COUNT_EVENT(WORKER_GMT_ALLOC);
    return gmt_array;
}

GMT_INLINE void gmt_free(gmt_data_t gmt_array)
{
  if (gmt_array == GMT_DATA_NULL)
    return;

  //     _DEBUG("free %ld %d\n", GD_GET_GID(gmt_array), (int) GD_GET_TYPE_DISTR(gmt_array));
  uint32_t tid = uthread_get_tid();
  uint32_t wid = uthread_get_wid(tid);
  uint32_t i;
  for (i = 0; i < num_nodes; i++) {
    if (i != node_id) {
      cmd_free_t *cmd;
      cmd = (cmd_free_t *) agm_get_cmd(i, wid, sizeof(cmd_free_t),
          0, NULL);

      cmd->type = GMT_CMD_FREE;
      cmd->gmt_array = gmt_array;
      cmd->tid = tid;
      uthread_incr_req_nbytes(tid, sizeof(uint64_t));
      agm_set_cmd_data(i, wid, NULL, 0);
    }
  }
  mem_free(gmt_array);
  worker_wait_data(tid, wid);
  COUNT_EVENT(WORKER_GMT_FREE);
}

gmt_data_t gmt_attach(const char *name)
{
    if (name == NULL)
        return GMT_DATA_NULL;
    uint32_t i;
    for (i = 0; i < GMT_MAX_ALLOC_PER_NODE; i++) {
        gentry_t *ga = &mem.gentry[i];
        if (ga->name != NULL && (strcmp(ga->name, name) == 0))
            return ga->gmt_array;
    }
    return GMT_DATA_NULL;
}

GMT_INLINE uint64_t gmt_get_elem_bytes(gmt_data_t gmt_array)
{
    gentry_t *const ga = mem_get_gentry(gmt_array);
    _assert(ga != NULL);
    return ga->nbytes_elem;
}

typedef struct memcpy_func_args_t {
    gmt_data_t g_src;
    gmt_data_t g_dst;
    uint64_t g_src_offset;
    uint64_t g_dst_offset;
    uint64_t nbytes;
} memcpy_func_args_t;

/* function used with GMT execute to perform mem_copy */
uint32_t _memcpy_func(void *args, void *ret)
{
    memcpy_func_args_t *la = (memcpy_func_args_t *) args;
    int64_t l_src_offset = -1;
    gentry_t *const ga_src =
        mem_get_gentry(la->g_src);
    mem_check_last_byte(ga_src, la->g_src_offset + la->nbytes);
    _assert(ga_src != NULL);

    bool retvalue = mem_gmt_data_is_local(ga_src, la->g_src, la->g_src_offset,
                                          &l_src_offset);

    _unused(retvalue);
    _unused(ret);
    _assert(retvalue == true);
    _assert(l_src_offset != -1);

    _assert(la->nbytes <= ga_src->nbytes_loc - l_src_offset);
    gmt_put(la->g_dst, la->g_dst_offset, &ga_src->data[l_src_offset],
            la->nbytes);
    return 0;
}

GMT_INLINE void gmt_memcpy(gmt_data_t g_src, uint64_t g_src_offset,
                gmt_data_t g_dst, uint64_t g_dst_offset, uint64_t nbytes)
{
    uint64_t g_src_offset_cur = g_src_offset;
    uint64_t g_dst_offset_cur = g_dst_offset;
    uint64_t g_src_offset_end = g_src_offset + nbytes;

    gentry_t *const ga_src = mem_get_gentry(g_src);
    gentry_t *const ga_dst = mem_get_gentry(g_dst);
    mem_check_last_byte(ga_src, g_src_offset + nbytes);
    mem_check_last_byte(ga_dst, g_dst_offset + nbytes);

    /* prepare space for argument of possible remote execution */
    memcpy_func_args_t *args =
        (memcpy_func_args_t *) _malloc((num_nodes - 1) *
                                       sizeof(memcpy_func_args_t));

    //counter of remote executions
    uint32_t cnt = 0;


    while (g_src_offset_cur < g_src_offset_end) {

        uint64_t nbytes_remaining = g_src_offset_end - g_src_offset_cur;
        uint64_t avail_bytes = 0;
        int64_t l_src_offset = 0;

        // if source is local, we can solve this with a non blocking put 
        if (mem_gmt_data_is_local
            (ga_src, g_src, g_src_offset_cur, &l_src_offset)) {

            avail_bytes =
                MIN(nbytes_remaining, ga_src->nbytes_loc - l_src_offset);
            gmt_put_nb(g_dst, g_dst_offset_cur, &ga_src->data[l_src_offset],
                       avail_bytes);

            // if destination is local, we can solve this with a non blocking get 
        } else
            if (mem_gmt_data_is_local
                (ga_dst, g_dst, g_dst_offset_cur, &l_src_offset)) {
            avail_bytes =
                MIN(nbytes_remaining, ga_dst->nbytes_loc - l_src_offset);
            gmt_get_nb(g_src, g_src_offset_cur, &ga_dst->data[l_src_offset],
                       avail_bytes);

            // destination and source are both remote, we locate the node where 
            // the source is and we perform a remote memcpy_func on it
        } else {
            uint32_t rnode_id = 0;
            uint64_t roffset_bytes = 0;
            mem_locate_gmt_data_remote(ga_src, g_src_offset_cur,
                                       &rnode_id, &roffset_bytes);
            if (ga_src != NULL)
                avail_bytes =
                    MIN(nbytes_remaining, ga_src->nbytes_block - roffset_bytes);
            else
                avail_bytes = nbytes_remaining;

            _assert(cnt < num_nodes - 1);
            args[cnt].g_src = g_src;
            args[cnt].g_dst = g_dst;
            args[cnt].g_src_offset = g_src_offset_cur;
            args[cnt].g_dst_offset = g_dst_offset_cur;
            args[cnt].nbytes = avail_bytes;
            gmt_execute_on_node_nb(rnode_id, (gmt_execute_func_t) _memcpy_func,
                                   &args[cnt], sizeof(memcpy_func_args_t),
                                   NULL, NULL, GMT_PREEMPTABLE);
            cnt++;

        }
        g_src_offset_cur += avail_bytes;
        g_dst_offset_cur += avail_bytes;
    }
    gmt_wait_data();
    gmt_wait_execute_nb();

    //free space used by arguments of possible remote executions 
    free(args);
}
