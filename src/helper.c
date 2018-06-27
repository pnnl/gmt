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

#include <execinfo.h>
#include <sys/mman.h>
#include <stdbool.h>
#include "gmt/helper.h"
#include "gmt/worker.h"

#if !ENABLE_SINGLE_NODE_ONLY

volatile bool helper_stop_flag;
helper_t *helpers;

void helper_team_stop()
{
  while (helper_stop_flag) ;
  helper_stop_flag = true;
  uint32_t h;
  for (h = 0; h < NUM_HELPERS; h++)
    pthread_join(helpers[h].pthread, NULL);
}

INLINE uint64_t helper_check_aggreg_timeout(uint32_t hid, uint64_t old_tick)
{
#if ENABLE_AGGREGATION
  uint32_t i;
  uint64_t tick = rdtsc();
  if (tick - old_tick > helpers[hid].aggr_timeout_interval) {
    for (i = helpers[hid].part_start_node_id;
        i < helpers[hid].part_end_node_id; i++) {
      if (i == node_id)
        continue;

      /* Check node aggregation timeout */
      if (tick - agms[i].tick > helpers[hid].aggr_timeout_interval) {
        agm_aggregate_and_send(i, hid + NUM_WORKERS, true);
        /* record last time we tried to aggregate for this node */
        agms[i].tick = tick;
      }

      uint32_t loc_tasks = 0;
      uint32_t j;
      for (j = 0; j < NUM_WORKERS; j++)
        loc_tasks += uthread_queue_size(&workers[j].uthread_queue);

      helpers[hid].aggr_timeout_interval =
        MIN(loc_tasks * 1500, config.node_agg_check_interv);
    }
    old_tick = tick;
  }
  return old_tick;
#else
  _unused(hid);
  _unused(old_tick);
  return 0;
#endif
}

void *helper_loop(void *arg)
{
  uint32_t hid = (uint64_t) arg;

  if (config.thread_pinning) {
    uint32_t thread_id = get_thread_id();
    uint32_t core = select_core(thread_id, config.num_cores,
        config.stride_pinning);
    pin_thread(core);
    if (node_id == 0) {
      DEBUG0(printf("pining CPU %u with pthread_id %u\n",
            core, thread_id););
    }
  }

  uint32_t part_num_nodes = CEILING(num_nodes, NUM_HELPERS);
  helpers[hid].part_start_node_id = part_num_nodes * hid;
  helpers[hid].part_end_node_id = MIN(part_num_nodes * (hid + 1), num_nodes);

  uint64_t aggr_timeout = rdtsc();
  uint64_t cmdb_timeout = rdtsc();

  while (!helper_stop_flag) {
    /* check general  timeout */
    START_TIME(ts1);
    aggr_timeout = helper_check_aggreg_timeout(hid, aggr_timeout);
    END_TIME(ts1, HELPER_SERVICE_AGGR);

    START_TIME(ts2);
    agm_check_cmdb_timeout(hid + NUM_WORKERS, &cmdb_timeout);
    END_TIME(ts2, HELPER_SERVICE_CMDB);

    /* check for incoming buffers */
    START_TIME(ts3);
    helper_check_in_buffers(hid);
    END_TIME(ts3, HELPER_SERVICE_BUF);
  }
  pthread_exit(NULL);
}

void helper_team_run()
{
  helper_stop_flag = false;
  uint64_t i;

  pthread_attr_t attr;
  pthread_attr_init(&attr);

  for (i = 0; i < NUM_HELPERS; i++) {
    /* set stack address for this worker */
    void *stack_addr = (void *)(pt_stacks +
        (NUM_WORKERS + i) * PTHREAD_STACK_SIZE);
    int ret = pthread_attr_setstack(&attr, stack_addr, PTHREAD_STACK_SIZE);
    if (ret)
      perror("FAILED TO SET STACK PROPERTIES"), exit(EXIT_FAILURE);

    ret =
      pthread_create(&helpers[i].pthread, &attr, &helper_loop, (void *)i);
    if (ret)
      perror("FAILED TO CREATE HELPER"), exit(EXIT_FAILURE);
  }
}

void helper_team_init()
{
  helpers = (helper_t *)_malloc(sizeof(helper_t) * NUM_HELPERS);
  uint32_t i;
  for (i = 0; i < NUM_HELPERS; i++) {
    helpers[i].aggr_timeout_interval = config.node_agg_check_interv;
    netbuffer_init(&helpers[i].tmp_buff, 0, NULL);
    helpers[i].num_mt_res = 0;
    helpers[i].mt_res =
      (mtask_t **)_malloc(config.mtasks_res_block_loc * sizeof(mtask_t *));
  }
  helper_stop_flag = true;
}

void helper_team_destroy()
{
  uint32_t i;
  for (i = 0; i < NUM_HELPERS; i++)
    pthread_join(helpers[i].pthread, NULL);

  for (i = 0; i < NUM_HELPERS; i++)
    netbuffer_destroy(&helpers[i].tmp_buff);
  free(helpers);
}

INLINE void helper_enqueue_mtask(cmd_gen_t * gcmd, mtask_type_t type, 
    uint32_t gpid, uint32_t hid)
{
  mtask_t *mt = NULL;
  while (!qmpmc_pop(&mtm.mtasks_pool, (void **)&mt))
    /* while there is work in the queue */;

  // TODO add timeout warning message
  _assert(mt != NULL);

  switch (type) {
    case MTASK_EXECUTE:
      {
        cmd_exec_t *c = (cmd_exec_t *) gcmd;
        mtm_push_mtask_queue(mt, (void *)((uint64_t) c->func_ptr),
            c->args_bytes, c + 1, gpid, c->nest_lev,
            MTASK_EXECUTE, 0, 1, 1, GMT_DATA_NULL,
            (uint32_t *) ((uint64_t) (c->ret_size_ptr)),
            (void *)((uint64_t) c->ret_buf_ptr),
            c->handle, hid + config.num_workers);
      }
      break;
    case MTASK_FOR:
      {
        cmd_for_t *c = (cmd_for_t *) gcmd;
        mtm_push_mtask_queue(mt, (void *)((uint64_t) c->func_ptr),
            c->args_bytes, c + 1, gpid, c->nest_lev, MTASK_FOR, 
            c->it_start, c->it_end, c->it_per_task, c->gmt_array,
            NULL, NULL, c->handle, hid + config.num_workers);
      }
      break;
    default:
      ERRORMSG("ERROR mtask not recognized");
      break;
  }

}

INLINE void helper_send_rep_ack(uint32_t rnid, uint32_t hid, uint32_t tid)
{
  cmd32_t *c;
  c = (cmd32_t *) agm_get_cmd(rnid, hid + NUM_WORKERS,
      sizeof(cmd32_t), 0, NULL);
  c->type = GMT_CMD_REPLY_ACK;
  c->value = tid;
  agm_set_cmd_data(rnid, hid + NUM_WORKERS, NULL, 0);
}

INLINE void helper_send_rep_value(uint32_t rnid,
    uint32_t hid,
    uint32_t tid,
    uint64_t ret_value_ptr, uint64_t value)
{
  cmd_rep_value_t *c;
  c = (cmd_rep_value_t *) agm_get_cmd(rnid,
      hid + NUM_WORKERS,
      sizeof(cmd_rep_value_t), 0, NULL);
  c->type = GMT_CMD_REPLY_VALUE;
  c->tid = tid;
  c->ret_value_ptr = ret_value_ptr;
  c->value = value;
  agm_set_cmd_data(rnid, hid + NUM_WORKERS, NULL, 0);
}

INLINE void helper_check_in_buffers(uint32_t hid)
{
  net_buffer_t *recv_buff = comm_server_pop_recv_buff(hid);
  if (recv_buff == NULL) {
    sched_yield();
    return;
  }
  //int mtask_enq = 0;

#if ENABLE_HELPER_BUFF_COPY
  net_buffer_t *buff = &helpers[hid].tmp_buff;
  buff->num_bytes = recv_buff->num_bytes;
  buff->rnode_id = recv_buff->rnode_id;
  memcpy(buff->data, recv_buff->data, recv_buff->num_bytes);
  comm_server_push_recv_buff(hid);
#else
  net_buffer_t *buff = recv_buff;
#endif
  DEBUG0(printf
      ("n %d h %d received buffer of size %d from node %d\n", node_id, hid,
       buff->num_bytes, buff->rnode_id););
  uint8_t *cmds_ptr = buff->data;     /* pointer for commands */
  uint8_t *cmds_ptr_end = buff->data; /* end of commands for this block */
  uint8_t *data_ptr = buff->data;     /* pointer to data */
  uint32_t rnid = buff->rnode_id;
  /* reading buffer */
  while (data_ptr < buff->data + buff->num_bytes) {
    _assert(cmds_ptr == cmds_ptr_end);
    /* parsing block_info */
    block_info_t *bi = (block_info_t *) data_ptr;
    DEBUG0(printf
        ("n %d h %d parsed block_info cmds_bytes %u data_bytes %u"
         " cmds_ptr %lu data_ptr %lu buff->data %lu\n",
         node_id, hid, bi->cmds_bytes,
         bi->data_bytes, (uint64_t) cmds_ptr,
         (uint64_t) data_ptr, (uint64_t) buff->data););
    /* check that the info are correct */
    if (!(bi->cmds_bytes > 0))
      ERRORMSG
        ("n %u - h %u 0 - received buffer has an empty command!\n",
         node_id, hid);
    _assert(bi->cmds_bytes > 0);
    _assert(data_ptr +
        (sizeof(block_info_t) + bi->cmds_bytes + bi->data_bytes)
        <= buff->data + buff->num_bytes);
    /* setting cmds and data pointers */
    cmds_ptr = data_ptr + sizeof(block_info_t);
    cmds_ptr_end = cmds_ptr + bi->cmds_bytes;
    data_ptr = cmds_ptr_end;
    /* reading a block */
    while (cmds_ptr < cmds_ptr_end) {

      _assert(cmds_ptr < data_ptr);
      _assert(data_ptr <= cmds_ptr_end + bi->data_bytes);
      /* getting generic command */
      cmd_gen_t *gcmd = (cmd_gen_t *) cmds_ptr;
      switch (gcmd->type) {

        case GMT_CMD_FINALIZE:
          {
            worker_team_stop();
            cmds_ptr += sizeof(cmd_gen_t);
            COUNT_EVENT(HELPER_CMD_FINALIZE);
          }
          break;
        case GMT_CMD_ALLOC:
          {
            cmd_alloc_t *c = (cmd_alloc_t *) gcmd;
            mem_alloc(c->gmt_array, c->num_elems,
                c->bytes_per_elem, (char *)(c + 1), c->name_len);
            helper_send_rep_ack(rnid, hid, c->tid);
            cmds_ptr += sizeof(*c) + c->name_len;
            COUNT_EVENT(HELPER_CMD_ALLOC);
          }
          break;
        case GMT_CMD_FREE:
          {
            cmd_free_t *c = (cmd_free_t *) gcmd;
            mem_free(c->gmt_array);
            helper_send_rep_ack(rnid, hid, c->tid);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_FREE);
          }
          break;
        case GMT_CMD_ATOMIC_ADD:
          {
            cmd_atomic_add_t *c = (cmd_atomic_add_t *) gcmd;
            gentry_t *g = mem_get_gentry(c->gmt_array);
            uint8_t *p = mem_get_loc_ptr(g, c->offset, g->nbytes_elem);
            int64_t ret = mem_atomic_add(p, c->value, g->nbytes_elem);
            helper_send_rep_value(rnid, hid, c->tid,
                c->ret_value_ptr, ret);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_ATOMIC_ADD);
          }
          break;
        case GMT_CMD_ATOMIC_CAS:
          {
            cmd_atomic_cas_t *c = (cmd_atomic_cas_t *) gcmd;
            gentry_t *g = mem_get_gentry(c->gmt_array);
            uint8_t *p = mem_get_loc_ptr(g, c->offset, g->nbytes_elem);
            int64_t ret = mem_atomic_cas(p, c->old_value, c->new_value,
                g->nbytes_elem);
            helper_send_rep_value(rnid, hid, c->tid,
                c->ret_value_ptr, ret);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_ATOMIC_CAS);
          }
          break;
        case GMT_CMD_PUT:
          {
            cmd_put_t *c = (cmd_put_t *) gcmd;
            _assert(c->put_bytes > 0);
            gentry_t *g = mem_get_gentry(c->gmt_array);
            uint8_t *p = mem_get_loc_ptr(g, c->offset, c->put_bytes);
            mem_put(p, data_ptr, c->put_bytes);
            helper_send_rep_ack(rnid, hid, c->tid);
            cmds_ptr += sizeof(*c);
            data_ptr += c->put_bytes;
            COUNT_EVENT(HELPER_CMD_PUT);
          }
          break;
        case GMT_CMD_PUT_VALUE:
          {
            cmd_put_value_t *c = (cmd_put_value_t *) gcmd;
            gentry_t *g = mem_get_gentry(c->gmt_array);
            uint8_t *p = mem_get_loc_ptr(g, c->offset, g->nbytes_elem);
            mem_put_value(p, c->value, g->nbytes_elem);
            helper_send_rep_ack(rnid, hid, c->tid);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_PUT_VALUE);
          }
          break;
        case GMT_CMD_GET:
          {
            cmd_get_t *c = (cmd_get_t *) gcmd;
            gentry_t *g = mem_get_gentry(c->gmt_array);
            uint8_t *data = mem_get_loc_ptr(g, c->offset, c->get_bytes);
            uint64_t boffset = 0;
            while (boffset < c->get_bytes) {
              uint32_t granted_nbytes = 0;
              cmd_rep_get_t *cr;
              cr = (cmd_rep_get_t *) agm_get_cmd(rnid,
                  hid + NUM_WORKERS,
                  sizeof
                  (cmd_rep_get_t),
                  c->get_bytes -
                  boffset,
                  &granted_nbytes);
              _assert(granted_nbytes > 0
                  && granted_nbytes <= COMM_BUFFER_SIZE);
              cr->type = GMT_CMD_REPLY_GET;
              cr->tid = c->tid;
              cr->ret_data_ptr = c->ret_data_ptr + boffset;
              cr->get_bytes = granted_nbytes;
              agm_set_cmd_data(rnid, hid + NUM_WORKERS,
                  data + boffset, granted_nbytes);
              boffset += granted_nbytes;
            }
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_GET);
          }
          break;
        case GMT_CMD_EXEC_PREEMPT:
          {
            cmd_exec_t *c = (cmd_exec_t *) gcmd;
            //mtask_enq++;
            helper_enqueue_mtask(gcmd, MTASK_EXECUTE,
                uthread_get_gtid(c->pid, rnid), hid);
            cmds_ptr += sizeof(*c) + c->args_bytes;
            COUNT_EVENT(HELPER_CMD_EXEC_PREEMPT);
          }
          break;
        case GMT_CMD_EXEC_NON_PREEMPT:
          {
            cmd_exec_t *c = (cmd_exec_t *) gcmd;
            void *args = (c->args_bytes > 0) ? (void *)(c + 1) : NULL;
            uint32_t ret_size_value = 0;
            /* return buffer */
            uint8_t buf[UTHREAD_MAX_RET_SIZE];
            void * loc_buf = NULL, *loc_ret_size = NULL;
            if(((void *)(uint64_t)c->ret_buf_ptr) != NULL){
              loc_buf = buf;
              loc_ret_size = &ret_size_value;
            }

            worker_do_execute((void *)((uint64_t) c->func_ptr),
                args, c->args_bytes, loc_buf, (uint32_t *)loc_ret_size,
                GMT_HANDLE_NULL);
            //c->handle);                    
            helper_send_exec_completed(rnid, hid + NUM_WORKERS,
                c->pid, c->nest_lev,
                c->ret_buf_ptr,
                c->ret_size_ptr,
                ret_size_value,
                (uint8_t*)loc_buf, c->handle);
            cmds_ptr += sizeof(*c) + c->args_bytes;
            COUNT_EVENT(HELPER_CMD_EXEC_NON_PREEMPT);
          }
          break;
        case GMT_CMD_FOR:
          {
            cmd_for_t *c = (cmd_for_t *) gcmd;
            helper_enqueue_mtask(gcmd, MTASK_FOR,
                uthread_get_gtid(c->pid, rnid), hid);
            //mtask_enq++;
            cmds_ptr += sizeof(*c) + c->args_bytes;
            COUNT_EVENT(HELPER_CMD_FOR_LOOP);
          }
          break;
        case GMT_CMD_MTASKS_RES_REQ:
          {
            /*sending itb_reply */
            cmd64_t *rc;
            rc = (cmd64_t *) agm_get_cmd(rnid, hid + NUM_WORKERS,
                sizeof(cmd64_t), 0, NULL);
            rc->type = GMT_CMD_MTASKS_RES_REPLY;
            rc->value =
              mtm_reserve_mtask_block(config.mtasks_res_block_rem);
            agm_set_cmd_data(rnid, hid + NUM_WORKERS, NULL, 0);
            cmds_ptr += sizeof(cmd_gen_t);
            COUNT_EVENT(HELPER_CMD_MTASKS_RES_REQ);
          }
          break;
        case GMT_CMD_MTASKS_RES_REPLY:
          {
            cmd64_t *c = (cmd64_t *) gcmd;
            if (c->value > 0)
              mtm_mark_reservation_block(rnid, c->value);
            mtm_unlock_reservation(rnid);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_MTASKS_RES_REPLY);
          }
          break;
        case GMT_CMD_HANDLE_CHECK_TERM:
          {
            cmd_check_handle_t *c = (cmd_check_handle_t *) gcmd;
            _assert(c->node_counter <= num_nodes);
            uint32_t nnode =
              (node_id == num_nodes - 1) ? 0 : node_id + 1;
            cmd_check_handle_t *rc =
              (cmd_check_handle_t *) agm_get_cmd(nnode,
                  hid + NUM_WORKERS,
                  sizeof
                  (cmd_check_handle_t),
                  0, NULL);
            /* phase 1 - doing check terminated 
               increment and forward to next node */
            if (c->node_counter < num_nodes) {
              memcpy(rc, c, sizeof(*rc));
              rc->node_counter++;
              rc->mtasks_terminated +=
                mtm.handles[c->handle].mtasks_terminated;
            } else {    /* start phase 2 */
              rc->type = GMT_CMD_HANDLE_CHECK_CREAT;
              rc->node_counter = 1;
              rc->mtasks_created =
                mtm.handles[c->handle].mtasks_created;
              /* copy terminated from current phase */
              rc->mtasks_terminated = c->mtasks_terminated;
              rc->handle = c->handle;
            }
            agm_set_cmd_data(nnode, hid + NUM_WORKERS, NULL, 0);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_CHECK_HANDLE_REQ);
          }
          break;
        case GMT_CMD_HANDLE_CHECK_CREAT:
          {
            cmd_check_handle_t *c = (cmd_check_handle_t *) gcmd;
            _assert(c->node_counter <= num_nodes);
            /* Phase 2 - doing check created 
               increment and forward to next node */
            if (c->node_counter < num_nodes) {
              uint32_t nnode =
                (node_id == num_nodes - 1) ? 0 : node_id + 1;
              cmd_check_handle_t *rc =
                (cmd_check_handle_t *) agm_get_cmd(nnode,
                    hid +
                    NUM_WORKERS,
                    sizeof
                    (cmd_check_handle_t),
                    0, NULL);
              memcpy(rc, c, sizeof(*rc));
              rc->node_counter++;
              rc->mtasks_created +=
                mtm.handles[c->handle].mtasks_created;
              agm_set_cmd_data(nnode, hid + NUM_WORKERS, NULL, 0);
            } else {
              /* check completed */
                _assert(mtm.handles[c->handle].status ==
                  HANDLE_CHECK_PENDING);
              /* check handler value */
              _assert(c->handle >=
                  node_id * config.max_handles_per_node);
              _assert(c->handle <
                  (node_id + 1) * config.max_handles_per_node);
              bool ret;
              _unused(ret);
              if (c->mtasks_terminated == c->mtasks_created) {
                ret =
                  __sync_bool_compare_and_swap(&mtm.handles
                      [c->handle].status,
                      HANDLE_CHECK_PENDING,
                      HANDLE_RESET);
                uint32_t nnode =
                  (node_id == num_nodes - 1) ? 0 : node_id + 1;
                cmd_check_handle_t *rc =
                  (cmd_check_handle_t *) agm_get_cmd(nnode,
                      hid + NUM_WORKERS,
                      sizeof(cmd_check_handle_t),
                      0, NULL);
                memcpy(rc, c, sizeof(*rc));
                rc->type = GMT_CMD_HANDLE_RESET;
                rc->node_counter=1;
                agm_set_cmd_data(nnode, hid + NUM_WORKERS, NULL, 0);
              } else {
                ret =
                  __sync_bool_compare_and_swap(&mtm.handles
                      [c->handle].status,
                      HANDLE_CHECK_PENDING,
                      HANDLE_USED);
              }
              _assert(ret);
            }

            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_CHECK_HANDLE_REPLY);
          }
          break;
        case GMT_CMD_HANDLE_RESET:
          {
            cmd_check_handle_t *c = (cmd_check_handle_t *) gcmd;
            _assert(c->node_counter <= num_nodes);
            bool ret; 
            _unused(ret);
            if (c->node_counter < num_nodes) {
              uint32_t nnode =
                (node_id == num_nodes - 1) ? 0 : node_id + 1;
              cmd_check_handle_t *rc =
                (cmd_check_handle_t *) agm_get_cmd(nnode,
                    hid +
                    NUM_WORKERS,
                    sizeof
                    (cmd_check_handle_t),
                    0, NULL);
              memcpy(rc, c, sizeof(*rc));
              rc->node_counter++;

              mtm.handles[c->handle].mtasks_created=0;
              mtm.handles[c->handle].mtasks_terminated=0;

              agm_set_cmd_data(nnode, hid+NUM_WORKERS, NULL,0);
              ret = true;
            } else {
              mtm.handles[c->handle].mtasks_created=0;
              mtm.handles[c->handle].mtasks_terminated=0;
              ret =
                __sync_bool_compare_and_swap(&mtm.handles
                      [c->handle].status,
                      HANDLE_RESET,
                      HANDLE_COMPLETED);
            }
            _assert(ret);
            cmds_ptr += sizeof(*c);
          }
          break;
        case GMT_CMD_FOR_COMPL:
          {
            cmd_for_compl_t *c = (cmd_for_compl_t *) gcmd;
            uthread_incr_terminated_mtasks(c->tid, c->nest_lev);
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_FOR_COMPL);
          }
          break;
        case GMT_CMD_EXEC_COMPL:
          {
            cmd_exec_compl_t *c = (cmd_exec_compl_t *) gcmd;
            uint32_t *ptr = (uint32_t *) ((uint64_t) c->ret_size_ptr);
            if (ptr != NULL)
              *(ptr) = c->ret_size_value;
            /* copy return buffer */
            if (c->ret_size_value > 0) {
              uint8_t *ptr = (uint8_t *) ((uint64_t) c->ret_buf_ptr);
              _assert(ptr != NULL);
              memcpy(ptr, c + 1, c->ret_size_value);
            }
            if (c->handle == GMT_HANDLE_NULL)
              uthread_incr_terminated_mtasks(c->pid, c->nest_lev);
            else
               mtm_handle_icr_mtasks_terminated(c->handle, 1);
            cmds_ptr += sizeof(*c) + c->ret_size_value;
            COUNT_EVENT(HELPER_CMD_EXEC_COMPL);
          }
          break;
          /* reply commands */
        case GMT_CMD_REPLY_ACK:
          {
            cmd32_t *c = (cmd32_t *) gcmd;
            uthread_incr_recv_nbytes(c->value, sizeof(uint64_t));
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_REPLY_ACK);
          }
          break;
        case GMT_CMD_REPLY_VALUE:
          {
            cmd_rep_value_t *c = (cmd_rep_value_t *) gcmd;
            uint64_t *ptr = (uint64_t *) ((uint64_t) c->ret_value_ptr);
            if (ptr != NULL)
              *ptr = c->value;
            uthread_incr_recv_nbytes(c->tid, sizeof(uint64_t));
            cmds_ptr += sizeof(*c);
            COUNT_EVENT(HELPER_CMD_REPLY_VALUE);
          }
          break;
        case GMT_CMD_REPLY_GET:
          {
            cmd_rep_get_t *c = (cmd_rep_get_t *) gcmd;
            uint8_t *ptr = (uint8_t *) ((uint64_t) c->ret_data_ptr);
            memcpy(ptr, data_ptr, c->get_bytes);
            uthread_incr_recv_nbytes(c->tid, c->get_bytes);
            cmds_ptr += sizeof(*c);
            data_ptr += c->get_bytes;
            COUNT_EVENT(HELPER_CMD_REPLY_GET);
          }
          break;
        default:
          {
            ERRORMSG("n %d h %d - Command %d not recognized\n",
                node_id, hid, gcmd->type);
          }
      }
    }
  }
  DEBUG0(printf
      ("n %d h %d - processing done of buffer of size %d\n",
       node_id, hid, buff->num_bytes););
#if  !ENABLE_HELPER_BUFF_COPY
  comm_server_push_recv_buff(hid);
#endif
  //     if (mtask_enq > 0)
  //         _DEBUG("mtask_enq %d\n", mtask_enq);
}

#endif
