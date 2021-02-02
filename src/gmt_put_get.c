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
#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/worker.h"
#include "gmt/aggregation.h"
#include "gmt/memory.h"
#include "gmt/uthread.h"

#define GMT_TO_INITIALIZE UINT32_MAX

/**********************************************************************
  Primary API implementations 
    
    gmt_put_nb    
    gmt_put_bytes_nb
    gmt_put_value_nb
    gmt_get_nb
    gmt_get_bytes_nb
    
    gmt_put     
    gmt_put_bytes
    gmt_put_value 
    gmt_get 
    gmt_get_bytes
    
    gnt_gather_nb
    gnt_scatter_nb

    gmt_gather
    gmt_scatter

    gmt_get_local_ptr
    
    gmt_atomic_add_nb
    gmt_atomic_cas_nb
    gmt_atomic_add
    gmt_atomic_cas

    gmt_atomic_double_add_nb
    gmt_atomic_double_max_nb
    gmt_atomic_double_min_nb
    gmt_atomic_max_nb
    gmt_atomic_min_nb

    gmt_atomic_double_add
    gmt_atomic_double_max
    gmt_atomic_double_min
    gmt_atomic_max
    gmt_atomic_min

    gmt_wait_data
    

 ************************************************************************/

static inline void cmd_put_data(uint32_t tid, uint32_t wid,
                                uint32_t rnid, gmt_data_t gmt_array,
                                uint64_t goffset_bytes, const void *data,
                                uint64_t nbytes)
{
    uint64_t offset = 0;
    while (offset < nbytes) {
        uint32_t granted_nbytes = 0;
        cmd_put_t *cmd = (cmd_put_t *) agm_get_cmd(rnid, wid,
                                                   sizeof(cmd_put_t),
                                                   nbytes - offset,
                                                   &granted_nbytes);
        _assert(granted_nbytes > 0);
        _assert(granted_nbytes <= COMM_BUFFER_SIZE);

        cmd->type = GMT_CMD_PUT;
        cmd->gmt_array = gmt_array;
        cmd->offset = goffset_bytes + offset;
        cmd->tid = tid;
        cmd->put_bytes = granted_nbytes;
        uthread_incr_req_nbytes(tid, sizeof(uint64_t));
        agm_set_cmd_data(rnid, wid,
                         ((uint8_t * const)data) + offset, granted_nbytes);

        offset += granted_nbytes;
    }
}

static inline void cmd_put_value(uint32_t tid, uint32_t wid,
                                 uint32_t rnid, gmt_data_t gmt_array,
                                 uint64_t roffset_bytes, uint64_t value)
{
    cmd_put_value_t *cmd;
    cmd = (cmd_put_value_t *) agm_get_cmd(rnid, wid,
                                          sizeof(cmd_put_value_t), 0, NULL);
    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    cmd->tid = tid;
    cmd->type = GMT_CMD_PUT_VALUE;
    cmd->value = value;
    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

static inline void cmd_put_data_bytes(uint32_t tid, uint32_t wid, uint32_t rnid, gmt_data_t array,
     uint64_t curr_byte, uint64_t last_byte, void * data, uint64_t byte_offset, uint64_t num_bytes) {

  gentry_t * ga = mem_get_gentry(array);
  uint8_t * my_data = (uint8_t *) data;

  while (curr_byte < last_byte) {                                           // curr_byte and last_byte of gmt array
    uint32_t granted = num_bytes;                                                     // request at least num_bytes
    uint64_t remaining = ((last_byte - curr_byte) / ga->nbytes_elem) * num_bytes;     // remaining bytes to be put

    cmd_put_bytes_t * cmd = (cmd_put_bytes_t *)
         agm_get_cmd(rnid, wid, sizeof(cmd_put_bytes_t), remaining, & granted);

    uint64_t num_elems = granted / num_bytes;       // number of elements granted
    uint64_t put_bytes = num_elems * num_bytes;     // number of bytes put

    cmd->type = GMT_CMD_PUT_COLUMN;
    cmd->tid = tid;
    cmd->gmt_array = array;
    cmd->curr_byte = curr_byte;
    cmd->put_bytes = put_bytes;
    cmd->num_elems = num_elems;
    cmd->byte_offset = byte_offset;
    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, my_data, put_bytes);

    my_data += put_bytes;
    curr_byte += num_elems * ga->nbytes_elem;
} }

static inline void cmd_scatter(uint32_t tid, uint32_t wid,
     uint32_t rnid, gmt_data_t array, uint8_t * curr_byte, uint8_t * last_byte) {

  gentry_t * ga = mem_get_gentry(array);

  while (curr_byte < last_byte) {
    uint32_t granted = sizeof(uint64_t);
    uint64_t remaining = last_byte - curr_byte;
    cmd_scatter_t * cmd = (cmd_scatter_t *) agm_get_cmd(rnid, wid, sizeof(cmd_scatter_t), remaining, & granted);

    uint64_t num_elems = granted / (sizeof(uint64_t) + ga->nbytes_elem);       // number of index/elem pairs
    uint64_t put_bytes = num_elems * (sizeof(uint64_t) + ga->nbytes_elem);     // number of buffer bytes used


    cmd->type = GMT_CMD_SCATTER;
    cmd->tid = tid;
    cmd->gmt_array = array;
    cmd->put_bytes = put_bytes;
    cmd->num_elems = num_elems;
    uthread_incr_req_nbytes(tid, sizeof(uint64_t));

    agm_set_cmd_data(rnid, wid, curr_byte, put_bytes);
    curr_byte += put_bytes;
} }

static inline void cmd_gather(uint32_t tid, uint32_t wid, uint32_t rnid,
     gmt_data_t array, uint8_t * data, uint8_t * curr_byte, uint8_t * last_byte) {

  gentry_t * ga = mem_get_gentry(array);

  while (curr_byte < last_byte) {
    uint32_t granted = sizeof(uint64_t);
    uint64_t remaining = last_byte - curr_byte;
    cmd_gather_t * cmd = (cmd_gather_t *) agm_get_cmd(rnid, wid, sizeof(cmd_gather_t), remaining, & granted);

    uint64_t num_elems = granted / sizeof(uint64_t);       // number of index
    uint64_t put_bytes = num_elems * sizeof(uint64_t);     // number of buffer bytes used

    cmd->type = GMT_CMD_GATHER;
    cmd->tid = tid;
    cmd->gmt_array = array;
    cmd->num_elems = num_elems;
    cmd->ret_data_ptr = (uint64_t) data;
    cmd->index_ptr = (uint64_t) curr_byte;
    cmd->last_index = * (curr_byte + put_bytes - sizeof(uint64_t));

    uthread_incr_req_nbytes(tid, num_elems * ga->nbytes_elem);
    agm_set_cmd_data(rnid, wid, curr_byte, put_bytes);

    curr_byte += put_bytes;
    data += num_elems * ga->nbytes_elem;
} }

GMT_INLINE void gmt_put_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                const void *elem, uint64_t num_elem)
{
    _assert(elem != NULL);
//     _DEBUG("GA %d - offset %ld - size %ld\n", 
//            GD_GET_GID(gmt_array), goffset_bytes, nbytes);
    
    gentry_t *const ga = mem_get_gentry(gmt_array);
    _assert(ga!=NULL);
    uint64_t goffset_bytes = elem_offset * ga->nbytes_elem;
    uint64_t nbytes = num_elem * ga->nbytes_elem;
    uint64_t goffset_cur = goffset_bytes;
    uint64_t goffset_end = goffset_bytes + nbytes;
    mem_check_last_byte(ga, goffset_end);
    uint8_t *data_cur = (uint8_t *) elem;
    uint32_t tid = GMT_TO_INITIALIZE;
    uint32_t wid = GMT_TO_INITIALIZE;
    

    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE) {
        _assert(ga != NULL);
        uint32_t i;
        tid = uthread_get_tid();
        wid = uthread_get_wid(tid);
        for (i = 0; i < num_nodes; i++)
            if (node_id != i)
                cmd_put_data(tid, wid, i, gmt_array, goffset_bytes, elem,
                             nbytes);

        /* copy data in local replica */
        _assert(ga->data != NULL);
        mem_put(&ga->data[goffset_bytes], elem, nbytes);
        return;
    }

    /* this while takes into account the fact that an array could
     * be partitioned across multiple nodes */
    while (goffset_cur < goffset_end) {
        uint64_t avail_bytes = 0;
        uint64_t rest_bytes = goffset_end - goffset_cur;

        int64_t loffset;
        if (mem_gmt_data_is_local(ga, gmt_array, goffset_cur, &loffset)) {
            _assert(ga != NULL);
            //_DEBUG("local %ld - %ld\n", goffset_cur, goffset_end);
            avail_bytes = MIN(rest_bytes, ga->nbytes_loc - loffset);
            _assert(avail_bytes > 0 && ga->data != NULL);
            mem_put(&ga->data[loffset], data_cur, avail_bytes);
            COUNT_EVENT(WORKER_GMT_PUT_LOCAL);
        } else {
            //_DEBUG("remote %ld - %ld\n", goffset_cur, goffset_end);
            if (tid == GMT_TO_INITIALIZE) {
                tid = uthread_get_tid();
                wid = uthread_get_wid(tid);
            }
            uint32_t rnid = 0;
            uint64_t roffset = 0;
            mem_locate_gmt_data_remote(ga, goffset_cur, &rnid,
                                       &roffset);
            if (ga != NULL)
                avail_bytes = MIN(rest_bytes, ga->nbytes_block - roffset);
            else
                avail_bytes = rest_bytes;
            _assert(avail_bytes > 0);
            /* in case the available remote size is 8 bytes or less just use
             * a GMT_PUT_VALUE (it is faster)
             * Can use the GMT_PUT_VALUE only when remaning bytes are the size of an element
             * if size of element is less than 8 bytes, and exactly 8 bytes remain, the put value will only write
             * the bytes of the element
             */
            if ((avail_bytes == 8 ||
                avail_bytes == 4 || avail_bytes == 2 || avail_bytes == 1) && (avail_bytes==ga->nbytes_elem))
                cmd_put_value(tid, wid, rnid, gmt_array, roffset,
                              *(uint64_t *) (data_cur));

            else
                cmd_put_data(tid, wid, rnid, gmt_array, roffset,
                             data_cur, avail_bytes);

            COUNT_EVENT(WORKER_GMT_PUT_REMOTE);
        }
        goffset_cur += avail_bytes;
        data_cur += avail_bytes;
    }
}

GMT_INLINE void gmt_put_value_nb(gmt_data_t gmt_array, uint64_t elem_offset,
    uint64_t value)
{
    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    _assert(ga != NULL);

    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE) {
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        uint32_t i;
        for (i = 0; i < num_nodes; i++)
            if (node_id != i)
                cmd_put_value(tid, wid, i, gmt_array, goffset_bytes, value);
        /* copy data in local replica */
        mem_put_value(&ga->data[goffset_bytes], value, size);
        return;
    }

    int64_t loffset;
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        uint8_t *ptr = mem_get_loc_ptr(ga, loffset, size);
        mem_put_value(ptr, value, size);
        COUNT_EVENT(WORKER_GMT_PUTVALUE_LOCAL);
    } else {
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid,
                                   &roffset_bytes);
        cmd_put_value(tid, wid, rnid, gmt_array, roffset_bytes, value);
        COUNT_EVENT(WORKER_GMT_PUTVALUE_REMOTE);
    }
}

GMT_INLINE void gmt_put_bytes_nb(gmt_data_t array, uint64_t index,
     void * data, uint64_t num_elems, uint64_t byte_offset, uint64_t num_bytes) {

  gentry_t * ga = mem_get_gentry(array);

  _assert(ga != NULL);
  _assert(ga->data != NULL);
  _assert(index + num_elems <= gmt_nelems_tot(array));       // number of elements is not exceeded
  _assert(byte_offset <= ga->nbytes_elem);                   // number of bytes per element is not exceeded

  uint64_t curr_byte = index * ga->nbytes_elem;
  uint64_t last_byte = curr_byte + num_elems * ga->nbytes_elem;
  uint8_t * data_ptr = (uint8_t *) data;

  uint32_t tid = GMT_TO_INITIALIZE;
  uint32_t wid = GMT_TO_INITIALIZE;

  if (GD_GET_TYPE_DISTR(array) == GMT_ALLOC_REPLICATE) {
      tid = uthread_get_tid();
      wid = uthread_get_wid(tid);
      for (uint64_t i = 0; i < num_nodes; i++)
        if (node_id != i) cmd_put_data_bytes(tid, wid, i, array, curr_byte, last_byte, data, byte_offset, num_bytes);

       uint8_t * curr_byte_local = ga->data + curr_byte;
       uint8_t * last_byte_local = curr_byte_local + num_elems * ga->nbytes_elem;

       while (curr_byte_local < last_byte_local) {
         memcpy(curr_byte_local + byte_offset, data_ptr, num_bytes);
         curr_byte_local += ga->nbytes_elem;
         curr_byte += ga->nbytes_elem;
         data_ptr += num_bytes;
       }
        return;
  }

  /* this while takes into account the fact that an array could be partitioned across multiple nodes */
  while (curr_byte < last_byte) {
    int64_t loffset = 0;
    uint64_t remaining_bytes = last_byte - curr_byte;

    if (mem_gmt_data_is_local(ga, array, curr_byte, & loffset)) {
       COUNT_EVENT(WORKER_GMT_PUT_LOCAL);
       uint64_t avail_bytes = MIN(remaining_bytes, ga->nbytes_loc - loffset);

       uint8_t * curr_byte_local = ga->data + loffset;
       uint8_t * last_byte_local = ga->data + loffset + avail_bytes;

       while (curr_byte_local < last_byte_local) {
         memcpy(curr_byte_local + byte_offset, data_ptr, num_bytes);
         curr_byte_local += ga->nbytes_elem;
         curr_byte += ga->nbytes_elem;
         data_ptr += num_bytes;
       }

    } else {
       uint32_t rnid = 0;
       uint64_t roffset = 0;
       COUNT_EVENT(WORKER_GMT_PUT_REMOTE);

       mem_locate_gmt_data_remote(ga, curr_byte, & rnid, & roffset);
       uint64_t avail_bytes = MIN(remaining_bytes, ga->nbytes_block - roffset);
       if (tid == GMT_TO_INITIALIZE) {tid = uthread_get_tid(); wid = uthread_get_wid(tid);}

       cmd_put_data_bytes(tid, wid, rnid, array, roffset, roffset + avail_bytes, data_ptr, byte_offset, num_bytes);
       data_ptr  += (avail_bytes / ga->nbytes_elem) * num_bytes;
       curr_byte += avail_bytes;
} } }

GMT_INLINE void *gmt_get_local_ptr(gmt_data_t gmt_array, uint64_t elem_offset)
{
  _assert(gmt_array != GMT_DATA_NULL);
  gentry_t *const ga = mem_get_gentry(gmt_array);
  mem_check_last_byte(ga, elem_offset * ga->nbytes_elem);

  int64_t loffset;
  if (mem_gmt_data_is_local(ga, gmt_array, elem_offset*ga->nbytes_elem,
        &loffset)){
    return (void *)(ga->data + loffset);
  } else {
    return NULL;
  }
}

GMT_INLINE void gmt_get_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                void *data, uint64_t num_elem)
{
    _assert(data != NULL);

    gentry_t *const ga = mem_get_gentry(gmt_array);
    uint64_t goffset_bytes = elem_offset * ga->nbytes_elem;
    uint64_t nbytes = num_elem * ga->nbytes_elem;
    mem_check_last_byte(ga, goffset_bytes);
    uint64_t goffset_cur = goffset_bytes;
    uint64_t goffset_end = goffset_bytes + nbytes;
    uint8_t *data_cur = (uint8_t *) data;
    uint32_t wid = GMT_TO_INITIALIZE, tid = GMT_TO_INITIALIZE;
    while (goffset_cur < goffset_end) {
        uint64_t avail_bytes = 0;
        uint64_t rest_bytes = goffset_end - goffset_cur;

        int64_t loffset;
        if (mem_gmt_data_is_local(ga, gmt_array, goffset_cur, &loffset)) {
            avail_bytes = MIN(rest_bytes, ga->nbytes_loc - loffset);
            uint8_t *ptr = mem_get_loc_ptr(ga, loffset, avail_bytes);
            //_DEBUG("ptr %p - avail_bytes %ld\n", ptr, avail_bytes);
            memcpy(data_cur, ptr, avail_bytes);
            COUNT_EVENT(WORKER_GMT_GET_LOCAL);
        } else {
            if (wid == GMT_TO_INITIALIZE) {
                tid = uthread_get_tid();
                wid = uthread_get_wid(tid);
            }

            uint32_t rnid = 0;
            uint64_t roffset_bytes = 0;
            mem_locate_gmt_data_remote(ga, goffset_cur, &rnid,
                                       &roffset_bytes);
            if (ga != NULL)
                avail_bytes = MIN(rest_bytes, ga->nbytes_block - roffset_bytes);
            else
                avail_bytes = rest_bytes;
            _assert(avail_bytes > 0);
            cmd_get_t *cmd = (cmd_get_t *) agm_get_cmd(rnid, wid,
                                                       sizeof(cmd_get_t),
                                                       0, NULL);
            cmd->gmt_array = gmt_array;
            cmd->get_bytes = avail_bytes;
            cmd->offset = roffset_bytes;
            _assert((uint64_t) data_cur >> VIRT_ADDR_PTR_BITS == 0);
            cmd->ret_data_ptr = (uint64_t) data_cur;            
            cmd->tid = tid;
            cmd->type = GMT_CMD_GET;
            uthread_incr_req_nbytes(tid, avail_bytes);
            agm_set_cmd_data(rnid, wid, NULL, 0);
            COUNT_EVENT(WORKER_GMT_GET_REMOTE);
        }
        goffset_cur += avail_bytes;
        data_cur += avail_bytes;
    }
}

GMT_INLINE void gmt_get_bytes_nb(gmt_data_t array, uint64_t index,
     void * data, uint64_t num_elems, uint64_t byte_offset, uint64_t num_bytes) {

  gentry_t * ga = mem_get_gentry(array);

  _assert(ga != NULL);
  _assert(ga->data != NULL);
  _assert(index + num_elems <= gmt_nelems_tot(array));       // number of elements is not exceeded
  _assert(byte_offset + num_bytes <= ga->nbytes_elem);       // number of bytes per element is not exceeded

  uint8_t * data_ptr = (uint8_t *) data;
  uint64_t curr_byte = index * ga->nbytes_elem;
  uint64_t last_byte = curr_byte + num_elems * ga->nbytes_elem;

  uint32_t tid = GMT_TO_INITIALIZE;
  uint32_t wid = GMT_TO_INITIALIZE;

  while (curr_byte < last_byte) {
    int64_t loffset = 0;
    uint64_t avail_bytes, remaining_bytes = last_byte - curr_byte;

    if (mem_gmt_data_is_local(ga, array, curr_byte, & loffset)) {
       COUNT_EVENT(WORKER_GMT_GET_LOCAL);
       avail_bytes = MIN(remaining_bytes, ga->nbytes_loc - loffset);

       uint8_t * curr_byte_local = ga->data + loffset;
       uint8_t * last_byte_local = ga->data + loffset + avail_bytes;

       while (curr_byte_local < last_byte_local) {
         memcpy(data_ptr, curr_byte_local + byte_offset, num_bytes);
         curr_byte_local += ga->nbytes_elem;
         curr_byte += ga->nbytes_elem;
         data_ptr += num_bytes;
       }

    } else {

      uint32_t rnid = 0;
      uint64_t roffset = 0;
      COUNT_EVENT(WORKER_GMT_GET_REMOTE);

      mem_locate_gmt_data_remote(ga, curr_byte, & rnid, & roffset);
      avail_bytes = MIN(remaining_bytes, ga->nbytes_block - roffset);
      if (tid == GMT_TO_INITIALIZE) {tid = uthread_get_tid(); wid = uthread_get_wid(tid);}
      cmd_get_bytes_t * cmd = (cmd_get_bytes_t *) agm_get_cmd(rnid, wid, sizeof(cmd_get_bytes_t), 0, NULL);

      cmd->type = GMT_CMD_GET_COLUMN;
      cmd->tid = tid;
      cmd->gmt_array = array;
      cmd->get_bytes = avail_bytes;
      cmd->offset = roffset;
      cmd->ret_data_ptr = (uint64_t) data_ptr;
      cmd->byte_offset = byte_offset;
      uthread_incr_req_nbytes(tid, avail_bytes);
      agm_set_cmd_data(rnid, wid, NULL, 0);

      data_ptr += (avail_bytes / ga->nbytes_elem) * num_bytes;
      curr_byte += avail_bytes;
} } }


/************************************************************************/
/*                                                                      */
/*                        SCATTER/GATHER METHODS                        */
/*                                                                      */
/************************************************************************/

void gmt_gather_nb(gmt_data_t array, uint64_t * index, void * data, uint64_t num_elems) {
  gentry_t * ga = mem_get_gentry(array);

  _assert(ga != NULL);
  _assert(ga->data != NULL);

  uint64_t i = 0;
  int64_t  loffset = 0;
  uint64_t roffset = 0;
  uint32_t rnid_0, rnid_1;
  uint32_t tid = GMT_TO_INITIALIZE;
  uint32_t wid = GMT_TO_INITIALIZE;
  uint8_t * my_data = (uint8_t *) data;

  while (i < num_elems) {
    uint64_t ga_byte = index[i] * ga->nbytes_elem;

    if (mem_gmt_data_is_local(ga, array, ga_byte, & loffset)) {     // element is local
       memcpy(my_data, ga->data + ga_byte, ga->nbytes_elem);

       i ++;
       my_data += ga->nbytes_elem;
       COUNT_EVENT(WORKER_GMT_GET_LOCAL);

    } else {                                                        // element is remote
       uint64_t first_i = i;
       mem_locate_gmt_data_remote(ga, ga_byte, & rnid_0, & roffset);

       while (true) {                                               // identify all eleemnts on rnid 
         i ++;
         if (i == num_elems) break;       // all done

         ga_byte = index[i] * ga->nbytes_elem;
         mem_locate_gmt_data_remote(ga, ga_byte, & rnid_1, & roffset);

         if (rnid_1 != rnid_0) break;     // found all puts on rnid_0
       }

       if (tid == GMT_TO_INITIALIZE) {tid = uthread_get_tid(); wid = uthread_get_wid(tid);}
       cmd_gather(tid, wid, rnid_0, array, my_data, (uint8_t *) (index + first_i), (uint8_t *) (index + i));

       my_data += (i - first_i) * ga->nbytes_elem;
       COUNT_EVENT(WORKER_GMT_GET_REMOTE);
} } }

void gmt_scatter_nb(gmt_data_t array, uint64_t * stream, uint64_t num_elems) {
  gentry_t * ga = mem_get_gentry(array);

  _assert(ga != NULL);
  _assert(ga->data != NULL);
  uint32_t tid = GMT_TO_INITIALIZE;
  uint32_t wid = GMT_TO_INITIALIZE;
  uint64_t num_cols = ga->nbytes_elem / sizeof(uint64_t);;

  if (GD_GET_TYPE_DISTR(array) == GMT_ALLOC_REPLICATE) {               // REPLICATE array
  tid = uthread_get_tid();
  wid = uthread_get_wid(tid);
  for (uint64_t i = 0; i < num_nodes; i++) { }     // TODO

  for (uint64_t i = 0; i < num_elems; i ++) {
    uint64_t ga_byte = stream[0] * ga->nbytes_elem;
    memcpy(ga->data + ga_byte, stream + 1, ga->nbytes_elem);
    stream += num_cols + 1;
  }

  } else {                                                             // LOCAL or PARTITION array

     int64_t  loffset = 0;
     uint32_t rnid_0, rnid_1;
     uint64_t i = 0, roffset = 0;;

     while (i < num_elems) {
       uint64_t ga_byte = stream[0] * ga->nbytes_elem;

       if (mem_gmt_data_is_local(ga, array, ga_byte, & loffset)) {     // element is local
          COUNT_EVENT(WORKER_GMT_PUT_LOCAL);
          memcpy(ga->data + ga_byte, stream + 1, ga->nbytes_elem);

          i ++;
          stream += num_cols + 1;

       } else {                                                        // element is on remote node rnid
         COUNT_EVENT(WORKER_GMT_PUT_REMOTE);
         uint8_t * first_data_byte = (uint8_t *) stream;
         uint8_t * last_data_byte  = (uint8_t *) stream;
         mem_locate_gmt_data_remote(ga, ga_byte, & rnid_0, & roffset);

         while (true) {                                                // identify all eleemnts on rnid 
           i ++;
           stream += num_cols + 1;
           last_data_byte = (uint8_t *) stream;

           if (i == num_elems) break;       // all done

           ga_byte = stream[0] * ga->nbytes_elem;
           mem_locate_gmt_data_remote(ga, ga_byte, & rnid_1, & roffset);

           if (rnid_1 != rnid_0) break;     // found all puts on rnid_0
         }

         if (tid == GMT_TO_INITIALIZE) {tid = uthread_get_tid(); wid = uthread_get_wid(tid);}
         cmd_scatter(tid, wid, rnid_0, array, first_data_byte, last_data_byte);
} }  } }

INLINE void _atomic_add_remote(uint32_t tid, uint32_t wid,
                               uint32_t rnid, gmt_data_t gmt_array,
                               uint64_t roffset_bytes, uint64_t value,
                               int64_t * ret_value_ptr)
{
    cmd_atomic_add_t *cmd;
    cmd = (cmd_atomic_add_t *) agm_get_cmd(rnid, wid,
                                           sizeof(cmd_atomic_add_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    _assert( (uint64_t) ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_ADD;
    cmd->value = value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

INLINE void _atomic_double_add_remote(uint32_t tid, uint32_t wid,
                                     uint32_t rnid, gmt_data_t gmt_array,
                                     uint64_t roffset_bytes, double value,
                                     double * ret_value_ptr)
{
    cmd_atomic_double_t *cmd;
    cmd = (cmd_atomic_double_t *) agm_get_cmd(rnid, wid, sizeof(cmd_atomic_double_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    _assert( (uint64_t) ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_DOUBLE_ADD;
    cmd->value = value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

INLINE void _atomic_double_max_remote(uint32_t tid, uint32_t wid,
                                     uint32_t rnid, gmt_data_t gmt_array,
                                     uint64_t roffset_bytes, uint64_t field_offset,
                                     double value, double * ret_value_ptr)
{
    cmd_atomic_double_t *cmd;
    cmd = (cmd_atomic_double_t *) agm_get_cmd(rnid, wid, sizeof(cmd_atomic_double_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    cmd->field_offset = field_offset;
    _assert( (uint64_t) ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_DOUBLE_MAX;
    cmd->value = value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

INLINE void _atomic_double_min_remote(uint32_t tid, uint32_t wid,
                                     uint32_t rnid, gmt_data_t gmt_array,
                                     uint64_t roffset_bytes, double value,
                                     double * ret_value_ptr)
{
    cmd_atomic_double_t *cmd;
    cmd = (cmd_atomic_double_t *) agm_get_cmd(rnid, wid, sizeof(cmd_atomic_double_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    _assert( (uint64_t) ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_DOUBLE_MIN;
    cmd->value = value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

INLINE void _atomic_max_remote(uint32_t tid, uint32_t wid,
                               uint32_t rnid, gmt_data_t gmt_array,
                               uint64_t roffset_bytes, int64_t value,
                               int64_t * ret_value_ptr)
{
    cmd_atomic_int_t *cmd;
    cmd = (cmd_atomic_int_t *) agm_get_cmd(rnid, wid, sizeof(cmd_atomic_int_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    _assert( (uint64_t) ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_MAX;
    cmd->value = value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

INLINE void _atomic_min_remote(uint32_t tid, uint32_t wid,
                               uint32_t rnid, gmt_data_t gmt_array,
                               uint64_t roffset_bytes, int64_t value,
                               int64_t * ret_value_ptr)
{
    cmd_atomic_int_t *cmd;
    cmd = (cmd_atomic_int_t *) agm_get_cmd(rnid, wid, sizeof(cmd_atomic_int_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    _assert( (uint64_t) ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_MIN;
    cmd->value = value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

GMT_INLINE void gmt_atomic_add_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       int64_t value, int64_t * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE OPERATION NOT VALID");

    int64_t loffset;
    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        COUNT_EVENT(WORKER_GMT_ATOMIC_ADD_LOCAL);
        uint8_t *ptr = mem_get_loc_ptr(ga, loffset, size);
        int64_t ret = mem_atomic_add(ptr, value, size);
        if (ret_value_ptr != NULL)
            *ret_value_ptr = ret;
    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, &roffset_bytes);
        _atomic_add_remote(tid, wid, rnid, gmt_array, roffset_bytes, value,
                           ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_ADD_REMOTE);
    }
}


GMT_INLINE void gmt_atomic_double_add_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       double value, double * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE OPERATION NOT VALID");

    int64_t loffset;
    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        COUNT_EVENT(WORKER_GMT_ATOMIC_ADD_LOCAL);
        int64_t * ptr = (int64_t *) mem_get_loc_ptr(ga, loffset, size);
        double old_value;

        while (true) {
           old_value = * (double *) ptr;
           double new_value = old_value + value;
           int64_t * old_value_ptr = (int64_t *) & old_value;
           int64_t * new_value_ptr = (int64_t *) & new_value;
           if (__sync_bool_compare_and_swap(ptr, * old_value_ptr, * new_value_ptr)) break;
        }

        if (ret_value_ptr != NULL) * ret_value_ptr = old_value;

    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, &roffset_bytes);
        _atomic_double_add_remote(tid, wid, rnid, gmt_array, roffset_bytes, value, ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_ADD_REMOTE);
    }
}

GMT_INLINE void gmt_atomic_double_max_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       double value, double * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE OPERATION NOT VALID");

    uint64_t field_offset = elem_offset >> 56;
    elem_offset = elem_offset & 0x0fffffff;

    int64_t loffset;
    gentry_t *const ga = mem_get_gentry(gmt_array);
    // mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        COUNT_EVENT(WORKER_GMT_ATOMIC_MAX_LOCAL);
        int64_t * ptr = ((int64_t *) mem_get_loc_ptr(ga, loffset, size)) + field_offset;
        double old_value;

        while (true) {
           old_value = * (double *) ptr;
           double new_value = MAX(old_value, value);
           int64_t * old_value_ptr = (int64_t *) & old_value;
           int64_t * new_value_ptr = (int64_t *) & new_value;
           if (__sync_bool_compare_and_swap(ptr, * old_value_ptr, * new_value_ptr)) break;
        }

        if (ret_value_ptr != NULL) * ret_value_ptr = old_value;

    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, &roffset_bytes);
        _atomic_double_max_remote(tid, wid, rnid, gmt_array, roffset_bytes, field_offset, value, ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_MAX_REMOTE);
    }
}

GMT_INLINE void gmt_atomic_double_min_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       double value, double * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE OPERATION NOT VALID");

    int64_t loffset;
    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        COUNT_EVENT(WORKER_GMT_ATOMIC_MIN_LOCAL);
        int64_t * ptr = (int64_t *) mem_get_loc_ptr(ga, loffset, size);
        double old_value;

        while (true) {
           old_value = * (double *) ptr;
           double new_value = MIN(old_value, value);
           int64_t * old_value_ptr = (int64_t *) & old_value;
           int64_t * new_value_ptr = (int64_t *) & new_value;
           if (__sync_bool_compare_and_swap(ptr, * old_value_ptr, * new_value_ptr)) break;
        }

        if (ret_value_ptr != NULL) * ret_value_ptr = old_value;

    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, &roffset_bytes);
        _atomic_double_min_remote(tid, wid, rnid, gmt_array, roffset_bytes, value, ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_MIN_REMOTE);
    }
}

GMT_INLINE void gmt_atomic_max_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       int64_t value, int64_t * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE OPERATION NOT VALID");

    int64_t loffset;
    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        COUNT_EVENT(WORKER_GMT_ATOMIC_MAX_LOCAL);
        int64_t * ptr = (int64_t *) mem_get_loc_ptr(ga, loffset, size);
        int64_t old_value;

        while (true) {
           old_value = * (int64_t *) ptr;
           int64_t new_value = MAX(old_value, value);
           int64_t * old_value_ptr = (int64_t *) & old_value;
           int64_t * new_value_ptr = (int64_t *) & new_value;
           if (__sync_bool_compare_and_swap(ptr, * old_value_ptr, * new_value_ptr)) break;
        }

        if (ret_value_ptr != NULL) * ret_value_ptr = old_value;

    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, &roffset_bytes);
        _atomic_max_remote(tid, wid, rnid, gmt_array, roffset_bytes, value, ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_MAX_REMOTE);
    }
}

GMT_INLINE void gmt_atomic_min_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       int64_t value, int64_t * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE OPERATION NOT VALID");

    int64_t loffset;
    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem;
    uint64_t goffset_bytes = elem_offset * size;
    mem_check_last_byte(ga, goffset_bytes + size);
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        COUNT_EVENT(WORKER_GMT_ATOMIC_MIN_LOCAL);
        int64_t * ptr = (int64_t *) mem_get_loc_ptr(ga, loffset, size);
        int64_t old_value;

        while (true) {
           old_value = * (int64_t *) ptr;
           int64_t new_value = MIN(old_value, value);
           int64_t * old_value_ptr = (int64_t *) & old_value;
           int64_t * new_value_ptr = (int64_t *) & new_value;
           if (__sync_bool_compare_and_swap(ptr, * old_value_ptr, * new_value_ptr)) break;
        }

        if (ret_value_ptr != NULL) * ret_value_ptr = old_value;

    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        uint32_t tid = uthread_get_tid();
        uint32_t wid = uthread_get_wid(tid);
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid, &roffset_bytes);
        _atomic_min_remote(tid, wid, rnid, gmt_array, roffset_bytes, value, ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_MIN_REMOTE);
    }
}

INLINE void _atomic_cas_remote(uint32_t tid, uint32_t wid,
                               uint32_t rnid, gmt_data_t gmt_array,
                               uint64_t roffset_bytes,
                               uint64_t old_value, uint64_t new_value,
                               int64_t * ret_value_ptr)
{
    cmd_atomic_cas_t *cmd;
    cmd = (cmd_atomic_cas_t *) agm_get_cmd(rnid, wid,
                                           sizeof(cmd_atomic_cas_t), 0, NULL);

    cmd->gmt_array = gmt_array;
    cmd->offset = roffset_bytes;
    _assert((uint64_t)ret_value_ptr >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_value_ptr = (uint64_t) ret_value_ptr;     
    cmd->tid = tid;
    cmd->type = GMT_CMD_ATOMIC_CAS;
    cmd->old_value = old_value;
    cmd->new_value = new_value;

    uthread_incr_req_nbytes(tid, sizeof(uint64_t));
    agm_set_cmd_data(rnid, wid, NULL, 0);
}

GMT_INLINE void gmt_atomic_cas_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                       int64_t old_value, int64_t new_value,
                       int64_t * ret_value_ptr)
{
    if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_REPLICATE)
        ERRORMSG("DATA ALLOCATED WITH GMT_ALLOC_REPLICATE"
            " OPERATION NOT VALID");

    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);

    gentry_t *const ga = mem_get_gentry(gmt_array);
    mem_check_word_elem_size(ga);
    uint64_t size = ga->nbytes_elem; 
    uint64_t goffset_bytes = size * elem_offset;
    mem_check_last_byte(ga, goffset_bytes + size);

    int64_t loffset;
    if (mem_gmt_data_is_local(ga, gmt_array, goffset_bytes, &loffset)) {
        uint8_t *ptr = mem_get_loc_ptr(ga, loffset, size);
        int64_t ret = mem_atomic_cas(ptr, old_value, new_value, size);
        if (ret_value_ptr != NULL)
            *ret_value_ptr = ret;

        COUNT_EVENT(WORKER_GMT_ATOMIC_CAS_LOCAL);
        /* We do a context switch here to avoid deadlocks
           in case atomic is used as synchronization primitive between tasks */
        uthreads[tid].tstatus = TASK_WAITING_DATA;
        worker_schedule(tid, wid);
        uthreads[tid].tstatus = TASK_RUNNING;
    } else {
        uint32_t rnid = 0;
        uint64_t roffset_bytes = 0;
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid,
                                   &roffset_bytes);
        _atomic_cas_remote(tid, wid, rnid, gmt_array, roffset_bytes, old_value,
                           new_value, ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_CAS_REMOTE);
    }
}

GMT_INLINE void gmt_wait_data()
{
    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);
    worker_wait_data(tid, wid);
}

GMT_INLINE void gmt_get(gmt_data_t gmt_array, uint64_t goffset_bytes,
             void *data, uint64_t num_bytes)
{
    gmt_get_nb(gmt_array, goffset_bytes, data, num_bytes);
    gmt_wait_data();
}

GMT_INLINE void gmt_get_bytes(gmt_data_t array, uint64_t index,
     void * data, uint64_t num_elems, uint64_t byte_offset, uint64_t num_bytes) {

  gmt_get_bytes_nb(array, index, data, num_elems, byte_offset, num_bytes);
  gmt_wait_data();
}

GMT_INLINE void gmt_put_value(gmt_data_t gmt_array, uint64_t goffset_bytes,
                   uint64_t value)
{
    gmt_put_value_nb(gmt_array, goffset_bytes, value);
    gmt_wait_data();
}

GMT_INLINE void gmt_put(gmt_data_t gmt_array, uint64_t goffset_bytes,
             const void *data, uint64_t num_bytes)
{
    gmt_put_nb(gmt_array, goffset_bytes, data, num_bytes);
    gmt_wait_data();
}

GMT_INLINE void gmt_put_bytes(gmt_data_t array, uint64_t index,
     void * data,  uint64_t num_elems, uint64_t byte_offset, uint64_t num_bytes) {

  gmt_put_bytes_nb(array, index, data, num_elems, byte_offset, num_bytes);
  gmt_wait_data();
}

void gmt_gather(gmt_data_t array, uint64_t * index, void * data, uint64_t num_elems) {
  gmt_gather_nb(array, index, data, num_elems);
  gmt_wait_data();
}
//
// merge index and data into single stream of {index, data} pairs, and then call gmt_scatter_nb
void gmt_scatter(gmt_data_t array, uint64_t * index, void * data, uint64_t num_elems) {
  gentry_t * ga = mem_get_gentry(array);

  _assert(ga != NULL);
  _assert(ga->data != NULL);
  uint64_t num_cols = ga->nbytes_elem / sizeof(uint64_t);;
  uint64_t * stream = (uint64_t *) malloc(num_elems * (ga->nbytes_elem + sizeof(uint64_t)));

  for (uint64_t i = 0; i < num_elems; i ++) {
    uint64_t d_ndx = i * num_cols;
    uint64_t s_ndx = i * num_cols + i;
    stream[s_ndx] = index[i];
    memcpy(stream + s_ndx + 1, (uint64_t *) data + d_ndx, ga->nbytes_elem);
  }

  gmt_scatter_nb(array, stream, num_elems);
  gmt_wait_data();
  free(stream);
}

GMT_INLINE int64_t gmt_atomic_cas(gmt_data_t gmt_array, uint64_t elem_offset,
                       int64_t old_value, int64_t new_value)
{
    int64_t ret_value;
    gmt_atomic_cas_nb(gmt_array, elem_offset,
                      old_value, new_value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE int64_t gmt_atomic_add(gmt_data_t gmt_array, uint64_t elem_offset,
                       int64_t value)
{
    int64_t ret_value;
    gmt_atomic_add_nb(gmt_array, elem_offset, value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE double gmt_atomic_double_add(gmt_data_t gmt_array, uint64_t elem_offset, double value)
{
    double ret_value;
    gmt_atomic_double_add_nb(gmt_array, elem_offset, value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE double gmt_atomic_double_max(gmt_data_t gmt_array, uint64_t elem_offset, double value)
{
    double ret_value;
    gmt_atomic_double_max_nb(gmt_array, elem_offset, value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE double gmt_atomic_double_min(gmt_data_t gmt_array, uint64_t elem_offset, double value)
{
    double ret_value;
    gmt_atomic_double_min_nb(gmt_array, elem_offset, value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE int64_t gmt_atomic_max(gmt_data_t gmt_array, uint64_t elem_offset, int64_t value)
{
    int64_t ret_value;
    gmt_atomic_max_nb(gmt_array, elem_offset, value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE int64_t gmt_atomic_min(gmt_data_t gmt_array, uint64_t elem_offset, int64_t value)
{
    int64_t ret_value;
    gmt_atomic_min_nb(gmt_array, elem_offset, value, &ret_value);
    gmt_wait_data();
    return ret_value;
}

GMT_INLINE uint64_t gmt_count_local_elements(gmt_data_t gmt_array) {
  gentry_t *const ga = mem_get_gentry(gmt_array);
  uint64_t nbytes_loc, nbytes_block, goffset_bytes;
  block_partition(node_id, ga->nbytes_tot/ga->nbytes_elem,
        ga->nbytes_elem, gmt_array,
        &nbytes_loc, &nbytes_block, &goffset_bytes);
  return nbytes_loc/ga->nbytes_elem;
}
