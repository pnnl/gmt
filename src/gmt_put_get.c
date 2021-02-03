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
    gmt_put_value_nb
    gmt_get_nb
    
    gmt_put     
    gmt_put_value 
    gmt_get 
    
    gmt_get_local_ptr
    
    gmt_atomic_add_nb
    gmt_atomic_cas_nb
    gmt_atomic_add
    gmt_atomic_cas
    
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

static inline void cmd_mem_put_data(uint32_t tid, uint32_t wid,
                                    uint32_t rnid, uint8_t* address,
                                    const uint8_t *data,
                                    uint64_t nbytes)
{
    uint64_t offset = 0;
    while (offset < nbytes) {
        uint32_t granted_nbytes = 0;
        cmd_mem_put_t *cmd = (cmd_mem_put_t *) agm_get_cmd(rnid, wid,
                                                   sizeof(cmd_mem_put_t),
                                                   nbytes - offset,
                                                   &granted_nbytes);
        _assert(granted_nbytes > 0);
        _assert(granted_nbytes <= COMM_BUFFER_SIZE);

        cmd->type = GMT_CMD_MEM_PUT;
        cmd->address = address + offset;
        cmd->tid = tid;
        cmd->put_bytes = granted_nbytes;
        uthread_incr_req_nbytes(tid, sizeof(uint64_t));
        agm_set_cmd_data(rnid, wid,
                         ((uint8_t * const)data) + offset, granted_nbytes);

        offset += granted_nbytes;
    }
}

static inline void cmd_mem_strided_put_data(uint32_t tid, uint32_t wid,
                                            uint32_t rnid, uint8_t* address,
                                            const uint8_t *data,
                                            uint64_t chunk_offset,
                                            uint64_t chunk_size,
                                            uint64_t nbytes)
{
    uint64_t proc_bytes = 0;
    uint64_t first_chunck_size = 0;
    uint64_t last_chunck_size = 0;
    while (proc_bytes < nbytes) {
        uint32_t granted_nbytes = 0;
        cmd_mem_strided_put_t *cmd = 
           (cmd_mem_strided_put_t *) agm_get_cmd(rnid, wid,
                                                 sizeof(cmd_mem_strided_put_t),
                                                 nbytes - proc_bytes,
                                                 &granted_nbytes);
        _assert(granted_nbytes > 0);
        _assert(granted_nbytes <= COMM_BUFFER_SIZE);
        last_chunck_size = (granted_nbytes - first_chunck_size) % chunk_size;
        cmd->type = GMT_CMD_MEM_STRIDED_PUT;
        cmd->address = address;
        cmd->chunk_offset = chunk_offset;
        cmd->chunk_size = chunk_size;
        cmd->first_chunk_size = first_chunck_size;
        cmd->last_chunk_size = last_chunck_size;
        cmd->tid = tid;
        cmd->put_bytes = granted_nbytes;
        uthread_incr_req_nbytes(tid, sizeof(uint64_t));
        agm_set_cmd_data(rnid, wid, ((uint8_t * const)data) + proc_bytes,
                         granted_nbytes);
        uint64_t nelem = (granted_nbytes - first_chunck_size) / chunk_size;
        uint64_t offset = nelem*chunk_offset + last_chunck_size;
        address += offset;
        proc_bytes += granted_nbytes;
        if (last_chunck_size > 0) {
            first_chunck_size = chunk_size - last_chunck_size;
        }
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

GMT_INLINE void gmt_mem_put_nb(uint32_t rnid, uint8_t* raddress,
                               const uint8_t* data, uint64_t num_bytes)
{
    if (rnid == node_id) {
        mem_put(raddress, data, num_bytes);
        return;
    }
    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);
    cmd_mem_put_data(tid, wid, rnid, raddress, data, num_bytes);
    COUNT_EVENT(WORKER_GMT_MEM_PUT_REMOTE);
}

GMT_INLINE void gmt_mem_strided_put_nb(uint32_t rnid, uint8_t* raddress,
                                       const uint8_t* data,
                                       uint64_t chunk_offset,
                                       uint64_t chunk_size,
                                       uint64_t num_chunks)
{
    if (rnid == node_id)
    {
        for (uint64_t i = 0; i < num_chunks; ++i)
        {
            mem_put(raddress, data, chunk_size);
            raddress += chunk_offset;
            data += chunk_size;
        }
        return;
    }
    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);
    cmd_mem_strided_put_data(tid, wid, rnid, raddress, data,
                             chunk_offset, chunk_size, chunk_size*num_chunks);
    COUNT_EVENT(WORKER_GMT_MEM_STRIDED_PUT_REMOTE);
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

GMT_INLINE void gmt_mem_get_nb(uint32_t rnid, uint8_t* data,
                               const uint8_t* raddress, uint64_t nbytes)
{
    if (rnid == node_id)
    {
        memcpy(data, raddress, nbytes);
        return;
    }
    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);
    cmd_mem_get_t *cmd = (cmd_mem_get_t *) agm_get_cmd(rnid, wid,
                                                       sizeof(cmd_get_t),
                                                       0, NULL);
    cmd->address = raddress;
    cmd->get_bytes = nbytes;
    _assert((uint64_t) data >> VIRT_ADDR_PTR_BITS == 0);
    cmd->ret_data_ptr = (uint64_t) data;            
    cmd->tid = tid;
    cmd->type = GMT_CMD_MEM_GET;
    uthread_incr_req_nbytes(tid, nbytes);
    agm_set_cmd_data(rnid, wid, NULL, 0);
    COUNT_EVENT(WORKER_GMT_MEM_GET_REMOTE);
}

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
        mem_locate_gmt_data_remote(ga, goffset_bytes, &rnid,
                                   &roffset_bytes);
        _atomic_add_remote(tid, wid, rnid, gmt_array, roffset_bytes, value,
                           ret_value_ptr);
        COUNT_EVENT(WORKER_GMT_ATOMIC_ADD_REMOTE);
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

GMT_INLINE void gmt_mem_get(uint32_t rnid, uint8_t* data,
                            const uint8_t* raddress, uint64_t nbytes)
{
     gmt_mem_get_nb(rnid, data, raddress, nbytes);
     gmt_wait_data();
}

GMT_INLINE void gmt_mem_put(uint32_t rnid, uint8_t* raddress,
                            const uint8_t* data, uint64_t num_bytes)
{
    gmt_mem_put_nb(rnid, raddress, data, num_bytes);
    gmt_wait_data();
}

GMT_INLINE void gmt_mem_strided_put(uint32_t rnid, uint8_t* raddress,
                                    const uint8_t* data,
                                    uint64_t chunk_offset,
                                    uint64_t chunck_size,
                                    uint64_t num_chunks)
{
    gmt_mem_strided_put_nb(rnid, raddress, data, chunk_offset, chunck_size, num_chunks);
    gmt_wait_data();
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

GMT_INLINE uint64_t gmt_count_local_elements(gmt_data_t gmt_array) {
  gentry_t *const ga = mem_get_gentry(gmt_array);
  uint64_t nbytes_loc, nbytes_block, goffset_bytes;
  block_partition(node_id, ga->nbytes_tot/ga->nbytes_elem,
        ga->nbytes_elem, gmt_array,
        &nbytes_loc, &nbytes_block, &goffset_bytes);
  return nbytes_loc/ga->nbytes_elem;
}
