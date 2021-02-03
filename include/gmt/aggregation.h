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

#ifndef __AGGREGATION_H__
#define __AGGREGATION_H__

#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/queue.h"
#include "gmt/comm_server.h"
#include "gmt/commands.h"
#include "gmt/profiling.h"
#include "gmt/utils.h"
#include "gmt/queue.h"

#if !ENABLE_SINGLE_NODE_ONLY

typedef struct ag_data_t {
    uint8_t *ptr;
    uint32_t size;
} ag_data_t;

typedef struct block_info_t {
    uint32_t cmds_bytes;        /* bytes used in cmds */
    uint32_t data_bytes;        /* sum of all data.size in data_array */
} block_info_t;

typedef struct cmd_block_t {
    uint64_t tick;
    uint32_t data_cnt;          /* number of data_t in data_array */
    block_info_t block_info;
    uint8_t * cmds;
    ag_data_t * data_array;
} cmd_block_t;

DEFINE_QUEUE_MPMC(cmdb_queue, cmd_block_t *, NUM_CMD_BLOCKS);

/* Node aggregation structure */
typedef struct agm_t {
    /* actual command block array */
    cmd_block_t *cmdbs;

    /* queue and pool */
    cmdb_queue_t queue_cmdb;
    cmdb_queue_t pool_cmdb;

    /* array of pointers to command blocks 
       An entity (worker or helper) will use only 
       its own block pointer  */
    cmd_block_t **p_cmdbs;

    /* bytes equivalent in the queue */
    int32_t equiv_bytes;

    /* tick when last aggregation was performed for this node */
    uint64_t tick;

} agm_t;

/* Array of  node_aggreg structs (one for each remote node ) */
extern agm_t *agms;

void aggreg_init();
void aggreg_destroy();

/* Get the next command pointer from the current block */
INLINE void *agm_get_cmd_na(uint32_t rnid, uint32_t thid,
                            uint32_t cmd_size, uint64_t req_data_size,
                            uint32_t * const granted_data_size)
{
    _assert(rnid < num_nodes);
    _assert(cmd_size <= CMD_BLOCK_SIZE);
    _assert(thid <= NUM_HELPERS + NUM_WORKERS);

    net_buffer_t *buff = comm_server_pop_send_buff(thid);
    while (buff == NULL)
        buff = comm_server_pop_send_buff(thid);

    buff->num_bytes = 0;
    buff->rnode_id = rnid;
    block_info_t bi;
    bi.cmds_bytes = cmd_size;

    if (granted_data_size != NULL) {
        *granted_data_size =
            MIN(COMM_BUFFER_SIZE - sizeof(block_info_t) - cmd_size,
                req_data_size);
        bi.data_bytes = *granted_data_size;
    } else {
        bi.data_bytes = 0;
    }
    netbuffer_append(buff, &bi, sizeof(block_info_t));
    netbuffer_skip(buff, cmd_size);
    void *cmd = (void *)(buff->data + sizeof(block_info_t));
    _assert(cmd != NULL);
    return cmd;
}

INLINE void agm_set_cmd_data_na(uint32_t rnid, uint32_t thid,
                                const uint8_t * data, uint32_t data_size)
{
    _assert(rnid < num_nodes);
    _unused(rnid);
    _assert(thid <= NUM_HELPERS + NUM_WORKERS);
    if (data != NULL) {
        net_buffer_t *buff = comm_server_pop_send_buff(thid);
        while (buff == NULL)
            buff = comm_server_pop_send_buff(thid);
        _assert(buff);
        uint8_t *ptr = buff->data;
        _assert(((block_info_t *) ptr)->data_bytes == data_size);
        _unused(ptr);

        netbuffer_append(buff, data, data_size);
    }
    comm_server_push_send_buff(thid);
}

INLINE uint32_t agm_get_cmdb_eq_bytes(cmd_block_t * cmdb)
{
    return cmdb->block_info.cmds_bytes +
        cmdb->block_info.data_bytes + sizeof(block_info_t);
}

/* Aggregate for a given remote node and send buffer */
INLINE int32_t agm_aggregate_and_send(uint32_t rnid, uint32_t thid,
                                      bool is_timeout)
{
    /* try to get a buffer */
    net_buffer_t *buff = comm_server_pop_send_buff(thid);
    /* if it is a timeout then no need to try again to get a buff */
    if (buff == NULL && is_timeout) {
        return -1;
    }

    while (buff == NULL) {      /* keep trying */
        buff = comm_server_pop_send_buff(thid);
    }

    int32_t rbytes = __sync_fetch_and_sub(&agms[rnid].equiv_bytes,
                                          COMM_BUFFER_SIZE);
    if (!is_timeout && rbytes < 0) {
        /* if rBytes < 0 some other thread is aggregating this buffer */
        (void)__sync_fetch_and_add(&agms[rnid].equiv_bytes, COMM_BUFFER_SIZE);
        return -2;
    }

    buff->num_bytes = 0;
    buff->rnode_id = rnid;
    cmd_block_t *cmdb;

    while (cmdb_queue_pop(&agms[rnid].queue_cmdb, &cmdb)) {

        /* _assert equivalent bytes of cmd_block is <= buffer size */
        _assert(agm_get_cmdb_eq_bytes(cmdb) <= COMM_BUFFER_SIZE);

        if (buff->num_bytes + agm_get_cmdb_eq_bytes(cmdb) <= COMM_BUFFER_SIZE) {

            /* copy block_info and commands */
            netbuffer_append(buff, &cmdb->block_info, sizeof(block_info_t));
            netbuffer_append(buff, cmdb->cmds, cmdb->block_info.cmds_bytes);

            /* copy all data from memory */
            uint32_t i;
            for (i = 0; i < cmdb->data_cnt; i++) {
                netbuffer_append(buff, cmdb->data_array[i].ptr,
                                 cmdb->data_array[i].size);
            }

            INCR_EVENT(AGGREGATION_CMD_BYTES, cmdb->block_info.cmds_bytes);
            INCR_EVENT(AGGREGATION_DATA_BYTES, cmdb->block_info.data_bytes);
            INCR_EVENT(AGGREGATION_BLOCK_INFO_BYTES, sizeof(block_info_t));

            /* cmd_block empty put back into pool */
            cmdb_queue_push(&agms[rnid].pool_cmdb, cmdb);
        } else {
            /* this is impossible, a cmdb should always fit in an empty buffer */
            _assert(buff->num_bytes != 0);
            /* cmdb has not been aggregated on buffer, put back into the queue */
            cmdb_queue_push(&agms[rnid].queue_cmdb, cmdb);
            break;
        }
    }

    if (COMM_BUFFER_SIZE != buff->num_bytes) {
        /* we are aggregating less than SIZE_BUFFERS and  byesEq needs to be restored */
        (void)__sync_fetch_and_add(&agms[rnid].equiv_bytes,
                                   COMM_BUFFER_SIZE - buff->num_bytes);
    }

    if (buff->num_bytes > 0) {
        comm_server_push_send_buff(thid);
        if (is_timeout) {
            COUNT_EVENT(AGGREGATION_ON_TIMEOUT);
        }
        INCR_EVENT(AGGREGATION_BYTE_WASTE, COMM_BUFFER_SIZE - buff->num_bytes);
    }
    return buff->num_bytes;
}

INLINE int32_t agm_push_cmdb(uint32_t rnid, uint32_t thid, bool is_timeout)
{
    cmd_block_t *cmdb = agms[rnid].p_cmdbs[thid];
    if (cmdb == NULL)
        return 0;

    if (cmdb->block_info.cmds_bytes == 0)
        return -1;

    agms[rnid].p_cmdbs[thid] = NULL;
    _assert(cmdb->tick > 0);
    _assert(cmdb->block_info.cmds_bytes > 0);
    cmdb_queue_push(&agms[rnid].queue_cmdb, cmdb);
    int32_t ret = __sync_fetch_and_add(&agms[rnid].equiv_bytes,
                                       agm_get_cmdb_eq_bytes(cmdb));
    if (ret >= (int32_t) COMM_BUFFER_SIZE) {
        int bytes = agm_aggregate_and_send(rnid, thid, is_timeout);
        if (bytes > 0) {
            COUNT_EVENT(AGGREGATION_ON_FULLBLOCK);
        }
    }
    return ret;
}

INLINE void agm_force_aggr_a(int32_t rnid, uint32_t thid)
{
    if (rnid == -1) {
        uint32_t i;
        for (i = 0; i < num_nodes; i++) {
            if (i != node_id) {
                agm_push_cmdb(i, thid, false);
                agm_aggregate_and_send(i, thid, false);
            }
        }
    } else {
        agm_push_cmdb(rnid, thid, false);
        agm_aggregate_and_send(rnid, thid, false);
    }
}

INLINE bool agm_check_cmdb_timeout_a(uint32_t thid, uint64_t * const old_tick)
{
    bool ret = false;
    uint64_t tick = rdtsc();
    if (tick - *old_tick > config.cmdb_check_interv) {
        uint32_t i;
        for (i = 0; i < num_nodes; i++) {
            if (i != node_id) {
                cmd_block_t *cmdb = agms[i].p_cmdbs[thid];
                if (cmdb != NULL) {
                    if (tick - cmdb->tick > config.cmdb_check_interv)
                        agm_push_cmdb(i, thid, true);
                }
            }
        }
        *old_tick = tick;
        ret = true;
    }
    return ret;
}

/* Returns an available cmd_block */
INLINE cmd_block_t *agm_set_cmdb(const uint32_t rnid, const uint32_t thid)
{
    while (!cmdb_queue_pop(&agms[rnid].pool_cmdb, &agms[rnid].p_cmdbs[thid])) {
        int32_t num_bytes = agm_aggregate_and_send(rnid, thid, false);
        if (num_bytes > 0) {
            COUNT_EVENT(AGGREGATION_ON_MISS_CMDB);
        }
    }
    cmd_block_t *cmdb = agms[rnid].p_cmdbs[thid];
    _assert(cmdb != NULL);
    /* resetting cmd_block */
    cmdb->tick = rdtsc();
    cmdb->data_cnt = 0;
    cmdb->block_info.cmds_bytes = 0;
    cmdb->block_info.data_bytes = 0;
    return cmdb;
}

INLINE void *agm_get_cmd_a(uint32_t rnid, uint32_t thid,
                           uint32_t cmd_size, uint64_t req_data_size,
                           uint32_t * const granted_data_size)
{
// JTF - on entry, if granted_data_size is not NULL, * granted_size is minimum transfer size
    _assert(rnid < num_nodes);
    _assert(cmd_size <= CMD_BLOCK_SIZE);

    uint32_t min_transfer = (granted_data_size == NULL) ? 0 : * granted_data_size;
    _assert(cmd_size + min_transfer <= COMM_BUFFER_SIZE);

    cmd_block_t *cmdb = agms[rnid].p_cmdbs[thid];
    if (cmdb == NULL) cmdb = agm_set_cmdb(rnid, thid);

    /* if   we can't fit more commands in this command block OR
            the number of bytes remaining the communication buffer is less than minimum
       then push communication buffer and get a new one
    */
    if (cmdb->block_info.cmds_bytes + cmd_size > CMD_BLOCK_SIZE
        || agm_get_cmdb_eq_bytes(cmdb) + cmd_size + min_transfer > COMM_BUFFER_SIZE) {
        agm_push_cmdb(rnid, thid, false);
        cmdb = agm_set_cmdb(rnid, thid);
    }

    /* if we are requesting a command that needs to send data, check
     * for availability (a command block can't contain more bytes than
     * COMM_BUFFER_SIZE bytes) */
    if (granted_data_size != NULL) {
        int32_t avail_bytes = COMM_BUFFER_SIZE - cmd_size -
            agm_get_cmdb_eq_bytes(cmdb);

        /* if we can't fit more data bytes in this command block */
        if (avail_bytes <= 0) {
            agm_push_cmdb(rnid, thid, false);
            cmdb = agm_set_cmdb(rnid, thid);
            /* recompute available data size */
            avail_bytes = COMM_BUFFER_SIZE - sizeof(block_info_t) - cmd_size;
        }
        _assert(avail_bytes > 0);
        *granted_data_size = MIN((uint64_t) avail_bytes, req_data_size);
    }

    void *ret = (void *)&cmdb->cmds[cmdb->block_info.cmds_bytes];
    cmdb->block_info.cmds_bytes += cmd_size;
    _assert(agm_get_cmdb_eq_bytes(cmdb) <= COMM_BUFFER_SIZE);
    return ret;
}

/* Set data for the current command */
INLINE void agm_set_cmd_data_a(uint32_t rnid, uint32_t thid,
                               const uint8_t * data, uint32_t data_size)
{
    if (data == NULL) {
        _assert(data_size == 0);
        return;
    }
    _assert(rnid < num_nodes);
    cmd_block_t *cmdb = agms[rnid].p_cmdbs[thid];

    _assert(cmdb != NULL);
    _assert(data_size > 0);
    ag_data_t *data_str = &(cmdb->data_array[cmdb->data_cnt]);

    data_str->ptr = (uint8_t *) data;
    data_str->size = data_size;
    cmdb->data_cnt++;
    cmdb->block_info.data_bytes += data_size;
    _assert(cmdb->data_cnt <= CMD_BLOCK_SIZE/sizeof(cmd_gen_t));
    _assert(agm_get_cmdb_eq_bytes(cmdb) <= COMM_BUFFER_SIZE);
}

INLINE void *agm_get_cmd(uint32_t rnid, uint32_t thid,
                         uint32_t cmd_size, uint64_t req_data_size,
                         uint32_t * const granted_data_size)
{
#if ENABLE_AGGREGATION
    return agm_get_cmd_a(rnid, thid,
                         cmd_size, req_data_size, granted_data_size);
#else
    return agm_get_cmd_na(rnid, thid,
                          cmd_size, req_data_size, granted_data_size);
#endif

}

INLINE void agm_set_cmd_data(uint32_t rnid, uint32_t thid,
                             const uint8_t * data, uint32_t data_size)
{
#if ENABLE_AGGREGATION
    agm_set_cmd_data_a(rnid, thid, data, data_size);
#else
    agm_set_cmd_data_na(rnid, thid, data, data_size);
#endif
}

INLINE bool agm_check_cmdb_timeout(uint32_t thid, uint64_t * const old_tick)
{
#if ENABLE_AGGREGATION
    return agm_check_cmdb_timeout_a( thid, old_tick);
#else
    return false;
#endif
}

INLINE void agm_force_aggr(int32_t rnid, uint32_t thid)
{
#if ENABLE_AGGREGATION
    agm_force_aggr_a( rnid,  thid);
#endif
}
/* ENABLE_SINGLE_NODE_ONLY */
#else
INLINE void *agm_get_cmd(uint32_t rnid, uint32_t thid,
                         uint32_t cmd_size, uint64_t req_data_size,
                         uint32_t * const granted_data_size)
{
    _unused(rnid);
    _unused(thid);
    _unused(cmd_size);
    _unused(req_data_size);
    _unused(granted_data_size);
    return NULL;
}

INLINE void agm_set_cmd_data(uint32_t rnid, uint32_t thid,
                             const uint8_t * data, uint32_t data_size)
{
    _unused(rnid);
    _unused(thid);
    _unused(data);
    _unused(data_size);
}

INLINE bool agm_check_cmdb_timeout(uint32_t thid, uint64_t * const old_tick)
{
    _unused(thid);
    _unused(old_tick);    
    return true;
}

INLINE void agm_force_aggr(int32_t rnid, uint32_t thid)
{
    _unused(rnid);
    _unused(thid);
}


#endif

#endif                          /* __AGGREGATION_H__ */
