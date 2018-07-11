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

#ifndef __HELPER__H__
#define __HELPER__H__

#include <pthread.h>
#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/commands.h"
#include "gmt/comm_server.h"
#include "gmt/aggregation.h"
#include "gmt/memory.h"
#include "gmt/profiling.h"
#include "gmt/timing.h"
#include "gmt/uthread.h"

#if !(ENABLE_SINGLE_NODE_ONLY)
typedef struct helper_tag {
    net_buffer_t tmp_buff;
    pthread_t pthread;

    /* start and end id of the nodes for which this helper 
     *is responsible to check timeouts */
    uint32_t part_start_node_id;
    uint32_t part_end_node_id;
    uint32_t aggr_timeout_interval;
    
#if !DTA
    mtask_t **mt_res;
    uint32_t num_mt_res;
#endif
} helper_t;
#endif

extern helper_t *helpers;

void helper_team_run();
void helper_team_init();
void helper_team_stop();
void helper_team_destroy();

INLINE void helper_send_exec_completed(uint32_t rnid, uint32_t thid,
                                       int32_t pid, uint32_t nest_lev,
                                       uint64_t ret_buf_ptr, 
                                       uint64_t ret_size_ptr,
                                       uint32_t ret_size_value,
                                       uint8_t * loc_ret_buf,
                                       gmt_handle_t handle)
{
    if (ret_size_value > UTHREAD_MAX_RET_SIZE)
        ERRORMSG(" execute() return buffer size cannot be larger than %u "
                 "(see MAX_RET_SIZE in config.h).\n", UTHREAD_MAX_RET_SIZE);

    cmd_exec_compl_t *cmd;
    cmd = (cmd_exec_compl_t *) agm_get_cmd(rnid, thid,
                                           sizeof(cmd_exec_compl_t) +
                                           ret_size_value, 0, NULL);
    cmd->type = GMT_CMD_EXEC_COMPL;
    cmd->pid = pid;
    cmd->nest_lev = nest_lev;
    cmd->ret_buf_ptr = ret_buf_ptr;
    cmd->ret_size_ptr = ret_size_ptr;
    cmd->ret_size_value = ret_size_value;
    cmd->handle = handle;
    
    /* copy local return buffer */
    if ( ret_size_value > 0 && loc_ret_buf != NULL)
        memcpy(cmd + 1, loc_ret_buf, ret_size_value);
    agm_set_cmd_data(rnid, thid, NULL, 0);
}

INLINE void helper_check_in_buffers(uint32_t hid);


#endif
