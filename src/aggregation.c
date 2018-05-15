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

#include "gmt/aggregation.h"

#if !(ENABLE_SINGLE_NODE_ONLY)

#if ENABLE_AGGREGATION
agm_t *agms;

void aggreg_init_node(agm_t * agm)
{
    agm->equiv_bytes = 0;
    agm->tick = rdtsc();

    cmdb_queue_init(&agm->queue_cmdb);
    cmdb_queue_init(&agm->pool_cmdb);
    agm->cmdbs = (cmd_block_t*)_malloc(NUM_CMD_BLOCKS * sizeof(cmd_block_t));

    uint32_t i;
    for (i = 0; i < NUM_CMD_BLOCKS; i++) {
        memset(&agm->cmdbs[i], 0, sizeof(cmd_block_t));
        agm->cmdbs[i].cmds = (uint8_t*)_calloc(CMD_BLOCK_SIZE, 1);
        /* Divided by the smallest cmd stored in cmd_block. 
           its size must be larger than the number of cmds */
        uint32_t size = CEILING(CMD_BLOCK_SIZE,sizeof(cmd_gen_t));
        agm->cmdbs[i].data_array = (ag_data_t*)_calloc( size * sizeof(ag_data_t), 1);

        /* fill pool */
        cmdb_queue_push(&agm->pool_cmdb, &agm->cmdbs[i]);
    }

    agm->p_cmdbs = (cmd_block_t**)_malloc(sizeof(cmd_block_t *) * NUM_SEND_CHANNELS);
    for (i = 0; i < NUM_SEND_CHANNELS; i++)
        agm->p_cmdbs[i] = NULL;
}

void aggreg_init()
{
    agms = (agm_t*)_malloc(sizeof(agm_t) * num_nodes);
    uint32_t i;

    /* Initialize node aggregation structures */
    for (i = 0; i < num_nodes; i++)
        if (i != node_id)
            aggreg_init_node(&agms[i]);
}
#else
void aggreg_init()
{
}
#endif

void aggreg_destroy()
{
#if ENABLE_AGGREGATION
    uint32_t i, j;
    for (i = 0; i < num_nodes; i++) {
        if (i != node_id) {
            for (j = 0; j < NUM_CMD_BLOCKS; j++) {                
                free(agms[i].cmdbs[j].cmds);
                free(agms[i].cmdbs[j].data_array);
            }
            cmdb_queue_destroy(&agms[i].queue_cmdb);
            cmdb_queue_destroy(&agms[i].pool_cmdb);
            free(agms[i].p_cmdbs);
            free(agms[i].cmdbs);
        }
    }
    free(agms);
#endif
}

#endif
