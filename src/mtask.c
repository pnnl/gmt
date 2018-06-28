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

#include "gmt/mtask.h"
#include "gmt/network.h"

mtask_manager_t mtm;

void mtm_init()
{
	uint32_t i;

	/* initialize structures for task allocation */
#if !DTA
#if ALL_TO_ALL || SCHEDULER
	mtm.pool_size = config.num_workers * config.mtasks_per_queue;
#else
	mtm.pool_size = config.num_mtasks_queues * config.mtasks_per_queue;
#endif
	qmpmc_init(&mtm.mtasks_pool, mtm.pool_size);
	mtm.mtasks = (mtask_t *) _malloc(mtm.pool_size * sizeof(mtask_t));
	uint32_t cnt = 0;
	for (i = 0; i < mtm.pool_size; i++) {
		mtm.mtasks[i].args = NULL;
		mtm.mtasks[i].largs = NULL;
		mtm.mtasks[i].args_bytes = 0;
		mtm.mtasks[i].max_args_bytes = 0;
		mtm.mtasks[i].qid = cnt;
		qmpmc_push(&mtm.mtasks_pool, &mtm.mtasks[i]);
		cnt++;
#if ALL_TO_ALL || SCHEDULER
		if (cnt >= config.num_workers)
#else
		if (cnt >= config.num_mtasks_queues)
#endif
			cnt = 0;
	}
	mtm.num_mtasks_avail = mtm.pool_size;
#endif

	/* initialize structures fot task scheduling */
#if ALL_TO_ALL
    uint32_t j;
    mtm.worker_in_degree = config.num_workers + config.num_helpers;
    mtm.mtasks_queues = (spsc_t **)_malloc(sizeof(spsc_t *) * (config.num_workers + config.num_helpers));
    for (i = 0; i < config.num_workers + config.num_helpers; i++) {
    	mtm.mtasks_queues[i] = (spsc_t *)_malloc(sizeof(spsc_t) * config.num_workers);
    	for(j = 0; j < config.num_workers; ++j)
    		spsc_init(&mtm.mtasks_queues[i][j], config.mtasks_per_queue);
    }
#elif !SCHEDULER
    mtm.worker_in_degree = config.num_mtasks_queues;
    mtm.mtasks_queue = (qmpmc_t *)_malloc(sizeof(qmpmc_t) * config.num_mtasks_queues);
    for (i = 0; i < config.num_mtasks_queues; i++)
        qmpmc_init(&mtm.mtasks_queue[i], config.mtasks_per_queue);
#else
    mtm.worker_in_degree = 1;
    mtm.mtasks_sched_in_queues = (spsc_t *)_malloc(sizeof(spsc_t) * (config.num_workers + config.num_helpers));
    mtm.mtasks_sched_out_queues = (spsc_t *)_malloc(sizeof(spsc_t) * config.num_workers);
    for (i = 0; i < config.num_workers; i++) {
    	spsc_init(&mtm.mtasks_sched_in_queues[i], mtm.pool_size);
    	spsc_init(&mtm.mtasks_sched_out_queues[i], config.mtasks_per_queue);
    }
    for (; i < config.num_workers + config.num_helpers; i++)
    	spsc_init(&mtm.mtasks_sched_in_queues[i], mtm.pool_size);
#endif

    /* initialize structures for handle management */
    mtm.handles =
        (g_handle_t *)_malloc(config.max_handles_per_node * num_nodes * sizeof(g_handle_t));

    for (i = 0; i < config.max_handles_per_node * num_nodes; i++) {
        mtm.handles[i].mtasks_terminated = 0;
        mtm.handles[i].mtasks_created = 0;
        mtm.handles[i].has_left_node = false;
        mtm.handles[i].gtid = (uint32_t) - 1;
        mtm.handles[i].status = HANDLE_NOT_USED;
    }
    mtm.num_used_handles = 0;

    uint32_t start_handleid = node_id * config.max_handles_per_node;
    handleid_queue_init(&mtm.handleid_pool);
    for (i = start_handleid; i < start_handleid + config.max_handles_per_node;
         i++)
        handleid_queue_push(&mtm.handleid_pool, i);

    /* initialize structures for remote task reservation */
    mtm.num_mtasks_res_array =
        (int64_t volatile *)_malloc(num_nodes * sizeof(int64_t));
    mtm.mtasks_res_pending = (bool *) _malloc(num_nodes * sizeof(bool));

    for (i = 0; i < num_nodes; i++) {
        mtm.num_mtasks_res_array[i] = 0;
        mtm.mtasks_res_pending[i] = false;
    }

    // Pre-reserving remote mtasks
    uint64_t to_reserve = config.mtasks_res_block_rem*(num_nodes-1);
    uint64_t res = mtm_reserve_mtask_block(to_reserve);
    _assert(res == to_reserve);
    for (i = 0; i < num_nodes; i++){
      if (i != node_id) 
        mtm_mark_reservation_block(i, config.mtasks_res_block_rem);
    }

    /* initialize iterations counter */
    mtm.total_its = 0;
}

void mtm_destroy()
{
	/* destroy structures for task allocation */
#if !DTA
    uint32_t i;
    for (i = 0; i < mtm.pool_size; i++)
    if(mtm.mtasks[i].args)
        free(mtm.mtasks[i].args);
    free(mtm.mtasks);
    qmpmc_destroy(&mtm.mtasks_pool);
#endif

    /* destroy structures for task scheduling */
#if ALL_TO_ALL
    uint32_t j;
    for (i = 0; i < config.num_workers + config.num_helpers; i++) {
    	for(j = 0; j < config.num_workers; ++j)
    	    spsc_destroy(&mtm.mtasks_queues[i][j]);
        free(mtm.mtasks_queues[i]);
    }
    free(mtm.mtasks_queues);
#elif !SCHEDULER
    for (i = 0; i < config.num_mtasks_queues; i++)
        qmpmc_destroy(&mtm.mtasks_queue[i]);
    free(mtm.mtasks_queue);
#else
    for (i = 0; i < config.num_workers; i++) {
    	spsc_destroy(&mtm.mtasks_sched_in_queues[i]);
    	spsc_destroy(&mtm.mtasks_sched_out_queues[i]);
    }
    for (; i < config.num_workers + config.num_helpers; i++)
    	spsc_destroy(&mtm.mtasks_sched_in_queues[i]);
    free(mtm.mtasks_sched_in_queues);
    free(mtm.mtasks_sched_out_queues);
#endif

    /* destroy everything else */
    handleid_queue_destroy(&mtm.handleid_pool);
    free((void *)mtm.num_mtasks_res_array);
    free(mtm.mtasks_res_pending);
    free(mtm.handles);
}
