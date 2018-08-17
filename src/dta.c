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

#include "gmt/config.h"

#if DTA
#include "gmt/dta.h"

dta_manager_t dtam;

void dta_init_(dta_t *dta, uint32_t id, uint32_t in_rdeg, uint32_t out_rdeg,
		uint32_t max_chunks, uint32_t prealloc_chunks) {
	uint32_t i;
	uint32_t max_tasks = config.dta_chunk_size * max_chunks;

	dta->id = id;
	dta->max_chunks = max_chunks;
	dta->mapping_rr_cnt = 0;
	dta->qin_rr_cnt = 0;
#if !NO_RESERVE
	dta->num_avail = max_tasks;
#endif

	/* preallocate chunks and set up the cache */
	dta->chunk_last = dta->chunks_head = dta_make_chunk(dta);
	++dta->num_chunks;
	for(i = 0; i < prealloc_chunks - 1; ++i)
		dta_add_chunk(dta);
	dta->local_cache = dta->chunks_head->mtasks;
	dta->local_cache_idx = 0;
	dta->chunk_last_cached = dta->chunks_head;

	/* input-side recycling */
	_assert(in_rdeg);
	dta->in_rec_deg = in_rdeg;
	dta->in_rec_queues = (spsc_t *)_malloc(in_rdeg * sizeof(spsc_t));
	for(i = 0; i < in_rdeg; ++i)
		spsc_init(&dta->in_rec_queues[i], max_tasks);

	/* output-side recycling */
	dta->out_rec_deg = out_rdeg;
	if(out_rdeg)
		dta->out_rec_queues = (spsc_t **) _malloc(out_rdeg * sizeof(spsc_t *));
}

void dta_worker_init(dta_t *dta, uint32_t wid) {
	dta_init_(dta, wid, config.num_workers, config.num_workers + config.num_helpers,
			dtam.max_worker_chunks, config.dta_prealloc_worker_chunks);
}

void dta_helper_init(dta_t *dta, uint32_t hid) {
	dta_init_(dta, config.num_workers + hid, config.num_workers, 0,
			dtam.max_helper_chunks, config.dta_prealloc_helper_chunks);
}

void dta_clear_chunks(dta_t *dta) {
	dta_chunk_t *head = dta->chunks_head, *tmp;
	uint32_t i;
	while (head) {
		tmp = head;
		head = tmp->next;
		for (i = 0; i < config.dta_chunk_size; ++i)
			mtm_mtask_destroy(&tmp->mtasks[i]);
		free(tmp->mtasks);
		free(tmp);
	}
	dta->chunks_head = dta->chunk_last = NULL;
	dta->num_chunks = 0;
}

void dta_destroy(dta_t *dta) {
	uint32_t i;

	/* output-side recycling */
	if (dta->out_rec_deg)
		free(dta->out_rec_queues);

	/* input-side recycling */
	for (i = 0; i < dta->in_rec_deg; ++i)
		spsc_destroy(&dta->in_rec_queues[i]);
	free(dta->in_rec_queues);

	/* destroy mtasks memory */
	dta_clear_chunks(dta);
}

void dta_rec_bind(dta_t *dta, dta_t *other, uint32_t rid) {
	_assert(rid < dta->out_rec_deg);
	_assert(dta->id < other->in_rec_deg);
        dta->out_rec_queues[rid] = &other->in_rec_queues[dta->id];
}

/*
 * compute allocation sizing so that no more than
 * config.num_mtasks_queues * config.mtasks_per_queue
 * mtasks are allocated to be scheduled
 */
uint32_t max_worker_chunks() {
  uint32_t max_worker_pool_size;
  max_worker_pool_size = config.num_mtasks_queues;
  max_worker_pool_size *= config.mtasks_per_queue;
  max_worker_pool_size /= (config.num_workers * num_nodes);
  return max_worker_pool_size / config.dta_chunk_size;
}

uint32_t max_helper_chunks() {
  uint32_t max_worker_pool_size, max_helper_pool_size;
  max_helper_pool_size = config.num_mtasks_queues;
  max_helper_pool_size *= config.mtasks_per_queue;
  max_helper_pool_size *= (num_nodes - 1);
  max_helper_pool_size /= config.num_helpers;
  max_helper_pool_size /= num_nodes;
  return max_helper_pool_size / config.dta_chunk_size;
}

void dtam_init() {
	uint32_t i;

	dtam.max_worker_chunks = max_worker_chunks();
	dtam.max_helper_chunks = max_helper_chunks();

	/* initialize per-worker allocators */
	dtam.w_alloc = (dta_t *)_malloc(config.num_workers * sizeof(dta_t));
	for(i = 0; i < config.num_workers; ++i)
		dta_worker_init(&dtam.w_alloc[i], i);

	/* initialize per-helper allocators */
	dtam.h_alloc = (dta_t *)_malloc(config.num_helpers * sizeof(dta_t));
	for(i = 0; i < config.num_helpers; ++i)
		dta_helper_init(&dtam.h_alloc[i], i);

	/* make the bindings for recycling */
	uint32_t j;
	for(i = 0; i < config.num_workers; ++i) {
		for(j = 0; j < config.num_workers; ++j)
			dta_rec_bind(&dtam.w_alloc[i], &dtam.w_alloc[j], j);
		for(j = 0; j < config.num_helpers; ++j)
			dta_rec_bind(&dtam.w_alloc[i], &dtam.h_alloc[j], config.num_workers + j);
	}
}

void dtam_destroy() {
	uint32_t i;

	for(i = 0; i < config.num_workers; ++i)
	    dta_destroy(&dtam.w_alloc[i]);
	for(i = 0; i < config.num_helpers; ++i)
		dta_destroy(&dtam.h_alloc[i]);
	free(dtam.w_alloc);
	free(dtam.h_alloc);
}
#endif
