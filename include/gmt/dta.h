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

#ifndef __DTA__H__
#define __DTA__H__

#if DTA

#include <stdint.h>
#include <pthread.h>
#include <iostream>
#include <fstream>

#include "gmt/config.h"
#include "gmt/queue.h"
#include "gmt/mtask.h"
#include "gmt/queue.h"

DEFINE_QUEUE_SPSC(spsc, void *);

typedef struct dta_chunk_tag {
	mtask_t *mtasks;
	dta_chunk_tag *next;
} dta_chunk_t;

typedef struct dta_tag {
	/* identifier of the thread associated with the allocator */
	uint32_t id;

	/*
	 * the local list of mtasks chunks
	 */
	dta_chunk_t *chunks_head, *chunk_last;
	uint32_t max_chunks, num_chunks;

	/*
	 * mtask caching from the local chunk list
	 */
	mtask_t *local_cache;
	uint32_t local_cache_idx;
	dta_chunk_t *chunk_last_cached;

	/*
	 * memory recycling:
	 * - upon freeing, the calling allocator gives the mtasks memory back to the
	 *   allocator that allocated the memory, by pushing it into the
	 *   corresponding recycling queue
	 * - upon allocating, in case of cache miss, the calling allocator try to
	 *   get tasks from the recycling queues
	 */
	spsc_t *in_rec_queues;
	uint32_t in_rec_deg; //number of input recycling queues
	mtask_t **in_rec_buf; //input recycling buffer
	uint32_t out_rec_deg; //number of output recycling queues
	spsc_t **out_rec_queues; //(pointers to) output recycling queues

#if !NO_RESERVE
	/*
	 * a counter to track the mtasks available for allocation, that can be used
	 * for reservation purposes. The counter is signed so that overbooking is
	 * supported.
	 */
	int64_t num_avail;
#endif

	/*
	 * round-robin counters
	 */
	uint32_t mapping_rr_cnt; //for init-time mtasks-to-queue mapping
	uint32_t qin_rr_cnt; //for selecting input recycling queues
} dta_t;

typedef struct dta_manager_tag {
	dta_t *w_alloc, *h_alloc;
	uint32_t max_worker_chunks, max_helper_chunks;
} dta_manager_t;

extern dta_manager_t dtam;

INLINE dta_chunk_t *dta_make_chunk(dta_t *dta) {
	uint32_t i;
	dta_chunk_t *c = (dta_chunk_t *) _malloc(sizeof(dta_chunk_t));
	c->mtasks = (mtask_t *) _malloc(config.dta_chunk_size * sizeof(mtask_t));
	for (i = 0; i < config.dta_chunk_size; ++i) {
		mtm_mtask_init(&c->mtasks[i], dta->id, &dta->mapping_rr_cnt);
		mtm_mtask_bind_allocator(&c->mtasks[i], dta->id);
	}
	c->next = NULL;
	return c;
}

INLINE void dta_add_chunk(dta_t *dta) {
	dta_chunk_t *c = dta_make_chunk(dta);
	dta->chunk_last->next = c;
	dta->chunk_last = c;
	++dta->num_chunks;
}

#if !NO_RESERVE
INLINE int64_t dta_mtasks_reserve(dta_t *dta, int64_t n) {
	/* try satisfying with the already available mtasks */
	if(dta->num_avail >= n) {
		dta->num_avail -= n;
		return n;
	}

	/* try overbooking by including mtasks from recycling queues */
	uint32_t qi;
	int64_t est = 0;
	for(qi = 0; qi < dta->in_rec_deg; ++qi)
		est += spsc_size(&dta->in_rec_queues[qi]); //lower-bound on queue size
	if(dta->num_avail + est >= n) {
		dta->num_avail = dta->num_avail - n;
		return n;
	}

	/* try satisfyng partially */
	int64_t res = 0;
	if(dta->num_avail + est) {
		res = dta->num_avail + est;
		dta->num_avail = -est;
	}
	return res;
}
#endif

INLINE mtask_t *dta_mtask_alloc(dta_t *dta) {
	/* try from cached local chunk */
	if(dta->local_cache && dta->local_cache_idx < config.dta_chunk_size) {
		return (&(dta->local_cache[dta->local_cache_idx++]));
	}

	/* try caching another local chunk */
	if (dta->chunk_last_cached->next) {
		dta->local_cache = dta->chunk_last_cached->next->mtasks;
		dta->local_cache_idx = 0;
		dta->chunk_last_cached = dta->chunk_last_cached->next;
		return (&(dta->local_cache[dta->local_cache_idx++]));
	}

	/* try caching from recycle queues */
	uint32_t qcnt = 0, src;
	spsc_t *qin;
	mtask_t *dst_mt;
	while (qcnt++ < dta->in_rec_deg) {
		src = dta->qin_rr_cnt;
		qin = &dta->in_rec_queues[src];
		if (spsc_pop(qin, (void **) &dst_mt)) {
#if !NO_RESERVE
			++dta->num_avail;
#endif
          return dst_mt;
        }
		/* skip to the next recycle-input queue */
		if (++dta->qin_rr_cnt == dta->in_rec_deg)
			dta->qin_rr_cnt = 0;
	}

	/* try creating and caching a chunk */
	if (dta->num_chunks < dta->max_chunks) {
		dta_add_chunk(dta);
		dta->local_cache = dta->chunk_last->mtasks;
		dta->local_cache_idx = 0;
		dta->chunk_last_cached = dta->chunk_last;
		return (&(dta->local_cache)[dta->local_cache_idx++]);
	}

	return NULL;
}

INLINE void dta_mtask_free(dta_t *dta, mtask_t *mt) {
  spsc_push(dta->out_rec_queues[mt->allocator_id], mt);
}

INLINE uint64_t dta_mem_footprint(dta_t *dta) {
    return dta->num_chunks * config.dta_chunk_size;
}

uint32_t max_worker_chunks();
uint32_t max_helper_chunks();

void dtam_init();
void dtam_destroy();

#endif

#endif /* __DTA__H__ */
