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

#if SCHEDULER

#include <stdbool.h>

#include "gmt/scheduler.h"
#include "gmt/mtask.h"

volatile bool scheduler_stop_flag;
scheduler_t scheduler;

void scheduler_stop()
{
  while (scheduler_stop_flag) ;
  scheduler_stop_flag = true;
  pthread_join(scheduler.pthread, NULL);
}

void *scheduler_loop(void *)
{
	/* in thread to core */
  if (config.thread_pinning) {
    uint32_t thread_id = get_thread_id();
	uint32_t core = select_core(thread_id, config.num_cores, config.stride_pinning);
	pin_thread(core);
	if (node_id == 0) {
	  DEBUG0(printf("pining CPU %u with pthread_id %u\n", core, thread_id););
	}
  }

  mtask_t *buf;
  while (!scheduler_stop_flag) {
	if (sched_queue_pop(&mtm.mtasks_sched_in_queues[scheduler.in_rr_cnt], (void **)&buf)) {
#ifdef TRACE_QUEUES
        ++scheduler.pop_hits;
#endif
        sched_queue_push(&mtm.mtasks_sched_out_queues[buf->qid], buf);
	}
	else {
#ifdef TRACE_QUEUES
      ++scheduler.pop_misses;
#endif
	  if (++scheduler.in_rr_cnt == scheduler.in_degree)
	    scheduler.in_rr_cnt = 0;
    }
  }
  pthread_exit(NULL);
}

void scheduler_run()
{
  scheduler_stop_flag = false;

  pthread_attr_t attr;
  pthread_attr_init(&attr);

  /* set stack address for this worker */
  void *stack_addr =
      (void *)((uint64_t)pt_stacks + (NUM_WORKERS + NUM_HELPERS + 1) * PTHREAD_STACK_SIZE);

  int ret = pthread_attr_setstack(&attr, stack_addr, PTHREAD_STACK_SIZE);
  if (ret)
      perror("FAILED TO SET STACK PROPERTIES"), exit(EXIT_FAILURE);

  ret =
      pthread_create(&scheduler.pthread, &attr, &scheduler_loop, NULL);
  if (ret)
      perror("FAILED TO CREATE SCHEDULER"), exit(EXIT_FAILURE);
}

void scheduler_init()
{
  scheduler.in_degree = config.num_workers + config.num_helpers;
  scheduler.out_degree = config.num_workers;
  scheduler.in_rr_cnt = 0;

  scheduler_stop_flag = true;
}

void scheduler_destroy()
{
#if TRACE_QUEUES
    char tfname[128];
    sprintf(tfname, "qt_scheduler_n%d", node_id);
    FILE *tf = fopen(tfname, "w");
    fprintf(tf, "[n=%u] mtasks: pop-hits = %llu\tpop-misses = %llu",
    	       node_id, scheduler.pop_hits, scheduler.pop_misses);
    fprintf(tf, "\t(hit-rate = %g%%)\n", (float)scheduler.pop_hits / (scheduler.pop_hits + scheduler.pop_misses) * 100);
    fclose(tf);
#endif
}

#endif /* SCHEDULER */
