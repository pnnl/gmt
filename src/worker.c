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

#include <stdlib.h>
#include <signal.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <execinfo.h>
#include <sys/mman.h>

#include "gmt/worker.h"
#include "gmt/mtask.h"

/* gmt_main arguments */
extern uint8_t *gm_args;
extern uint32_t gm_argc;
extern uint32_t gm_args_bytes;

/* Stop flag used to stop the workers */
volatile bool workers_stop_flag;

/* worker array */
worker_t *workers;

extern uint32_t prime_numbers[1000];

void worker_team_init()
{
    workers = (worker_t *)_malloc(sizeof(worker_t) * NUM_WORKERS);
    /* initialize all stacks */
    uthread_init_all();

    uint32_t i;
    for (i = 0; i < NUM_WORKERS; i++) {
        uthread_queue_init(&workers[i].uthread_queue);
        uthread_queue_init(&workers[i].uthread_pool);

        /* initialize a default random generator (in case the user calls 
           gmt_rand without calling before gmt_srand) */
        workers[i].rand_seed = (node_id * prime_numbers[646] +
                                i * prime_numbers[223]) * prime_numbers[103];

        workers[i].cnt_mtasks_check = 0;
        workers[i].tick_cmdb_timeout = rdtsc();
        workers[i].cnt_print_sched = 0;
        workers[i].rr_cnt = 0;
        workers[i].num_mt_res = 0;
        workers[i].num_mt_ret = 0;
        workers[i].mt_res = (mtask_t **)_malloc(config.mtasks_res_block_loc * sizeof(mtask_t *));
        workers[i].mt_ret = (mtask_t **)_malloc(config.mtasks_res_block_loc * sizeof(mtask_t *));
        
        uint32_t j;
        for (j = 0; j < NUM_UTHREADS_PER_WORKER; j++) {
            uint32_t tid = i * NUM_UTHREADS_PER_WORKER + j;

            /* Create the uthread initial uthreads context */
            gmt_init_ctxt(&uthreads[tid].ucontext,
                          (void *)((uint64_t) ut_stacks +
                                   (tid) * UTHREAD_MAX_STACK_SIZE),
                          UTHREAD_MAX_STACK_SIZE, &workers[i].worker_ctxt);
            uthread_queue_push(&workers[i].uthread_pool, &uthreads[tid]);
        }
    }

    /* set stop flag to true as workers have not started yet */
    workers_stop_flag = true;
}

extern int log2pagesize;

/* 
   This is a segmentation fault signal handler each worker executes
   when a uthread uses an address outside its stack range, if the address
   is in the area of expandable stack, this handler set the new stack 
   size. When a task completes, the uthread keeps the stacks expanded 
   and it does not return the memory */
void worker_sigsegv_handler(int signum, siginfo_t * info, void *data)
{
    _assert(info != NULL);
    (void)signum;
    (void)data;

#if !ENABLE_EXPANDABLE_STACKS
    _DEBUG("Segmentation fault at address %p\n", info->si_addr);
    btrace();
    _exit(EXIT_FAILURE);
#endif
    /* this is a real segmentation fault outside task stack space 
       just notify and kill the program */
    if (((long)info->si_addr < UTHREADS_STACK_ENTRY_ADDRESS) ||
        ((long)info->si_addr > UTHREADS_STACK_ENTRY_ADDRESS +
         UTHREAD_MAX_STACK_SIZE * NUM_WORKERS * NUM_UTHREADS_PER_WORKER)) {
        _DEBUG("Segmentation fault at address %p\n", info->si_addr);
        btrace();
        _exit(EXIT_FAILURE);
    }

    uint32_t tid = ((long)info->si_addr - UTHREADS_STACK_ENTRY_ADDRESS)
        / UTHREAD_MAX_STACK_SIZE;
    _assert(tid < NUM_WORKERS * NUM_UTHREADS_PER_WORKER);

    long stack_addr =
        UTHREADS_STACK_ENTRY_ADDRESS + (tid + 1) * UTHREAD_MAX_STACK_SIZE;

    _assert(log2pagesize != 0);
    uint64_t mask_page = ~((1l << log2pagesize) - 1);

    long new_size = stack_addr - ((long)info->si_addr & mask_page);

    uthread_set_stack(tid, new_size);
}

void *worker_loop(void *args)
{
	/* initialize thread index for the MPMC queue */
	qmpmc_assign_tid();

    uint32_t thread_id = get_thread_id();
    uint32_t wid = (uint32_t) ((uint64_t) args);
    _assert(wid == thread_id);

    /* worker 0 of node 0 insert first task */
    if (node_id == 0 && wid == 0) {
        mtask_t *mt = worker_pop_mtask_pool(wid);
        _assert(mt != NULL);
        mtm_push_mtask_queue(mt, (void *)gmt_main, gm_args_bytes, gm_args, -1, 0,
                             MTASK_GMT_MAIN, gm_argc, gm_argc + 1, 1,
                             GMT_DATA_NULL, NULL, NULL, GMT_HANDLE_NULL);
    }

    /* Registering signal handler for seg fault */
    struct sigaction action;
    bzero(&action, sizeof(action));
    action.sa_flags = SA_SIGINFO | SA_STACK;
    action.sa_sigaction = &worker_sigsegv_handler;
    sigaction(SIGSEGV, &action, NULL);

    /* create a stack that is going to be used by the signal
       handler used by this pthread */
    stack_t segv_stack;
    segv_stack.ss_sp = valloc(1024 * 1024l);
    segv_stack.ss_flags = 0;
    segv_stack.ss_size = 1024 * 1024l;
    sigaltstack(&segv_stack, NULL);

    /* thread pinning if enabled */
    if (config.thread_pinning) {
        uint32_t core = select_core(thread_id, config.num_cores,
                                    config.stride_pinning);
        pin_thread(core);
        if (node_id == 0) {
            DEBUG0(printf("pining CPU %u with pthread_id %u\n",
                  core, thread_id););
        }
    }

    while (!workers_stop_flag)
        worker_schedule(-1, thread_id);

    /* free stack used during segmentation fault */
    free(segv_stack.ss_sp);

    if (thread_id == 0)
        return NULL;
    else
        pthread_exit(NULL);
}

void worker_team_run()
{
    workers_stop_flag = false;
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    int64_t i;
    for (i = 1; i < NUM_WORKERS; i++) {
        /* set stack address for this worker */
        void *stack_addr =
            (void *)((uint64_t) pt_stacks + i * PTHREAD_STACK_SIZE);
        int ret = pthread_attr_setstack(&attr, stack_addr, PTHREAD_STACK_SIZE);
        if (ret)
            perror("FAILED TO SET STACK PROPERTIES"), exit(EXIT_FAILURE);
        ret =
            pthread_create(&workers[i].pthread, &attr, &worker_loop, (void *)i);
        if (ret)
            perror("FAILED TO CREATE WORKER"), exit(EXIT_FAILURE);
    }
    i = 0;
    worker_loop((void *)i);

    for (i = 1; i < NUM_WORKERS; i++)
        pthread_join(workers[i].pthread, NULL);
}

void worker_team_stop()
{
    while (workers_stop_flag) ;
    workers_stop_flag = true;
}

void worker_team_destroy()
{
    uint32_t i;
    for (i = 0; i < NUM_WORKERS; i++) {
        uthread_queue_destroy(&workers[i].uthread_queue);
        uthread_queue_destroy(&workers[i].uthread_pool);
        free(workers[i].mt_res);
        free(workers[i].mt_ret);
    }

    uthread_destroy_all();
    free(workers);
}

extern int realRet;

void worker_task_wrapper(uint32_t start_it_L32, uint32_t start_it_H32)
{
    uint64_t start_it = (uint64_t) FROM_32_TO_64(start_it_L32, start_it_H32);
    /* get uthread and mark the task as RUNNING */
    uthread_t *ut = &uthreads[uthread_get_tid()];
    ut->tstatus = TASK_RUNNING;
    _assert(ut->wid == get_thread_id());
    _assert(ut->mt != NULL);
    /* get mtask this uthread is going to execute */
    /* This copy has everything valid but the pointer to executed_it */
    mtask_t mt;
    memcpy(&mt, ut->mt, sizeof(mtask_t));

    uint32_t buf_size = 0;
    uint8_t buf[UTHREAD_MAX_RET_SIZE];

    switch (mt.type) {
    case MTASK_EXECUTE:
        {
          if(mt.ret_buf != NULL) 
            worker_do_execute(mt.func, mt.args, mt.args_bytes,
                buf, &buf_size, mt.handle);
          else
            worker_do_execute(mt.func, mt.args, mt.args_bytes,
                              NULL, NULL, mt.handle);

          if (buf_size > UTHREAD_MAX_RET_SIZE)
            ERRORMSG(" task writing out of bound in the return buffer"
                     " (see UTHREAD_MAX_RET_SIZE)\n");

          uint32_t rnid = uthread_get_node(mt.gpid);
          uint32_t pid = uthread_get_tid_from_gtid(mt.gpid, rnid);
          _assert(pid < NUM_UTHREADS_PER_WORKER * NUM_WORKERS);
          if (rnid == node_id) {
            if (mt.type == MTASK_EXECUTE && mt.ret_buf != NULL) {
              memcpy(mt.ret_buf, buf, buf_size);
              if (mt.ret_buf_size_ptr != NULL)
                *(mt.ret_buf_size_ptr) = buf_size;
            }
            if (mt.handle != GMT_HANDLE_NULL)
              mtm_handle_icr_mtasks_terminated(mt.handle, 1);
            else
              uthread_incr_terminated_mtasks(pid, mt.nest_lev);
          } else {
            helper_send_exec_completed(rnid, ut->wid, pid, mt.nest_lev,
                                       (uint64_t) mt.ret_buf, (uint64_t)
                                       mt.ret_buf_size_ptr, buf_size, buf, mt.handle);
          }
          /* push completed mtask in the pool */
          worker_push_mtask_pool(ut->wid, ut->mt);
        }
        break;
    case MTASK_FOR:
        {
            uint64_t its = MIN(mt.step_it, mt.end_it - start_it);
            //  GMT_DEBUG_PRINTF("start_it %lu end_it %lu step_it %u\n", 
            //  start_it, (uint64_t)mt.end_it, (uint64_t)mt.step_it);
            worker_do_for(mt.func, start_it, its, mt.args, mt.gmt_array,
                          mt.handle);
            uint64_t ret = __sync_add_and_fetch(&ut->mt->executed_it, its);
            if (ret == mt.end_it) {
                if (mt.handle != GMT_HANDLE_NULL)
                    mtm_handle_icr_mtasks_terminated(mt.handle, 1);
                else {
                    uint32_t rnid = uthread_get_node(mt.gpid);
                    uint32_t pid = uthread_get_tid_from_gtid(mt.gpid, rnid);
                    _assert(pid < NUM_UTHREADS_PER_WORKER * NUM_WORKERS);
                    if (rnid == node_id)
                        uthread_incr_terminated_mtasks(pid, mt.nest_lev);
                    else {
                        cmd_for_compl_t *cmd;
                        cmd = (cmd_for_compl_t *) agm_get_cmd(rnid, ut->wid,
                                                              sizeof
                                                              (cmd_for_compl_t),
                                                              0, NULL);
                        cmd->type = GMT_CMD_FOR_COMPL;
                        cmd->tid = pid;
                        cmd->nest_lev = mt.nest_lev;
                        agm_set_cmd_data(rnid, ut->wid, NULL, 0);
                    }

                }
                /* push completed mtask in the pool */
                worker_push_mtask_pool(ut->wid, ut->mt);
            }
        }
        break;

    case MTASK_GMT_MAIN:
        {
            _assert(mt.gpid == -1);
            realRet = gmt_main((int)start_it, (char **)mt.args);
            worker_exit();
        }
        break;

    default:
        ERRORMSG("ERROR mtask not recognized");
        break;
    }

    if (ut->nest_lev == 0) {
        /* put uthread back to pool */
        ut->tstatus = TASK_NOT_INIT;
        uthread_queue_push(&workers[ut->wid].uthread_pool, ut);
    }
}

/* Exit function called when a the master tasks completes or any task 
 * calls exit() - registered in main() with atexit(worker_exit()); 
 * This function sends the GMT_CMD_FINALIZE to all the nodes.
 */

void worker_exit()
{
    uint32_t tid = uthread_get_tid();
    uint32_t wid = uthread_get_wid(tid);

    /* we can't stop we we not yet started */
    while (workers_stop_flag) ;

#if !ENABLE_SINGLE_NODE_ONLY
    /* send command GMT_CMD_FINALIZE to each remote node */
    uint32_t i;
    for (i = 0; i < num_nodes; i++) {
        if (i != node_id) {
            cmd_gen_t *cmd =
                (cmd_gen_t *) agm_get_cmd_na(i, wid, sizeof(cmd_gen_t),
                                             0,
                                             NULL);
            cmd->type = GMT_CMD_FINALIZE;
            agm_set_cmd_data_na(i, wid, NULL, 0);
        }
    }

    if (num_nodes != 1) {
        /* make sure everything was sent for this worker, by acquiring (draining)
         * all the communication buffers. By definition when we are 
         * able to acquire NUM_BUFFS_PER_CHANNEL buffers it means everything 
         * there before was sent */
//         uint32_t count = 0;
//         while (count < NUM_BUFFS_PER_CHANNEL)
//             if (comm_server_drain_send_buff(wid) != NULL)
//                 count++;
        sleep(1);

    }
#endif
    /* stopping all workers on this node */
    worker_team_stop();

    /* push mtask that generated this task back in the pool */
    worker_push_mtask_pool(wid, uthreads[tid].mt);

    /* return to worker context */
    gmt_swapcontext(&uthreads[tid].ucontext, &workers[wid].worker_ctxt);
}
