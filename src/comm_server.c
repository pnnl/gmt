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

#include "gmt/comm_server.h"
#include "gmt/profiling.h"
#include "gmt/aggregation.h"
#include <execinfo.h>
#include <sys/mman.h>

#if !(ENABLE_SINGLE_NODE_ONLY)
comm_server_t cs;

void comm_server_init_channel(channel_t * ch)
{
    ch_queue_init(&ch->queue);
    ch_queue_init(&ch->pool);
    ch->current_buff = NULL;
    ch->buffers = (net_buffer_t*)_malloc(sizeof(net_buffer_t) * NUM_BUFFS_PER_CHANNEL);
    uint32_t i;
    for (i = 0; i < NUM_BUFFS_PER_CHANNEL; i++) {
        /* Init the buffer with a pointer to its channel */
        netbuffer_init(&ch->buffers[i], i, (void *)ch);
        ch_queue_push(&ch->pool, &ch->buffers[i]);
    }
}

void comm_server_destroy_channel(channel_t * ch)
{
    ch_queue_destroy(&ch->queue);
    ch_queue_destroy(&ch->pool);
    uint32_t i;
    for (i = 0; i < NUM_BUFFS_PER_CHANNEL; i++)
        netbuffer_destroy(&ch->buffers[i]);
    free(ch->buffers);
}

void comm_server_init()
{
    uint32_t i;
    cs.send_channels = (channel_t*)_malloc(NUM_SEND_CHANNELS * sizeof(channel_t));
    for (i = 0; i < NUM_SEND_CHANNELS; i++)
        comm_server_init_channel(&cs.send_channels[i]);

    cs.recv_channels = (channel_t*)_malloc(NUM_RECV_CHANNELS * sizeof(channel_t));
    for (i = 0; i < NUM_RECV_CHANNELS; i++)
        comm_server_init_channel(&cs.recv_channels[i]);

    pend_queue_init(&cs.pending_send);
    pend_queue_init(&cs.pending_recv);
    cs.init_done = true;
}

void comm_server_destroy()
{
    uint32_t i;
    for (i = 0; i < NUM_SEND_CHANNELS; i++)
        comm_server_destroy_channel(&cs.send_channels[i]);
    for (i = 0; i < NUM_RECV_CHANNELS; i++)
        comm_server_destroy_channel(&cs.recv_channels[i]);
    free(cs.send_channels);
    free(cs.recv_channels);
    pend_queue_destroy(&cs.pending_send);
    pend_queue_destroy(&cs.pending_recv);
}

INLINE bool comm_server_do_send()
{
    /* send buffers in queue */
    uint32_t i;
    bool sent = false;
    for (i = 0; i < NUM_SEND_CHANNELS; i++) {
        net_buffer_t *buff;
        channel_t *send_channel = &cs.send_channels[i];
        if (ch_queue_pop(&send_channel->queue, &buff)) {
            _assert(buff->num_bytes != 0);
            DEBUG0(printf("n %d comm_server sending buffer id %u size %u on "
                          "channel %p\n", node_id, buff->id, buff->num_bytes,
                          send_channel););
            network_send_nb(buff);
            pend_queue_push(&cs.pending_send, buff);
            sent = true;
        }
    }
    return sent;
}

INLINE void comm_server_test_send()
{
    /* push completed buffers in pool */
    net_buffer_t *buff;
    if (pend_queue_pop(&cs.pending_send, &buff)) {
        if (network_test_send(buff)) {
            channel_t *send_channel = (channel_t *) buff->context;
            DEBUG0(printf("n %d comm server sent %u bytes on buffer id %u "
                          "channel %p\n", node_id, buff->num_bytes,
                          buff->id, send_channel););

            INCR_EVENT(COMM_SEND_BYTES, buff->num_bytes);
            ch_queue_push(&send_channel->pool, buff);
            COUNT_EVENT(COMM_SEND_COMPLETED);
        } else {                /* push back in queue */
            pend_queue_push(&cs.pending_send, buff);
        }
    }

}

INLINE bool comm_server_do_recv()
{
    /* post receive buffers in pool */
    uint32_t i;
    bool received = false;
    for (i = 0; i < NUM_RECV_CHANNELS; i++) {
        net_buffer_t *buff;
        channel_t *recv_channel = &cs.recv_channels[i];
        if (ch_queue_pop(&recv_channel->pool, &buff)) {
            _assert(buff != NULL);
            DEBUG0(printf("n %d comm_server post receive buffer id %u "
                          "on channel %p\n", node_id, buff->id, recv_channel););
            network_recv_nb(buff);
            pend_queue_push(&cs.pending_recv, buff);
            received = true;
        }
    }
    return received;
}

INLINE void comm_server_test_recv()
{
    net_buffer_t *buff;
    if (pend_queue_pop(&cs.pending_recv, &buff)) {
        if (network_test_recv(buff)) {
            _assert(buff->num_bytes > 0);
            channel_t *recv_channel = (channel_t *) buff->context;
            DEBUG0(printf("n %d comm server received %u bytes buffer id %u "
                          " on channel %p\n", node_id, buff->num_bytes,
                          buff->id, recv_channel););
            INCR_EVENT(COMM_RECV_BYTES, buff->num_bytes);
            ch_queue_push(&recv_channel->queue, buff);
            COUNT_EVENT(COMM_RECV_COMPLETED);
        } else {
            pend_queue_push(&cs.pending_recv, buff);
        }
    }
}

void *comm_server_loop(void *arg)
{
	/* initialize thread index for the MPMC queue */
	qmpmc_assign_tid();

    _unused(arg);
    if (config.thread_pinning) {
      pin_thread(config.num_cores - 1);
      if (node_id == 0) {
        DEBUG0(printf("pining CPU %u with pthread_id %u\n",
              config.num_cores - 1, get_thread_id()););
      }
    }
    network_barrier();
    while (!cs.init_done) ;

    bool sent = false;
    bool received = false;
    DEBUG0(uint64_t loops = 0;);
    /* When cs.stop_flag is true continue until there are no more send or recv */
    while (!cs.stop_flag || sent || received) {
        DEBUG0(if (loops++ % DEBUG_PRINT_INTERVAL == 0)
               printf
               ("n %d comm server alive stop_flag:%u sent:%u received:%u\n",
                node_id, cs.stop_flag, sent, received);) ;

        comm_server_test_send();
        comm_server_test_recv();
        sent = comm_server_do_send();
        received = comm_server_do_recv();
    }
    DEBUG0(printf("n %d communication server loop completed\n", node_id););    
    pthread_exit(NULL);
}

void comm_server_run()
{
    cs.stop_flag = false;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    /* set stack address for this worker */
    void *stack_addr =
        (void *)((uint64_t)pt_stacks + (NUM_WORKERS + NUM_HELPERS) * PTHREAD_STACK_SIZE);

    int ret = pthread_attr_setstack(&attr, stack_addr, PTHREAD_STACK_SIZE);
    if (ret)
        perror("FAILED TO SET STACK PROPERTIES"), exit(EXIT_FAILURE);

    ret =
        pthread_create(&cs.pthread, &attr, &comm_server_loop, NULL);
    if (ret)
        perror("FAILED TO CREATE COMM SERVER"), exit(EXIT_FAILURE);
}

void comm_server_stop()
{
    /* we can't stop if we not yet started */
    while (cs.stop_flag) ;

    cs.stop_flag = true;
    pthread_join(cs.pthread, NULL);

    network_barrier();
}
#endif
