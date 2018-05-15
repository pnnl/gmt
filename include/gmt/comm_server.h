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

#ifndef __COMM_SERVER_H__
#define __COMM_SERVER_H__

#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/queue.h"
#include "gmt/network.h"
#include "gmt/queue.h"

#if !(ENABLE_SINGLE_NODE_ONLY)

#define NUM_RECV_CHANNELS (NUM_HELPERS)
#define NUM_SEND_CHANNELS (NUM_WORKERS + NUM_HELPERS)

DEFINE_QUEUE(pend_queue, net_buffer_t *,
             NUM_BUFFS_PER_CHANNEL * NUM_SEND_CHANNELS);
DEFINE_QUEUE_SPSC(ch_queue, net_buffer_t *, NUM_BUFFS_PER_CHANNEL);

typedef struct channel_tag {
    ch_queue_t queue;
    ch_queue_t pool;
    net_buffer_t *buffers;
    net_buffer_t *current_buff;
} channel_t;

typedef enum {
    SEND_TYPE,
    RECV_TYPE
} chan_type_t;

typedef struct comm_server_t {
    volatile bool stop_flag;
    volatile bool init_done;
    pthread_t pthread;
    channel_t *send_channels;
    channel_t *recv_channels;
    pend_queue_t pending_send;
    pend_queue_t pending_recv;
} comm_server_t;

void comm_server_init();
void comm_server_run();
void comm_server_stop();
void comm_server_destroy();
channel_t *comm_server_get_channel(chan_type_t type);

extern comm_server_t cs;

INLINE net_buffer_t *comm_server_pop_send_buff(uint32_t tid)
{
    channel_t *ch = &(cs.send_channels[tid]);
    if (ch->current_buff == NULL)
        ch_queue_pop(&ch->pool, &ch->current_buff);
    return ch->current_buff;
}

INLINE net_buffer_t *comm_server_drain_send_buff(uint32_t tid)
{
    channel_t *ch = &(cs.send_channels[tid]);
    net_buffer_t *nb = NULL;
    ch_queue_pop(&ch->pool, &nb);
    return nb;
}

INLINE void comm_server_push_send_buff(uint32_t tid)
{
    channel_t *ch = &(cs.send_channels[tid]);
    ch_queue_push(&ch->queue, ch->current_buff);
    ch->current_buff = NULL;
}

INLINE net_buffer_t *comm_server_pop_recv_buff(uint32_t tid)
{
    channel_t *ch = &(cs.recv_channels[tid]);
    if (ch->current_buff == NULL)
        ch_queue_pop(&ch->queue, &ch->current_buff);
    return ch->current_buff;
}

INLINE void comm_server_push_recv_buff(uint32_t tid)
{
    channel_t *ch = &(cs.recv_channels[tid]);
    ch_queue_push(&ch->pool, ch->current_buff);
    ch->current_buff = NULL;
}

#endif

#endif                          /* __COMM_SERVER_H__ */

