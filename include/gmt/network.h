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

#ifndef __NETWORK_H__
#define __NETWORK_H__

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "gmt/config.h"
#include "gmt/debug.h"

extern uint32_t node_id;
extern uint32_t num_nodes;

#if  !ENABLE_SINGLE_NODE_ONLY

/* Workaround for compilers that complains for openmpi */
/* Test for GCC > 4.6.0 */
#if __GNUC__ > 4 || \
	(__GNUC__ == 4 && (__GNUC_MINOR__ > 6 || \
			   (__GNUC_MINOR__ == 6 && \
			    __GNUC_PATCHLEVEL__ > 0)))
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <mpi.h>
#pragma GCC diagnostic pop
#else
#include <mpi.h>
#endif
/* end of workaround */

/************* net_buffer_t ***************************/
typedef struct net_buffer_t {
    uint32_t id;
    void *context;              /* used to identify the channel */
    uint32_t rnode_id;
    uint32_t num_bytes;
    uint8_t * data;
    MPI_Request request;
} net_buffer_t;

INLINE void netbuffer_append(net_buffer_t * buff, const void *ptr,
                                      uint32_t size)
{
    _assert(buff->num_bytes + size <= COMM_BUFFER_SIZE);
    memcpy((uint8_t *) & (buff->data[buff->num_bytes]), ptr, size);
    buff->num_bytes += size;
}

INLINE void netbuffer_skip(net_buffer_t * buff,
                                      uint32_t size)
{
    _assert(buff->num_bytes + size <= COMM_BUFFER_SIZE);    
    buff->num_bytes += size;
}


void netbuffer_init(net_buffer_t * buff, uint32_t id, void *context);
void netbuffer_destroy(net_buffer_t * buff);

/****************** Network ***************/

void network_init(int *argc, char ***argv);
void network_finalize();
void network_barrier();

INLINE void network_send_nb(net_buffer_t * buff)
{
    _assert(buff != NULL);
    _assert(buff->rnode_id < num_nodes);
    _assert(buff->num_bytes != 0);
    int res = MPI_Isend(buff->data, buff->num_bytes, MPI_BYTE, buff->rnode_id,
                        /*tag */ 1, MPI_COMM_WORLD, &buff->request);
    _unused(res);
    _assert(res == MPI_SUCCESS);

}

INLINE void network_recv_nb(net_buffer_t * buff)
{
    _assert(buff != NULL);
    int res = MPI_Irecv(buff->data, COMM_BUFFER_SIZE, MPI_BYTE, MPI_ANY_SOURCE,
                        /* tag */ 1, MPI_COMM_WORLD, &buff->request);
    _unused(res);
    _assert(res == MPI_SUCCESS);
}

INLINE void network_recv(net_buffer_t * buff)
{
    _assert(buff != NULL);
    MPI_Status status;
    int res = MPI_Recv(buff->data, COMM_BUFFER_SIZE, MPI_BYTE, MPI_ANY_SOURCE,
                       /* tag */ 1, MPI_COMM_WORLD, &status);
    _unused(res);
    _assert(res == MPI_SUCCESS);
}

INLINE bool network_test_recv(net_buffer_t * buff)
{
    _assert(buff != NULL);
    int flag = 0;
    MPI_Status status;
    int res = MPI_Test(&buff->request, &flag, &status);
    _unused(res);
    _assert(res == MPI_SUCCESS);

    if (flag) {
        int count;
        int res = MPI_Get_count(&status, MPI_BYTE, &count);
        _unused(res);
        _assert(res == MPI_SUCCESS);
        buff->num_bytes = count;
        buff->rnode_id = status.MPI_SOURCE;
        return true;
    }
    return false;
}

INLINE bool network_test_send(net_buffer_t * buff)
{
    _assert(buff != NULL);
    int flag = 0;
    MPI_Status status;
    int res = MPI_Test(&buff->request, &flag, &status);
    _unused(res);
    _assert(res == MPI_SUCCESS);
    return (flag == 0) ? false : true;
}

#endif

#endif /* __NETWORK_H__ */
