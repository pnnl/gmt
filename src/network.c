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

#include "gmt/network.h"

#if !ENABLE_SINGLE_NODE_ONLY

/************* net_buffer_t ***************************/
void netbuffer_init(net_buffer_t * buff, uint32_t id, void *context)
{
    memset(buff, 0, sizeof(net_buffer_t));    
    buff->data = (uint8_t *)_malloc(COMM_BUFFER_SIZE);
    memset(buff->data, COMM_BUFFER_SIZE, sizeof(uint8_t));    
    buff->num_bytes = 0;
    buff->id = id;
    buff->context = context;
}


void netbuffer_destroy(net_buffer_t * buff)
{    
    free(buff->data);
}

/****************** Network ***************/
void network_init(int *argc, char ***argv)
{
    int ret = MPI_Init(argc, argv);
    if (ret != MPI_SUCCESS)
        ERRORMSG("Error initializing MPI\n");
    MPI_Comm_rank(MPI_COMM_WORLD, (int *)&node_id);
    MPI_Comm_size(MPI_COMM_WORLD, (int *)&num_nodes);
}

void network_finalize()
{
    MPI_Finalize();
}

void network_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD);
}

#endif
