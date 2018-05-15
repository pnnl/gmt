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

#ifndef __GMT_CONFIG_H__
#define __GMT_CONFIG_H__

/* define size in bytes of the communication buffers used on the network */
//#define COMM_BUFFER_SIZE  (256*1024)

/* define number of workers */
//#define NUM_WORKERS   (15)

/* define number of helpers */
//#define NUM_HELPERS   (15)

/* max level of nesting a uthread can self-execute */
//#define MAX_NESTING 64

/* max uthreads per worker */
//#define NUM_UTHREADS_PER_WORKER  (4) //(1024)

/* number of command blocks that can be used to send message to a remote node */
//#define NUM_CMD_BLOCKS  (128)

/* max number of mtasks per node (see mtask.h for description) */
//#define MAX_MTASKS_PER_THREAD   (512*1024)

/* size of the mtasks each node will try to reserve on a remote node */
//#define MTASKS_RES_BLOCK  (1024)

/* number of buffers per channel, a channel can be used only to send or to
   recv. A worker has 1 send channel while a helper has both a send and recv 
   channel */
//#define NUM_BUFFS_PER_CHANNEL  (64)

/* size in bytes of each command block */
//#define CMD_BLOCK_SIZE  (4096)

/* stack size for both workers helpers */
#define PTHREAD_STACK_SIZE ( 1 << 22)

/* entry address of the stack area for the uthreads */
#define UTHREADS_STACK_ENTRY_ADDRESS (0x000080000000000)

/* initial size of the stack of a uthread */
#define UTHREAD_INITIAL_STACK_SIZE  (1l << 15)  //32KB

/* maximum size of the stack of a uthread */
#define UTHREAD_MAX_STACK_SIZE   (1l << 20)

/* define max size of a return buffer for a uthread */
#define UTHREAD_MAX_RET_SIZE   (2048)

/* maximum number of gmt_alloc a node can do */
#define GMT_MAX_ALLOC_PER_NODE  (64*1024)

#endif                          /* __GMT_CONFIG_H__ */
