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

#ifndef __COMMANDS_H__
#define __COMMANDS_H__

#include <stdio.h>
#include <stdint.h>
#include "gmt/config.h"
#include "gmt/gmt.h"
#include "gmt/utils.h"

#define PACKED_STR __attribute__ ((__packed__))
//#define PACKED_STR

#define PRINT_STR_SIZE(x) printf("%s size %ld\n",#x, sizeof(x) );


#define ARGS_SIZE_BITS                          20
#define TID_BITS                                20
#define NESTING_BITS                            5
#define CMD_TYPE_BITS                           5
#define VIRT_ADDR_PTR_BITS                      48
#define ITER_BITS                               48

#define GMT_CMD_ALLOC                           0
#define GMT_CMD_FREE                            1
#define GMT_CMD_ATOMIC_ADD                      2
#define GMT_CMD_ATOMIC_CAS                      3
#define GMT_CMD_PUT                             4
#define GMT_CMD_GET                             5
#define GMT_CMD_PUT_VALUE                       6

#define GMT_CMD_FOR                             7
#define GMT_CMD_EXEC_PREEMPT                    8
#define GMT_CMD_EXEC_NON_PREEMPT                9
#define GMT_CMD_FOR_COMPL                       10
#define GMT_CMD_EXEC_COMPL                      11

#define GMT_CMD_HANDLE_CHECK_TERM               12
#define GMT_CMD_HANDLE_CHECK_CREAT              13
#define GMT_CMD_HANDLE_RESET                    14

#define GMT_CMD_FINALIZE                        15
#define GMT_CMD_REPLY_ACK                       16
#define GMT_CMD_REPLY_GET                       17
#define GMT_CMD_REPLY_VALUE                     18
#define GMT_CMD_MTASKS_RES_REQ                  19
#define GMT_CMD_MTASKS_RES_REPLY                20

#define GMT_MAX_CMD_NUM                         20

typedef uint8_t cmd_type_t;


/* generic command type used for 
   commands that do not carry info, used by
   GMT_CMD_FINALIZE and GMT_CMD_MTASKS_RES_REQ */
typedef struct cmd_gen_t {
  cmd_type_t type:CMD_TYPE_BITS;
} cmd_gen_t;

/* generic command used for commands 
 * that need to send only a 32-bit or 64-bit values 
 * used by  GMT_CMD_MTASKS_RES_REPLY(64-bit), 
 * GMT_CMD_REPLY_ACK(32-bit) and 
 */
typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t value;
} cmd32_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint64_t value;
} cmd64_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
  alloc_type_t alloc_type;
  uint64_t num_elems;
  uint64_t bytes_per_elem;
  uint16_t name_len;
} cmd_alloc_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
} cmd_free_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
  uint64_t offset;
  uint64_t put_bytes;
} cmd_put_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
  uint64_t offset;
  uint64_t value;
} cmd_put_value_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
  uint64_t offset;
  uint64_t ret_value_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t old_value;
  uint64_t new_value;
} cmd_atomic_cas_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
  uint64_t offset;
  uint64_t ret_value_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t value;
} cmd_atomic_add_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  gmt_data_t gmt_array;
  uint64_t offset;
  uint64_t ret_data_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t get_bytes;
} cmd_get_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  uint64_t ret_value_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t value;
} cmd_rep_value_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  uint64_t ret_data_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t get_bytes;
} cmd_rep_get_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t pid:TID_BITS;
  uint8_t nest_lev:NESTING_BITS;
  uint64_t func_ptr:VIRT_ADDR_PTR_BITS;
  uint32_t args_bytes:ARGS_SIZE_BITS;
  uint64_t ret_buf_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t ret_size_ptr:VIRT_ADDR_PTR_BITS;
  gmt_handle_t handle;
} cmd_exec_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t pid:TID_BITS;
  uint8_t nest_lev:NESTING_BITS;
  uint64_t ret_buf_ptr:VIRT_ADDR_PTR_BITS;
  uint64_t ret_size_ptr:VIRT_ADDR_PTR_BITS;
  uint32_t ret_size_value;
  gmt_handle_t handle;
} cmd_exec_compl_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t pid:TID_BITS;
  uint8_t nest_lev:NESTING_BITS;
  uint64_t func_ptr:VIRT_ADDR_PTR_BITS;    
  uint32_t args_bytes:ARGS_SIZE_BITS;
  uint64_t it_start:ITER_BITS;
  uint64_t it_end:ITER_BITS;
  uint32_t it_per_task;
  gmt_data_t gmt_array;
  gmt_handle_t handle;
} cmd_for_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  uint32_t tid:TID_BITS;
  uint8_t nest_lev:NESTING_BITS;
} cmd_for_compl_t;

typedef struct PACKED_STR {
  cmd_type_t type:CMD_TYPE_BITS;
  gmt_handle_t handle;
  uint32_t mtasks_created;
  uint32_t mtasks_terminated;
  uint32_t node_counter;
} cmd_check_handle_t;





// INLINE set_cmd_for(cmd_for_t * c, uint32_t pid, uint8_t nest_lev,
//                    void *func, uint32_t args_bytes,
//                    uint64_t it_start, uint64_t it_end, uint32_t it_per_task)
// {
//     c->func_ptr = (uint64_t) func;
//     c->args_bytes = args_bytes;
//     c->it_start = it_start;
//     c->it_end = it_end;
//     c->it_per_task = it_per_task;
//     c->pid = pid;
//     c->nest_lev = nest_lev;
//     c->type = GMT_CMD_FOR;
// }
// 
// INLINE set_cmd_for_h(cmd_for_h_t * c, uint32_t pid, uint8_t nest_lev,
//                    void *func, uint32_t args_bytes,
//                    uint64_t it_start, uint64_t it_end, uint32_t it_per_task,
//                    gmt_handle_t handle )
// {
//     c->func_ptr = (uint64_t) func;
//     c->args_bytes = args_bytes;
//     c->it_start = it_start;
//     c->it_end = it_end;
//     c->it_per_task = it_per_task;
//     c->gpid = pid;
//     c->nest_lev = nest_lev;
//     c->handle = handle;
//     c->type = GMT_CMD_FOR_H;
// }
//

INLINE uint64_t commands_max_cmd_size()
{

  uint64_t sizes[GMT_MAX_CMD_NUM] = {
    sizeof(cmd_gen_t),
    sizeof(cmd32_t),
    sizeof(cmd64_t),
    sizeof(cmd_alloc_t),
    sizeof(cmd_free_t),
    sizeof(cmd_put_t),
    sizeof(cmd_put_value_t),
    sizeof(cmd_atomic_cas_t),
    sizeof(cmd_atomic_add_t),
    sizeof(cmd_get_t),
    sizeof(cmd_rep_value_t),
    sizeof(cmd_rep_get_t),
    sizeof(cmd_exec_t),
    sizeof(cmd_exec_compl_t),
    sizeof(cmd_for_t),
    sizeof(cmd_for_compl_t),
    sizeof(cmd_check_handle_t)};

  uint64_t i;
  uint64_t max = 0;
  for( i = 0; i < GMT_MAX_CMD_NUM; i++ )
    if (sizes[i] > max )
      max = sizes[i];

  return max;
}
#endif
