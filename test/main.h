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

#ifndef _MAIN_H_
#define _MAIN_H_

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <string.h>

#include "gmt/gmt.h"
#include "gmt/timing.h"
#include "testUtils.h"

#ifdef DISABLE_SWAPCONTEXT
#error "DISABLE_SWAPCONTEXT is defined in libgmt/config.h"
#endif

#define MAX_FUN_STR 128
#define MAX_DATASET (8ul*1024*1024*1024)
#define TEST_FAILED false
#define TEST_SUCCESS true

#define SEND_NODE 0
#define RCV_NODE 1

#define BYTE_VALUE 0xab
#define BLOCK_VALUES_SIZE (16*1024)
#define CONTROL_VALUE 0xaaaaaaaaUL
static uint8_t block_values[BLOCK_VALUES_SIZE];

typedef enum {
    NO_VALUE,
    TEST_ALLOC,
    TEST_GET,
    TEST_GET_REPLICA,
    TEST_LOCAL_PTR,
    TEST_PUT,
    TEST_PUTVALUE,
    TEST_ATOMIC_ADD,
    TEST_ATOMIC_CAS,
    TEST_EXECUTE,
    TEST_EXECUTE_WITH_HANDLE,
    TEST_EXECUTE_ON_ALL,
    TEST_EXECUTE_ON_ALL_WITH_HANDLE,
    TEST_FOR_LOOP_WHANDLE,
    TEST_SPAWN_AT,
    TEST_FOR_LOOP_WHANDLE_NESTED,
    TEST_FOR_LOOP,
    TEST_FOR_LOOP_NESTED,
    TEST_YIELD,
    TEST_MEMCPY,
    TEST_FILE_WRITE,
    TEST_FOR_EACH,
    TEST_EXECUTE_ON_NODE,
    TEST_ALL
} test_type_t;

typedef struct glob_tag { /* global variables */
    char current_fun[MAX_FUN_STR];
    double end_time;
    char *prog_name;
    double tot_bytes;
    test_type_t test_num;
    uint64_t num_iterations;
    uint64_t num_oper;
    uint64_t elem_bytes;
    bool_t random_elems;
    bool_t random_offsets;
    /* follows task arguments */
    uint64_t non_blocking;
    bool_t check;
    bool_t zero_flag;
    alloc_type_t   alloc_type;
    spawn_policy_t spawn_policy;
    preempt_policy_t preempt_policy;
    gmt_data_t ginfo; // structure elemsInfo_t
    gmt_data_t glocal;
} glob_t;

static glob_t glob;


typedef struct arg_tag {
    bool_t check;
    bool_t zero_flag;
    uint64_t non_blocking;
    uint64_t num_iterations;
    uint64_t num_oper;
    uint64_t elem_bytes;
    alloc_type_t alloc_type;
    spawn_policy_t spawn_policy;
    preempt_policy_t preempt_policy;
    gmt_data_t ginfo;
    uint64_t control_value;
    uint64_t num_elems;
    uint64_t allocated_elements;
    gmt_data_t glocal;
} arg_t;

typedef struct exec_args_tag{
    gmt_data_t garray;
    preempt_policy_t preempt_policy;
    uint64_t offset;
    uint64_t elem_bytes;
    uint64_t control_value;
    uint64_t check_value;
    uint64_t iter;
    uint32_t nodeid;
    uint64_t total_tasks;
    gmt_handle_t handle;
}exec_args_t;

typedef struct nest_args_tag{
    bool_t check;
    gmt_data_t gdata;
    uint64_t iters_per_nesting_level;
    uint64_t num_iterations;
    uint64_t parent_iteration;
    uint64_t control_value;
    spawn_policy_t spawn_policy;
}nest_args_t;



#define DO_TEST(TEST_FUN, ARG, ARG_SIZE) {				\
    get_fun_str(#TEST_FUN);							\
    do_test(TEST_FUN, ARG, ARG_SIZE);						\
}										\


#define TEST(CONDITION) {                						\
    if (!(CONDITION)) { 								\
        printf("\n============================================================");	\
        printf("\n!!! TEST FAILED !!! node %u %s at %s line %d\n\n",				\
                node_id,glob.current_fun,__FILE__,__LINE__); 					\
        printf("\n============================================================\n");	\
        exit(0);									\
    }											\
}


void test_file_write ( uint64_t iter_id, void * args);
void test_for_loop ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_for_each ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_for_loop_nested ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_for_loop_whandle ( uint64_t iter_id, uint64_t num,const void * args, gmt_handle_t handle);
void test_spawn_at ( uint64_t iter_id, void * args);
void test_for_loop_whandle_nested ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_execute ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_execute_with_handle ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_execute_on_node ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_execute_on_all ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_execute_on_all_with_handle ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_atomic_cas ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_atomic_add ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_put ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_putvalue ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_get ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_get_replica ( uint64_t iter_id, void * args);
void test_local_ptr ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle );
void test_yield ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);
void test_alloc ( uint64_t it, uint64_t num, const void *args, gmt_handle_t handle);
void test_memcpy ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle);

#endif

