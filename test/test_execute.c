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

#include "main.h"

void execute_body( const void * args, uint32_t arg_size, void * ret, uint32_t *ret_size, gmt_handle_t handle ){
    _unused(arg_size);
    _unused(handle);
    TEST(args != NULL);
    TEST(ret != NULL);

    //printf("[n %u] %s args %p ret %p\n",node_id,__func__, args, ret); 
    exec_args_t *exec_args = (exec_args_t *) args;
    TEST(exec_args->control_value == CONTROL_VALUE);
    *((uint64_t*)ret) = ((uint64_t)exec_args->garray + 
        (uint64_t)exec_args->offset +(uint64_t) exec_args->check_value);
    //  printf("iter %lu returning %lu\n", exec_args->iter,  *(uint64_t*)ret);
    *ret_size = sizeof(uint64_t);
}

void execute_body_args_null(const void *args, uint32_t arg_size, void * ret, uint32_t *retsize , gmt_handle_t handle){
    _unused(arg_size);
    _unused(handle);

    TEST(args == NULL);
    TEST(ret != NULL);
    //printf("[n %u] %s \n", node_id,__func__);
    *((uint64_t*)ret) = node_id;
    *retsize = sizeof(uint64_t);
}

void execute_body_on_all(const void *args, uint32_t arg_size, void * ret, uint32_t *retsize , gmt_handle_t handle){
    _unused(arg_size);
    _unused(handle);
    exec_args_t *exec_args = (exec_args_t *) args;
    TEST(ret == NULL);
    TEST(retsize == NULL);

    if( exec_args->preempt_policy == GMT_PREEMPTABLE)
      gmt_atomic_add(exec_args->garray, 0, 1);
}

void test_execute_on_all (uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
  _unused(num); _unused(handle);
  _unused(iter_id);
  arg_t *arg = ( arg_t* ) args;
  uint64_t non_blocking = arg->non_blocking;
  uint64_t j = 0, i = 0;
  exec_args_t exec_args;
  exec_args.garray = gmt_alloc(1, sizeof(uint64_t), GMT_ALLOC_LOCAL, NULL);
  exec_args.preempt_policy = arg->preempt_policy;
  gmt_put_value(exec_args.garray, 0, 0);
  elemsInfo_t info;
  gmt_get(arg->ginfo,node_id,&info, 1); 
  for (i = 0; i < info.nElemsPerTask ;i++) {
    /*GMT_DEBUG_PRINTF("%s doing iter %lu execute_bodynumber %lu on offset %lu\n", 
      __func__, iter_id, i,exec_args[i].offset);*/

    gmt_execute_on_all_nb( execute_body_on_all, &exec_args, sizeof(exec_args), arg->preempt_policy);

    if( j == non_blocking){
      gmt_wait_execute_nb();
      if(arg->check && arg->preempt_policy == GMT_PREEMPTABLE){
        uint64_t count = 0;
        gmt_get(exec_args.garray, 0, &count, 1);
        GMT_DEBUG_PRINTF("\ncounted %lu j %lu\n", count, j);
        TEST(count == gmt_num_nodes()*(j+1));
        gmt_put_value(exec_args.garray, 0, 0);
      }
      j = 0;
    }
    j++;

  }
  gmt_wait_execute_nb();
  gmt_free(exec_args.garray);
}

void test_execute ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    assert(args!=NULL);
    arg_t *arg = ( arg_t* ) args;
    uint64_t non_blocking = arg->non_blocking;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 


    uint64_t idx = iter_id % arg->num_iterations;
    uint64_t *offsets=TestUtils_getTaskOffsets(idx,&info);
    uint64_t check_value = node_id+info.gdata+TEST_EXECUTE;
    exec_args_t *exec_args =(exec_args_t*) malloc(sizeof(exec_args_t)*info.nElemsPerTask);
    // GMT_DEBUG_PRINTF("line %d nElemsPerTask %lu", __LINE__, info.nElemsPerTask);
	assert(exec_args != NULL);
    uint64_t *ret_value = (uint64_t *)malloc(sizeof(uint64_t)* info.nElemsPerTask);
    assert(ret_value != NULL);
    uint32_t ret_size;
    uint64_t j = 0, i = 0, c= 0;
    for (i = 0; i < info.nElemsPerTask ;i++) {
        exec_args[i].garray = info.gdata;
        exec_args[i].offset = offsets[i];
        exec_args[i].check_value = check_value;
        exec_args[i].elem_bytes = info.elem_bytes;
        exec_args[i].control_value = CONTROL_VALUE;
        exec_args[i].iter = i;

        /*GMT_DEBUG_PRINTF("%s doing iter %lu execute_bodynumber %lu on offset %lu\n", 
          __func__, iter_id, i,exec_args[i].offset);*/

        ret_size = sizeof(uint64_t);
        ret_value[i] = 9999;

        gmt_execute_on_data_nb( exec_args[i].garray, i,
              execute_body, &exec_args[i], sizeof(exec_args_t),
              &ret_value[i], &ret_size, arg->preempt_policy);
        if(j == non_blocking){
            gmt_wait_execute_nb();
            if(arg->check){
                for(c = i - j; c <= i; c++){
                    assert(c < info.nElemsPerTask);
                    TEST(ret_value[c] == exec_args[c].garray +
                                exec_args[c].offset+exec_args[c].check_value);
                    TEST(ret_size == sizeof(uint64_t));
                }
            }
            j = 0;
        }
        j++;

    }
    gmt_wait_execute_nb();
    for (i = 0; i < info.nElemsPerTask; i++) {
        TEST(ret_value[i] == exec_args[c].garray + exec_args[c].offset+exec_args[c].check_value);
    }
    /* test NULL args */
    uint64_t ret_buf;
    if(arg->check){
        gmt_execute_on_data( exec_args[0].garray, 0, execute_body_args_null, NULL, 0, 
                             &ret_buf, &ret_size, GMT_NON_PREEMPTABLE);
        assert(ret_size == sizeof(uint64_t));
    }

    free(exec_args);
    free(ret_value);
}


