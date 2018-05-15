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


void local_ptr_body( const void * args, uint32_t arg_size, void * ret, uint32_t *ret_size, gmt_handle_t handle ){
    _unused(handle);
    _unused(ret);
    _unused(ret_size);
    _unused(arg_size);
    TEST(args != NULL);
    exec_args_t *exec_args = (exec_args_t *) args;
    TEST(exec_args->control_value == CONTROL_VALUE);
    void * local_ptr = gmt_get_local_ptr(exec_args->garray, exec_args->iter);
    TEST(local_ptr != NULL);
    uint64_t value = *(uint64_t*)local_ptr;
    TEST(value == exec_args->check_value);
    TEST(local_ptr != NULL);
    /* TODO test if returns NULL for other elements*/
}

void test_local_ptr ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    assert(args!=NULL);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 


    uint64_t idx = iter_id % arg->num_iterations;
    uint64_t *offsets=TestUtils_getTaskOffsets(idx,&info);
    uint64_t check_value = node_id+info.gdata+TEST_EXECUTE;
    exec_args_t *exec_args =(exec_args_t*) malloc(sizeof(exec_args_t)*info.nElemsPerTask);
    assert(exec_args != NULL);
    // GMT_DEBUG_PRINTF("line %d nElemsPerTask %lu", __LINE__, info.nElemsPerTask);
    uint64_t i = 0;

    for (i = 0; i < info.nElemsPerTask ;i++) {
      gmt_put_value_nb(info.gdata, i, check_value + i);
    }
    gmt_wait_data();

    for (i = 0; i < info.nElemsPerTask ;i++) {
        exec_args[i].garray = info.gdata;
        exec_args[i].offset = offsets[i];
        exec_args[i].check_value = check_value + i;
        exec_args[i].elem_bytes = info.elem_bytes;
        exec_args[i].control_value = CONTROL_VALUE;
        exec_args[i].iter = i;
        exec_args[i].total_tasks = info.nElemsPerTask;

        /*GMT_DEBUG_PRINTF("%s doing iter %lu execute_bodynumber %lu on offset %lu\n", 
          __func__, iter_id, i,exec_args[i].offset);*/
        gmt_execute_on_data_nb( exec_args[i].garray, i,
              local_ptr_body, &exec_args[i], sizeof(exec_args_t),
              NULL, NULL, arg->preempt_policy);
    }
    gmt_wait_execute_nb();
    free(exec_args);
}

