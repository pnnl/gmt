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

void for_loop_body( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle){
    _unused(num); _unused(handle);
    nest_args_t *arg = ( nest_args_t* ) args;
    TEST(arg!= NULL);
    TEST(arg->control_value == CONTROL_VALUE);
    //GMT_DEBUG_PRINTF("%s iter_id %lu check %u\n", __func__, iter_id, arg->check);

    if( iter_id == 0){
        if(arg->check){
            uint64_t check_iter = arg->parent_iteration % arg->num_iterations;
            uint64_t check_value = arg->gdata+TEST_FOR_LOOP+check_iter;
            uint64_t offset = (check_iter);
            /*GMT_DEBUG_PRINTF("putvalue parent_it %lu gdata %u offset %lu\n", 
              arg->parent_iteration, arg->gdata, arg->parent_iteration*sizeof(uint64_t));*/
            gmt_put_value(arg->gdata,offset,
                    check_value);
            /*GMT_DEBUG_PRINTF("putvalue gdata %u offset %lu done\n", 
              arg->gdata, arg->parent_iteration*sizeof(uint64_t));*/
        }
    }
}


#define PARFOR_ITERATIONS 8 
void test_for_loop ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    //GMT_DEBUG_PRINTF("%s iter_id %lu\n", __func__, iter_id);

    uint64_t non_blocking = arg->non_blocking;
    nest_args_t nest_args;
    nest_args.check = arg->check;
    nest_args.gdata = info.gdata;
    nest_args.iters_per_nesting_level = info.nElemsPerTask;
    nest_args.parent_iteration = iter_id;
    nest_args.num_iterations = arg->num_iterations;
    nest_args.control_value = CONTROL_VALUE;

    uint32_t i = 0, j = 0;
    for(i = 0; i < nest_args.iters_per_nesting_level; i++){
        gmt_for_loop_nb(PARFOR_ITERATIONS,1, for_loop_body, &nest_args, 
                sizeof(nest_args_t),glob.spawn_policy);
        if( j == non_blocking){
            // printf("wait commands\n");
            gmt_wait_for_nb();
            j = 0;
        }
        j++;
    }
    gmt_wait_for_nb();


    if(arg->check){
        uint64_t value;
        uint64_t check_iter = iter_id%arg->num_iterations;
        uint64_t check_value = info.gdata+TEST_FOR_LOOP+check_iter;
        uint64_t offset = check_iter;
        /*GMT_DEBUG_PRINTF("gettig gdata %u offset %lu\n", 
          info.gdata, iter_id*sizeof(uint64_t));*/
        gmt_get(info.gdata, offset, &value, 1); 
        /*GMT_DEBUG_PRINTF("value at gdata %u offset %lu is %lu\n", 
          info.gdata, iter_id*sizeof(uint64_t),value);*/
        TEST(value == check_value);
    }

    /*TEST(verify_check_values(info.gdata,offsets,info.nElemsPerTask,
      check_value,info.elem_bytes));*/
}

