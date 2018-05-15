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

void spawn_at_body( uint64_t iter_id, void * args, gmt_spawn_t handler){

    _unused(handler);
    nest_args_t *arg = ( nest_args_t* ) args;
    /*GMT_DEBUG_PRINTF("%s iter_id %lu check %u handler %u\n", __func__, iter_id, 
            arg->check, (uint32_t) handler);*/

    if(arg->check){
        TEST(arg->control_value == CONTROL_VALUE);
        if( iter_id == 0){
            uint64_t check_iter = arg->parent_iteration % arg->num_iterations;
            uint64_t check_value = arg->gdata+TEST_SPAWN+check_iter;
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


#define SPAWN_ITERATIONS 8
void test_spawn_at ( uint64_t iter_id, void * args ) {

    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info); 

    nest_args_t nest_args;
    nest_args.check = arg->check;
    nest_args.gdata = info.gdata;
    nest_args.iters_per_nesting_level = info.nElemsPerTask;
    nest_args.parent_iteration = iter_id;
    nest_args.num_iterations = arg->num_iterations;
    nest_args.control_value = CONTROL_VALUE;
    nest_args.spawn_policy = arg->spawn_policy;

    gmt_spawn_t handler = gmt_get_spawn_handler();
    //GMT_DEBUG_PRINTF(" got handler %u\n", handler);
    uint32_t i;
    for(i = 0; i < nest_args.iters_per_nesting_level; i++){
        // printf("[n %u ] %s doing loop %i\n",node_id, __func__,i);
        uint64_t offset = (iter_id % arg->num_iterations) * sizeof(uint64_t);

//TODO: gmt_spawn_at allows to define how many tasks to be spawned by setting num_it and step_it
//gmt_execute_on_data_with_handle creates and executes only 1 task. what's the equivalent of gmt_spawn_at in gmt 2.0?
//

/*            gmt_execute_on_data_with_handle(info.gdata, offset, spawn_at_body, &nest_args, SPAWN_ITERATIONS,1, spawn_at_body, &nest_args,  sizeof(nest_args),
                    info.gdata, offset, handler);*/

    }

    gmt_wait_spawn(handler);

    if(arg->check){
        uint64_t value;
        uint64_t check_iter = iter_id%arg->num_iterations;
        uint64_t check_value = info.gdata+TEST_SPAWN+check_iter;
        uint64_t offset = check_iter;
        /*GMT_DEBUG_PRINTF("gettig gdata %u offset %lu\n", 
          info.gdata, iter_id*sizeof(uint64_t));*/
        gmt_get(info.gdata, offset, &value); 
        /*GMT_DEBUG_PRINTF("value at gdata %u offset %lu is %lu\n", 
          info.gdata, iter_id*sizeof(uint64_t),value);*/
        TEST(value == check_value);
    }

    /*TEST(verify_check_values(info.gdata,offsets,info.nElemsPerTask,
      check_value,info.elem_bytes));*/
}
