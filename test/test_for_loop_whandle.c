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

void for_loop_whandle_body( uint64_t iter_id, uint64_t num_it, const void * args, gmt_handle_t handler){

    _unused(handler);
    _unused(num_it);
    nest_args_t *arg = ( nest_args_t* ) args;
    /*GMT_DEBUG_PRINTF("%s iter_id %lu check %u handler %u\n", __func__, iter_id, 
            arg->check, (uint32_t) handler);*/

    if(arg->check){
        TEST(arg->control_value == CONTROL_VALUE);
        if( iter_id == 0){
            uint64_t check_iter = arg->parent_iteration % arg->num_iterations;
            uint64_t check_value = TEST_FOR_LOOP_WHANDLE+check_iter;
            uint64_t offset = (check_iter);
        //    GMT_DEBUG_PRINTF("putvalue parent_it %lu gdata %lu offset %lu and check_value %lu\n", 
        //      arg->parent_iteration, arg->gdata, arg->parent_iteration, check_value);
            gmt_put_value(arg->gdata,offset,
                    check_value);
            /*GMT_DEBUG_PRINTF("putvalue gdata %u offset %lu done\n", 
              arg->gdata, arg->parent_iteration*sizeof(uint64_t));*/
        }
    }
}

void for_loop_whandle_nested_body ( uint64_t iter_id, uint64_t num_it,const void * args, gmt_handle_t handler){
    _unused(num_it);
    nest_args_t *arg = ( nest_args_t* ) args;

    /*GMT_DEBUG_PRINTF("%s iters_per_nesting_level %lu iter %lu handler %u\n", __func__, 
            arg->iters_per_nesting_level,iter_id, handler);*/
    /* this task is used only to avoid the local execution in case of 1 iteration*/
    if(arg->iters_per_nesting_level == 1){ 
        if(arg->check){
            TEST(arg->control_value == CONTROL_VALUE);
            if(iter_id ==0 ){
                uint64_t check_iter = arg->parent_iteration % arg->num_iterations;
                uint64_t check_value = arg->gdata+TEST_FOR_LOOP_WHANDLE_NESTED+check_iter;
                uint64_t offset = (check_iter);
                /*GMT_DEBUG_PRINTF(`"putvalue parent_it %lu gdata %u offset %lu\n", 
                  arg->parent_iteration, arg->gdata, arg->parent_iteration*sizeof(uint64_t));*/
                gmt_put_value(arg->gdata,offset,
                        check_value);
                /*GMT_DEBUG_PRINTF("putvalue gdata %u offset %lu done\n", 
                  arg->gdata, arg->parent_iteration*sizeof(uint64_t));*/
            }
        }
    }else{
        assert( arg->iters_per_nesting_level % 2 == 0);
        nest_args_t arg2;
        memcpy(&arg2, args,sizeof(nest_args_t));
        arg2.iters_per_nesting_level = arg->iters_per_nesting_level / 2;
        gmt_for_loop_with_handle(arg2.iters_per_nesting_level,1, for_loop_whandle_nested_body, &arg2, 
                sizeof(nest_args_t),arg->spawn_policy, handler);
    }
}

void for_loop_whandle_nested_body_old ( uint64_t iter_id, uint64_t num_it, const void * args, gmt_handle_t handler){
    _unused(num_it);
    nest_args_t *arg = ( nest_args_t* ) args;
    //GMT_DEBUG_PRINTF("%s iter %lu handler %u\n", __func__, iter_id, (uint32_t) handler);

    if(iter_id == 0){
        /* this task is used only to avoid the local execution in case of 1 iteration*/
    }else{
        if( arg->iters_per_nesting_level > 0){
            nest_args_t arg2;
            memcpy(&arg2, args,sizeof(nest_args_t));
            arg2.iters_per_nesting_level -= 1;
            gmt_for_loop_with_handle(2,1, for_loop_whandle_nested_body, &arg2, 
                    sizeof(nest_args_t),GMT_SPAWN_REMOTE, handler);
        }else{
            if(arg->check){
                TEST(arg->control_value == CONTROL_VALUE);
                uint64_t check_iter = arg->parent_iteration % arg->num_iterations;
                uint64_t check_value = arg->gdata+TEST_FOR_LOOP_WHANDLE_NESTED+check_iter;
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
}

#define SPAWN_ITERATIONS 8 
void test_for_loop_whandle ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    nest_args_t nest_args;
    nest_args.check = arg->check;
    nest_args.gdata = info.gdata;
    nest_args.iters_per_nesting_level = info.nElemsPerTask;
    nest_args.parent_iteration = iter_id;
    nest_args.num_iterations = arg->num_iterations;
    nest_args.control_value = CONTROL_VALUE;
    nest_args.spawn_policy = arg->spawn_policy;

    gmt_handle_t handler = gmt_get_handle();
    //GMT_DEBUG_PRINTF(" got handler %u\n", handler);
    uint32_t i = 0;
    for(i = 0; i < nest_args.iters_per_nesting_level; i++){
       // printf("[n %u ] %s doing loop %i\n",node_id, __func__,i);
        gmt_for_loop_with_handle(SPAWN_ITERATIONS,1, for_loop_whandle_body, &nest_args, 
                sizeof(nest_args_t),arg->spawn_policy, handler);

    }

    gmt_wait_handle(handler);

    if(arg->check){
        uint64_t value;
        uint64_t check_iter = iter_id%arg->num_iterations;
        uint64_t check_value = TEST_FOR_LOOP_WHANDLE+check_iter;
        uint64_t offset = check_iter;
        /*GMT_DEBUG_PRINTF("gettig gdata %u offset %lu\n", 
          info.gdata, iter_id*sizeof(uint64_t));*/
        gmt_get(info.gdata, offset, &value, 1); 
        //GMT_DEBUG_PRINTF("value at gdata %lu offset %lu is %lu check %lu\n", 
        //  info.gdata, iter_id,value, check_value);
        TEST(value == check_value);
    }

    /*TEST(verify_check_values(info.gdata,offsets,info.nElemsPerTask,
      check_value,info.elem_bytes));*/
}

void test_for_loop_whandle_nested ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    nest_args_t nest_args;
    nest_args.check = arg->check;
    nest_args.gdata = info.gdata;
    nest_args.iters_per_nesting_level = info.nElemsPerTask;
    nest_args.parent_iteration = iter_id;
    nest_args.num_iterations = arg->num_iterations;
    nest_args.control_value = CONTROL_VALUE;
    nest_args.spawn_policy = arg->spawn_policy;

    gmt_handle_t handler = gmt_get_handle();
    //GMT_DEBUG_PRINTF(" got handler %u\n", handler);
    gmt_for_loop_with_handle(nest_args.iters_per_nesting_level , 1, 
            for_loop_whandle_nested_body, &nest_args, sizeof(nest_args_t), nest_args.spawn_policy, handler);
    gmt_wait_handle(handler);

    uint64_t initial_iters = nest_args.iters_per_nesting_level;
    uint64_t tot_iters_pernode = 0;

    while( (initial_iters /= 2) > 1) tot_iters_pernode+=initial_iters;

    if(arg->check){
        uint64_t value;
        uint64_t check_iter = iter_id%arg->num_iterations;
        uint64_t check_value = info.gdata+TEST_FOR_LOOP_WHANDLE_NESTED+check_iter;
        uint64_t offset = check_iter;
        /*GMT_DEBUG_PRINTF("gettig gdata %u offset %lu\n", 
          info.gdata, iter_id*sizeof(uint64_t));*/

        gmt_get(info.gdata, offset, &value, 1); 
        /*GMT_DEBUG_PRINTF("gettig gdata %u offset %lu done\n", 
          info.gdata, iter_id*sizeof(uint64_t));*/
        TEST(value == check_value);
    }

    /*TEST(verify_check_values(info.gdata,offsets,info.nElemsPerTask,
      check_value,info.elem_bytes));*/

}
