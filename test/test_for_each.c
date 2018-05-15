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


void for_each_body(gmt_data_t data, uint64_t start_el, uint64_t num_el, const void *args, gmt_handle_t handle) {
    _unused(handle); _unused(data);
    nest_args_t *arg = ( nest_args_t* ) args;
    TEST(arg!= NULL);
    TEST(arg->control_value == CONTROL_VALUE);
    uint32_t taskid = gmt_task_id();
    uint64_t index = 0;

    if(arg->check) {
      for(index=0; index < num_el; index++) {
        TEST(gmt_get_local_ptr(data, start_el + index) != NULL);
        uint64_t old_value = gmt_atomic_cas(data, start_el + index, 0, 1);
        TEST(old_value == 0);
      }
    }
    //  GMT_DEBUG_PRINTF("FOR_EACH: start_el %lu num_el %lu"
    //  " taskid %u\n", start_el, num_el, taskid);
}

void test_for_each ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    // GMT_DEBUG_PRINTF("%s iter_id %lu num_elems %lu"
    // " num_iterations %lu\n", __func__,
    //    iter_id, info.nElems, arg->num_iterations);

    assert(arg->num_iterations > 0);
    //uint64_t non_blocking = arg->non_blocking;
    nest_args_t nest_args;
    nest_args.check = arg->check;
    nest_args.gdata = info.gdata;
    nest_args.iters_per_nesting_level = info.nElemsPerTask;
    nest_args.parent_iteration = iter_id;
    nest_args.num_iterations = arg->num_iterations;
    nest_args.control_value = CONTROL_VALUE;

    uint64_t iter_size = info.nElems/arg->num_iterations;
    assert(info.nElems%info.nElems == 0);
    uint64_t it_start = iter_size*iter_id;


    assert(info.nElems%arg->num_iterations == 0);
    gmt_for_each(info.gdata, 4, it_start, iter_size,
                      for_each_body, &nest_args,
                      sizeof(nest_args_t));
}
