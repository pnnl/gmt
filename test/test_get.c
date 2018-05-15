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


void test_get ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    uint64_t non_blocking = arg->non_blocking;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 


    uint64_t idx = iter_id % arg->num_iterations;
    //printf("n %u %s iter_id %lu id:%lu\n",node_id, __func__, iter_id, idx);

    uint8_t **elems=TestUtils_getTaskElems(idx, &info);
    //uint64_t *offsets=TestUtils_getTaskOffsets(idx, &info);
    uint8_t check_value = node_id+info.gdata+TEST_GET;

    if(arg->check){ /* put check values */
        uint32_t i;
        for (i=0;i < info.nElemsPerTask; i++ ) 
            gmt_put_value_nb( info.gdata, i, check_value);
        gmt_wait_data();
    }

    uint64_t j = 0, i = 0;
    for (i = 0; i < info.nElemsPerTask;i++) {
        /*printf("n %u task %lu putting elems[%lu]:%u at offset %lu of gdata %u\n",
          node_id, iter_id, i,(uint32_t) *elems[i],offsets[i],info.gdata);*/
        gmt_get_nb ( info.gdata, i, elems[i], 1 );

        if( j == non_blocking){
            gmt_wait_data();
            j = 0;
        }
        j++;
    }

    gmt_wait_data();

    if(arg->check) 
        TEST(TestUtils_check_elems_value(elems, info.nElemsPerTask, check_value, sizeof(uint8_t)));
}

