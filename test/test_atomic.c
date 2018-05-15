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

void test_atomic_cas ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    uint64_t idx = iter_id % arg->num_iterations;
    uint64_t check_value = node_id+info.gdata+TEST_ATOMIC_CAS;
    uint64_t new_value = check_value+TEST_ATOMIC_CAS;

    if(arg->check){ /* put check values */
        uint32_t i;
        for (i=0;i < info.nElemsPerTask; i++ ) {
            uint64_t offset = (idx*info.nElemsPerTask+i);
            gmt_put_value_nb( info.gdata, offset, check_value);
        }
        gmt_wait_data();
    }

    uint64_t i = 0;
    for (i = 0; i < info.nElemsPerTask ;i++) {
        uint64_t offset = (idx*info.nElemsPerTask+i);

        /* this should swap */
        uint64_t ret1 = gmt_atomic_cas ( info.gdata, offset, 
                check_value, new_value);
         /*GMT_DEBUG_PRINTF("%s atomicCAS on gdata %u offset %lu\n", 
          __func__, info.gdata, offset);*/
        if(arg->check){
        uint64_t ret2 = gmt_atomic_cas ( info.gdata, offset, 
                check_value, new_value);
            TEST( ret1 == check_value && ret2 == new_value);
        }
    }


}

void test_atomic_add ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    uint64_t idx = iter_id % arg->num_iterations;
    uint64_t check_value = node_id+info.gdata+TEST_ATOMIC_ADD;

    if(arg->check){ /* put check values */
        uint32_t i;
        for (i=0;i < info.nElemsPerTask; i++ ) {
            uint64_t offset = (idx*info.nElemsPerTask+i);
            gmt_put_value_nb( info.gdata, offset, check_value);
        }
        gmt_wait_data();
    }

    uint64_t i = 0;
    for (i = 0; i < info.nElemsPerTask ;i++) {

        uint64_t offset = (idx*info.nElemsPerTask+i);
        uint64_t ret1 = gmt_atomic_add ( info.gdata, offset, 1);

        if(arg->check){
            uint64_t ret2 = gmt_atomic_add ( info.gdata, offset, 1);
            TEST( ret1 == check_value && ret2 == check_value+1 );
        }
    }


}
