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


bool_t verify_check_values(gmt_data_t gdata, uint64_t* offsets, 
        uint64_t num_elems, uint64_t elem_value){
    _unused(offsets);
    /* get all my chunk in a buffer*/
    uint64_t *check_data = (uint64_t *)malloc(num_elems*sizeof(uint64_t));
    uint64_t i;
    for (i=0;i < num_elems; i++ ) {
        gmt_get_nb( gdata,i,&check_data[i], 1);
    }
    gmt_wait_data();

    /* check values*/
    bool_t ret = true;
    for (i=0;i < num_elems; i++ ) {
        ret = ret && TestUtils_check_elem((uint8_t*)&check_data[i], elem_value, sizeof(uint64_t));
    }

    free( check_data );
    return ret;
}

void test_put ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    uint64_t non_blocking = arg->non_blocking;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info, 1); 

    uint64_t idx = iter_id % arg->num_iterations;
    uint8_t **elems=TestUtils_getTaskElems(idx,&info);
    uint64_t *offsets=TestUtils_getTaskOffsets(idx,&info);
    uint8_t check_value = node_id+info.gdata+TEST_PUT;

    if(arg->check)
        TestUtils_set_elems_value(elems, info.nElemsPerTask,check_value ,sizeof(uint8_t));

    /* this iteration offset in the gdata  */
    //printf("n %u %s iter_id %u \n",node_id,__func__, iter_id);
    uint64_t j = 0, i = 0;

    for (i = 0; i < info.nElemsPerTask;i++) {
        /*printf("n %u iter_id %d %s elems[%lu]:%p value: %u at offsets[%lu]:%lu\n",
          node_id,iter_id,__func__,i,elems[i],(uint32_t) *elems[i],i,offsets[i]);*/
        gmt_put_nb ( info.gdata, i, elems[i], 1);

        if( j == non_blocking){
            gmt_wait_data();
            j = 0;
        }
        j++;
    }

    gmt_wait_data();

    if(arg->check)
        TEST(verify_check_values(info.gdata,offsets,info.nElemsPerTask, check_value));

}

void test_putvalue ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    uint64_t non_blocking = arg->non_blocking;
    elemsInfo_t info;
    gmt_get(arg->ginfo,node_id,&info,1); 

    uint64_t idx = iter_id % arg->num_iterations;
    uint64_t *offsets=TestUtils_getTaskOffsets(idx,&info);
    uint64_t check_value = node_id+info.gdata+TEST_PUTVALUE;

    assert(info.elem_bytes > 0);

    /* this iteration offset in the gdata  */
    uint64_t j = 0, i = 0;
    for (i = 0; i < info.nElemsPerTask ;i++) {

        /*printf("n %u iter_id %ld putting value %lu  of size %u at offset %lu of gdata %lu\n",
          node_id,iter_id,check_value, info.elem_bytes, offsets[i],info.gdata); */
        gmt_put_value_nb( info.gdata, i, check_value);

        if( j == non_blocking){
            // printf("wait commands\n");
            gmt_wait_data();
            j = 0;
        }
        j++;
    }

    gmt_wait_data();

    if(arg->check)
        TEST(verify_check_values(info.gdata,offsets,info.nElemsPerTask,check_value));
}
