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

void test_alloc ( uint64_t iter_id, uint64_t num_it, const void * args, gmt_handle_t handle ) {
    _unused(num_it); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    _unused(iter_id);
    gmt_data_t ga = 0;
    uint64_t n;
    for(n = 0; n < arg->num_oper; n++){
        if( arg->zero_flag){
            ga = gmt_alloc(arg->elem_bytes, 1, arg->alloc_type, NULL);
        }else{
            ga = gmt_alloc(arg->elem_bytes, 1, arg->alloc_type, NULL);
        }

        /*printf("[n %u] iter %lu allocated ga %u\n", node_id, iter_id, ga);*/
        if( arg->check ){
            /* check that garray is initialized to zero */
            uint64_t remaining = arg->elem_bytes;
            uint8_t data[BLOCK_VALUES_SIZE];

            while( remaining > 0 ){
                uint64_t offset = arg->elem_bytes - remaining;
                uint64_t to_get = MIN(remaining,BLOCK_VALUES_SIZE);
                /*printf("%s getting offset %lu of %lu bytes from ga %u\n", 
                  __func__, offset, to_get, ga);*/
                gmt_get(ga, offset, data, to_get);
                if(arg->zero_flag){
                    uint64_t i;
                    for (i = 0; i < to_get; i++)
                        TEST( data[i] == 0);
                }
                remaining -= to_get;
            }
        }
        gmt_free(ga);
    }
}

