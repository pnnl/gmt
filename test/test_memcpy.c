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

void test_memcpy ( uint64_t iter_id, uint64_t num, const void * args, gmt_handle_t handle ) {
    _unused(iter_id);
    _unused(num); _unused(handle);
    arg_t *arg = ( arg_t* ) args;
    uint32_t i;

    uint64_t array_size = arg->elem_bytes*arg->num_oper;

    gmt_data_t src = gmt_alloc(array_size, 1, arg->alloc_type, NULL);
    gmt_data_t dst = gmt_alloc(array_size, 1, arg->alloc_type, NULL);

    if(arg->check){ /* put check values */
            //GMT_DEBUG_PRINTF("write:");
        for (i=0;i < arg->num_oper; i++ ) {
            //printf(" %u",i);
            gmt_put_value_nb( src, i, (uint8_t)i);
        }
        //printf("\n");
        gmt_wait_data();
    }

    for (i=0;i < arg->num_oper; i++ ) {
        uint64_t offset = i;
        gmt_memcpy( src, offset, dst, offset, 1 );
    }

    if(arg->check){ /* check values */
        uint8_t *chunk = (uint8_t *)malloc(arg->elem_bytes);
        //GMT_DEBUG_PRINTF("check:");
        for (i=0;i < arg->num_oper; i++ ) {
            gmt_get( dst, i , chunk, 1);
            TEST(chunk[0] == (uint8_t)i);
            //printf(" %u",chunk[0]);
        }
        //printf("\n");
        free(chunk);
    }
    
    gmt_free(src);
    gmt_free(dst);

}

