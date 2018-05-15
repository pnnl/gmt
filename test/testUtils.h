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

#ifndef _TESTUTILS_H
#define _TESTUTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

#include "gmt/debug.h"
#include "gmt/utils.h"
#include "gmt/gmt.h"

extern uint32_t num_nodes;
extern uint32_t node_id;
typedef bool bool_t;
typedef struct elemsInfo_tag{
  uint8_t * dataset;
  uint8_t ** elems;
  uint64_t *offsets;
  uint64_t nElemsPerTask;
  uint64_t nElems;
  uint32_t elem_bytes;
  gmt_data_t gdata;
}elemsInfo_t;

uint8_t **TestUtils_getTaskElems(uint64_t tid, elemsInfo_t * info);
uint64_t *TestUtils_getTaskOffsets(uint64_t tid, elemsInfo_t * info);

void TestUtils_allocateElems(uint32_t elem_bytes, 
	uint64_t send_size, 
	uint64_t dataset_size, 
	uint32_t num_tasks, 
	bool_t random_elems,
	bool_t random_offsets,
	elemsInfo_t *info);


void TestUtils_freeElems(elemsInfo_t * info);

void TestUtils_print_bw(double t0, double t1, uint64_t dataBytes, uint64_t oper, uint64_t totBytes);

void TestUtils_set_elems_value ( uint8_t **elems, uint64_t num_elems, uint64_t elem_value, uint32_t elem_bytes);
bool_t TestUtils_check_elems_value( uint8_t **elems, uint64_t num_elems, uint64_t elem_value, uint32_t elem_bytes); 

bool_t TestUtils_check_elem(uint8_t*ptr,uint64_t elem_value,uint32_t elem_bytes);
void TestUtils_set_elem(uint8_t*ptr,uint64_t elem_value,uint32_t elem_bytes);
#endif
