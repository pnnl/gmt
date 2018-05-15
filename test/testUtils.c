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

#include "testUtils.h"


void TestUtils_allocateElems(uint32_t elem_bytes, 
	uint64_t send_size, 
	uint64_t dataset_size, 
	uint32_t num_tasks, 
	bool_t random_elems,
	bool_t random_offsets,
	elemsInfo_t *info){

    //assert ( SIZE_BUFFERS % elem_bytes == 0 );
    assert(elem_bytes >= sizeof(uint32_t));
    //assert(send_size % elem_bytes == 0 );
    assert(dataset_size > elem_bytes);
    assert(dataset_size % sizeof(uint64_t) == 0 );
    assert(num_tasks > 0);

    uint64_t nElems = send_size / (uint64_t) elem_bytes;
    uint64_t nElemsPerTask = nElems / (uint64_t)num_tasks;

    /* fixing rounding errors */
    nElems = nElemsPerTask * num_tasks;
    send_size = nElems *(uint64_t) elem_bytes;

    assert ( nElems != 0 );
    assert ( nElemsPerTask != 0 );

    if ( node_id == 0 ){
	printf ("*********** Data allocation parameters **************\n");
	printf ("n %d num_tasks:%d send_size:%ld elem_bytes:%d\n"
		"dataset_size:%ld nElemsPerTask:%lu\n" 
		"random_elems:%u random_offsets:%u\n\n",
		node_id, 
		num_tasks, 
		send_size ,
		elem_bytes,
		dataset_size,
		nElemsPerTask,
		random_elems,
		random_offsets);
    }


    info->nElemsPerTask = nElemsPerTask;
    info->nElems = nElems;
    info->elem_bytes = elem_bytes;

    info->dataset = ( uint8_t * ) calloc ( dataset_size , sizeof(uint8_t));
    info->elems = ( uint8_t ** ) calloc ( info->nElems,  sizeof( uint8_t * ) );
    info->offsets = ( uint64_t * ) calloc ( info->nElems,  sizeof( uint64_t * ) );

    /* generate elems array*/
    uint64_t pos;
    for ( pos = 0; pos < info->nElems ; pos++ ) {
	long int r;

	if(random_elems)
	    r = (rand()*elem_bytes) %  (dataset_size-elem_bytes);
	else
	    r = (pos*elem_bytes) % (dataset_size-elem_bytes);

	/* elems */
	info->elems[pos] =&(info->dataset[r]);

	/*offsets*/
	if(random_offsets)
	    info->offsets[pos] = (rand() % info->nElems)*elem_bytes;
	else
	    info->offsets[pos] = pos*elem_bytes;

	/*  printf("n %u elems[%lu] = &dataset[%ld] = %p -  offsets[%lu] = %lu\n",
	    node_id,pos,r,&info->dataset[r],pos,info->offsets[pos]);*/
    }

}

void TestUtils_freeElems(elemsInfo_t *info){
    free(info->dataset);
    free(info->elems);
    free(info->offsets);
}

void TestUtils_print_bw(double t0, double t1, uint64_t dataBytes, uint64_t oper, uint64_t totBytes){

    if(node_id==0){
        if(totBytes > 0){
            printf ( "time %f sec - data %ld MB -  BW (data): %f MB/sec -  BW (data+overhead): %f MB/sec", 
                    t1 - t0, totBytes / ( 1024 * 1024 ), 
                    (( double ) dataBytes /  1024 / 1024 ) / ( t1-t0 ),
                    (( double ) totBytes /  1024 / 1024 ) / ( t1-t0 ));
        }else{
            printf ( "time %f sec - data %ld MB - BW: %f MB/sec", 
                    t1 - t0, dataBytes / ( 1024 * 1024 ), 
                    (( double ) dataBytes /  1024 / 1024 ) / ( t1-t0 ));
        }

        printf(" - Op/sec %f\n", oper/(t1-t0));
    }

    //printf ("********************************************************************************\n");

}

uint64_t *TestUtils_getTaskOffsets(uint64_t tid, elemsInfo_t * info){
    uint64_t pos = tid * info->nElemsPerTask;
    //printf("thread %lu &offsets[%lu]: %p \n",tid,pos,&info->offsets[pos]);
    assert(pos<info->nElems);
    return &(info->offsets[pos]); 

}

uint8_t **TestUtils_getTaskElems(uint64_t tid, elemsInfo_t * info){
    uint64_t pos = tid * info->nElemsPerTask;
    //printf("thread %lu &elems[%lu]: %p \n",tid,pos,&info->elems[pos]);
    assert(pos<info->nElems);
    return &(info->elems[pos]); 

}

void TestUtils_set_elem(uint8_t*ptr,uint64_t elem_value,uint32_t elem_bytes){
    uint64_t * elem_ptr = (uint64_t *) ptr;

    switch (elem_bytes) {
	case 8:
	    *elem_ptr = elem_value ;
	    break;
	case 4:
	    *(uint32_t*)(elem_ptr) = (uint32_t)elem_value;
	    break;
	case 2:
	    *(uint16_t*)(elem_ptr) = (uint16_t)elem_value; 
	    break;
	case 1:
	    *(uint8_t*)(elem_ptr) = (uint8_t)elem_value; 
	    break;
	default:
	    ERRORMSG("Size not supported\n");
    }
}

bool_t TestUtils_check_elem(uint8_t*ptr,uint64_t elem_value,uint32_t elem_bytes){
    uint64_t * elem_ptr = (uint64_t*)ptr;

    bool_t ret = false;
    //printf("n %u %s elem_value %lu\n",node_id,__func__,*elem_ptr);
    switch (elem_bytes) {
	case 8:
	    ret = ((*elem_ptr) == elem_value);
	    break;
	case 4:
	    ret = (*(uint32_t*)(elem_ptr) == (uint32_t)elem_value);
	    break;
	case 2:
	    ret = (*(uint16_t*)(elem_ptr) == (uint16_t)elem_value);
	    break;
	case 1:
	    ret = (*(uint8_t*)(elem_ptr) == (uint8_t)elem_value);
	    break;
	default:
	    ERRORMSG("Size not supported\n");
    }

    return ret;
}

void TestUtils_set_elems_value ( uint8_t **elems, uint64_t num_elems, uint64_t elem_value, uint32_t elem_bytes) {
    uint64_t i;
    for ( i = 0; i < num_elems; i++ ) 
	TestUtils_set_elem(elems[i],elem_value,elem_bytes);
}

bool_t TestUtils_check_elems_value( uint8_t **elems, uint64_t num_elems, uint64_t elem_value, uint32_t elem_bytes) {
    /*printf("n %u %s elems %p num_elems %lu elem_value %lu elem_bytes %u ",
      node_id, __func__, elems, num_elems, elem_value, elem_bytes);*/

    uint64_t i;
    bool_t ret = true;
    for ( i = 0; ret && i < num_elems; i++ ) {
	ret = ret && TestUtils_check_elem(elems[i],elem_value,elem_bytes);
    }

    return ret;
}


