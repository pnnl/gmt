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


typedef struct fill_args_tag{
    gmt_data_t ga; 
    uint64_t arraysize;
}fill_args_t;

typedef struct test_file_args_tag{
    uint64_t arraysize;
    gmt_data_t ga; 
    gmt_data_t ga_new; 
}test_file_args_t;

void fill_garray_body( uint64_t iter_id, void*_args){
    fill_args_t *args = (fill_args_t *)_args;

    uint64_t node_chunk = MAX(CEILING(args->arraysize,gmt_num_nodes()),
            sizeof(uint64_t));
    uint64_t node_offset = node_chunk*iter_id;
    if(  node_offset >= args->arraysize)
        return;

    if((iter_id+1) * node_chunk > args->arraysize)
        node_chunk = args->arraysize - (iter_id*node_chunk);
   

    uint64_t written = 0, n = 0, to_write = 0;
    uint64_t*buf = (uint64_t*)calloc(node_chunk,1);

    GMT_DEBUG_PRINTF("%s node_chunk %lu node_offset %lu arraysize %lu\n", 
            __func__, node_chunk, node_offset, args->arraysize);

    for(written = 0; written < node_chunk; written += to_write){
        to_write  = MIN(args->arraysize - written, node_chunk);

        //assert(to_write % sizeof(uint64_t) == 0);
        for(n = 0; n < to_write/sizeof(uint64_t); n++){
            buf[n] = n+node_offset+written;
        }

       /*GMT_DEBUG_PRINTF("doing put of %lu with offset %lu\n", 
                to_write,node_offset +written);*/
        gmt_put(args->ga, node_offset + written, buf, to_write);

    }

    free(buf);

    //GMT_DEBUG_PRINTF("%s done\n", __func__);
}

#define CHECK_FILE_BLOCK (16*1024*1024UL)
void check_file_body( uint64_t iter_id, void *_args){

    test_file_args_t *args = (test_file_args_t*) _args;
    uint64_t node_chunk = MAX(CEILING(args->arraysize,gmt_num_nodes()),
            sizeof(uint64_t));
    uint64_t offset = node_chunk * iter_id;

    if( offset >= args->arraysize)
        return;

    if((iter_id+1) * node_chunk > args->arraysize)
        node_chunk = args->arraysize - (iter_id*node_chunk);


    uint64_t *ga_buf = (uint64_t*) calloc(CHECK_FILE_BLOCK,1);
    uint64_t *ga_buf_new = (uint64_t*) calloc(CHECK_FILE_BLOCK,1);
    TEST( ga_buf != NULL && ga_buf_new != NULL);

    uint64_t tested = 0;
    uint64_t test_chunk = 0;

    for(tested = 0; tested < node_chunk; tested += test_chunk){
        test_chunk = MIN(node_chunk, CHECK_FILE_BLOCK);

        gmt_get_nb(args->ga,offset+tested, ga_buf,test_chunk);
        gmt_get_nb(args->ga_new,offset+tested, ga_buf_new,test_chunk);
        gmt_wait_data();
        uint64_t i;
        for(i = 0; i < test_chunk/sizeof(uint64_t); i++){
     //       GMT_DEBUG_PRINTF("%lu (%lu)\n",ga_buf[i], ga_buf_new[i]);
            TEST(ga_buf[i] == ga_buf_new[i]);
        }
    }
    free(ga_buf);
    free(ga_buf_new);
}

#define HEADER_LEN 8 
#define HEADER_CONST (1000000*(node_id+1))
void test_file_write ( uint64_t iter_id, void * args) {

    arg_t *arg = (arg_t*)args;
    uint64_t elem_bytes = sizeof(uint64_t);
    bool_t check = arg->check;
    alloc_type_t alloc_type = arg->alloc_type;
    uint64_t num_oper = arg->num_oper;
    assert(num_oper > 0);
    /*GMT_DEBUG_PRINTF("num_oper: %lu elem_bytes: %lu check %d\n",
            num_oper, elem_bytes, check);*/

    gmt_data_t ga = 0;
    char filename[PATH_MAX];
    
    sprintf(filename,"./test_file_write.%lu.txt",iter_id);
    if(remove(filename) == -1 && errno != ENOENT)
        perror("test_file_write() system() failed");

    sleep(1);
    uint64_t arraysize = elem_bytes*num_oper;
    uint64_t filesize = arraysize;
    printf("doing gmt_alloc\n");
    ga = gmt_alloc(arraysize, 1, alloc_type, NULL);
    if( check){
        fill_args_t args;
        args.ga = ga;
        args.arraysize = arraysize;
        gmt_parFor_pernode(fill_garray_body, &args, sizeof(args));
    }
    printf("gmt_alloc done\n");
    
    
    double time;
    time = my_timer();
    gmt_file_write(ga, filename, 0);
    time = my_timer() - time;
    //GMT_DEBUG_PRINTF("file_write time %f BW %fs MB/s filesize %lu filename %s\n", 
    //        time, (filesize/(1024*1024))/time, filesize, filename);
    if( check ){
        struct stat str_stat;
        sleep(1);
        int ret = stat(filename,&str_stat);
        TEST(ret != -1);

        while((uint64_t)str_stat.st_size !=  filesize){
            ret = stat(filename,&str_stat);
            sleep(1);
            TEST((uint64_t)str_stat.st_size <=  filesize);
            TEST(ret != -1)
                /*        printf("str_stat.st_size %lu arraysize %lu\n",
                          (uint64_t)str_stat.st_size, arraysize);*/
        }

        time = my_timer();
        gmt_data_t ga_new = gmt_file_read( filename, 0, arraysize, alloc_type);
        time = my_timer() -time;
        GMT_DEBUG_PRINTF("file_read time %fs BW %f MB/s\n", time, (filesize/(1024*1024))/time);

        test_file_args_t args;
        args.arraysize = arraysize;
        args.ga = ga;
        args.ga_new = ga_new;
        gmt_parFor_pernode ( check_file_body, &args, sizeof(args) );

        gmt_free(ga_new);
    }
    
    gmt_free(ga);
    if(remove(filename) == -1 && errno != ENOENT)
        perror("test_file_write() system() failed");


}

