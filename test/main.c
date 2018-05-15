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


void get_fun_str ( const char * fun ) {

    strncpy ( glob.current_fun, fun, MAX_FUN_STR );
    /* remove arguments from string */
    char *end=strchr ( glob.current_fun,'(' );
    if ( end != NULL )
        *end = '\0';
}

void do_test ( gmt_for_loop_func_t test_func, void *arg, uint32_t arg_size) {

    uint64_t bw_bytes = glob.num_iterations *glob.num_oper*glob.elem_bytes;
    uint64_t total_iter = glob.num_iterations;

    printf("%s: ",glob.current_fun);

    double start_time = my_timer();
    gmt_for_loop (total_iter,1, test_func, arg, arg_size, glob.spawn_policy );
    double end_time  = my_timer();


    arg_t *args= (arg_t*)arg;
    if(args->check){
        printf("test PASSED - Time %f sec\n", end_time - start_time);
    }else{
        uint64_t oper = total_iter * glob.num_oper;
        TestUtils_print_bw(start_time, end_time, oper, bw_bytes, 0);
    }
}

void usage() {
    printf ( "\nUsage:\n" );
    printf ( " %s -b alloc         -i <iterations> -n <number of gmt_alloc() per iteration> -c <chunk size> \n",glob.prog_name );
    printf ( " %s -b for_loop_whandle        -i <iterations> -n <number of gmt_parFor() per iteration> -c <chunk size>\n",glob.prog_name );
    printf ( " %s -b for_loop_whandle_nested        -i <iterations> -n <number of gmt_parFor() per iteration> -c <chunk size>\n",glob.prog_name );
    printf ( " %s -b for_loop        -i <iterations> -n <number of gmt_parFor() per iteration> -c <chunk size>\n",glob.prog_name );
    printf ( " %s -b for_loop_nested -i <iterations> -n <nested gmt_parFor() per iteration> -c <chunk size>\n",glob.prog_name );
    printf ( " %s -b execute       -i <iterations> -n <gmt_execute() per iteration> -c <chunk size>    \n",glob.prog_name );
    printf ( " %s -b execute_on_all -i <iterations> -n <gmt_execute_pernode() per iteration> -c <chunk size>    \n",glob.prog_name );
    printf ( " %s -b get           -i <iterations> -n <operations per iteration> -c <chunk size> \n",glob.prog_name );
    printf ( " %s -b get_replica   -i <iterations> -n <operations per iteration> -c <chunk size>\n",glob.prog_name );
    printf ( " %s -b put        -i <iterations> -n <operations per iteration> -c <chunk size> \n",glob.prog_name );
    printf ( " %s -b putvalue   -i <iterations> -n <operations per iteration> -c <elem size>  \n",glob.prog_name );
    printf ( " %s -b atomic_add -i <iterations> -n <operations per iteration> -c <elem size>  \n",glob.prog_name );
    printf ( " %s -b atomic_cas -i <iterations> -n <operations per iteration> -c <elem size>  \n",glob.prog_name );
    printf ( " %s -b yield      -i <iterations> -n <operations per iteration> -c <elem size>  \n",glob.prog_name );
    printf ( " %s -b memcpy      -i <iterations> -n <operations per iteration> -c <chunk size>  \n",glob.prog_name );
    printf ( "\n Optional arguments:\n" );
    printf("-k <num non-blocking operations> (number of NB operations before calling a wait\n");
    printf("-a <alloc policy> (GMT_ALLOC_LOCAL, GMT_ALLOC_PARTITION, GMT_ALLOC_RANDOM or GMT_ALLOC_REMOTE)\n");
    printf("-s <spawn policy> (GMT_SPAWN_LOCAL, GMT_SPAWN_PARTITION, GMT_SPAWN_RANDOM or GMT_SPAWN_REMOTE)\n");
    printf ( " -p <preempt_policy> (GMT_PREEMPTABLE or GMT_NON_PREEMPTABLE or NA. Only for execute and execute_pernode test)\n");
    printf ( " -v (enable data integrity check)\n" );
    printf ( " -r (operations use random elems in local memory - only for put/get)\n");
    printf ( " -o (operations use random offset in global data (only for put/get/putvalue)\n");
    printf ( " -z (only for alloc test - set memory to zero )\n");

    exit ( EXIT_SUCCESS );
}


void alloc_type_str(alloc_type_t a, char * str){

    if( a == GMT_ALLOC_LOCAL)
        strcpy(str, "GMT_ALLOC_LOCAL");
    else if (a == GMT_ALLOC_PARTITION_FROM_ZERO)
        strcpy(str, "GMT_ALLOC_PARTITION_FROM_ZERO");
    else if (a == GMT_ALLOC_PARTITION_FROM_RANDOM)
        strcpy(str, "GMT_ALLOC_PARTITION_FROM_RANDOM");
    else if (a == GMT_ALLOC_PARTITION_FROM_HERE)
        strcpy(str, "GMT_ALLOC_PARTITION_FROM_HERE");
    else if (a == GMT_ALLOC_REMOTE)
        strcpy(str, "GMT_ALLOC_REMOTE");
    else if (a == GMT_ALLOC_REPLICATE)
        strcpy(str, "GMT_ALLOC_REPLICATE");
    else if (a == GMT_ALLOC_ZERO)
        strcpy(str, "GMT_ALLOC_ZERO");
    else if (a == GMT_ALLOC_RAM)
        strcpy(str, "GMT_ALLOC_RAM");
    else if (a == GMT_ALLOC_SHM)
        strcpy(str, "GMT_ALLOC_SHM");
    else if (a == GMT_ALLOC_SSD)
        strcpy(str, "GMT_ALLOC_SSD");
    else if (a==GMT_ALLOC_DISK)
        strcpy(str, "GMT_ALLOC_DISK");
    else{
        ERRORMSG("alloc policy not recognized");
    }
}

void spawn_policy_str(spawn_policy_t a, char * str){

    if( a == GMT_SPAWN_LOCAL)
        strcpy(str, "GMT_SPAWN_LOCAL");
    else if (a == GMT_SPAWN_REMOTE)
        strcpy(str, "GMT_SPAWN_REMOTE");
    else if (a == GMT_SPAWN_PARTITION_FROM_ZERO)
        strcpy(str, "GMT_SPAWN_PARTITION_FROM_ZERO");
    else if (a == GMT_SPAWN_PARTITION_FROM_RANDOM)
        strcpy(str, "GMT_SPAWN_PARTITION_FROM_RANDOM");
    else if (a == GMT_SPAWN_PARTITION_FROM_HERE)
        strcpy(str, "GMT_SPAWN_PARTITION_FROM_HERE");
    else if (a == GMT_SPAWN_SPREAD)
        strcpy(str, "GMT_SPAWN_SPREAD");
    else{
        ERRORMSG("spawn policy not recognized");
    }
}

void preempt_policy_str(preempt_policy_t a, char * str){

    if( a == GMT_PREEMPTABLE)
        strcpy(str, "GMT_PREEMPTABLE");
    else if (a == GMT_NON_PREEMPTABLE)
        strcpy(str, "GMT_NON_PREEMPTABLE");
    else{
        ERRORMSG("preempt policy not recognized");
    }
}

void test_parse_args ( int argc, char** argv ) {

    int c;
    /*   extract the program name  */
    for ( glob.prog_name = argv[0] + strlen ( argv[0] );
            glob.prog_name > argv[0] && * ( glob.prog_name - 1 ) != '/';
            glob.prog_name-- );

    while ( ( c = getopt ( argc, argv, "b:n:i:c:k:a:s:p:vroz" ) ) != -1 ) {

        switch ( c ) {
            case 'b':
                if ( strcmp ( optarg,"get" ) ==0 ) {
                    glob.test_num=TEST_GET;
                } else if ( strcmp ( optarg,"alloc" ) ==0 ) {
                    glob.test_num=TEST_ALLOC;
                } else if ( strcmp ( optarg,"file_write" ) ==0 ) {
                    glob.test_num=TEST_FILE_WRITE;
                } else if ( strcmp ( optarg,"get_replica" ) ==0 ) {
                    glob.test_num=TEST_GET_REPLICA;
                } else if ( strcmp ( optarg,"local_ptr" ) ==0 ) {
                    glob.test_num=TEST_LOCAL_PTR;
                } else if ( strcmp ( optarg,"put" ) ==0 ) {
                    glob.test_num=TEST_PUT;
                } else if ( strcmp ( optarg,"putvalue" ) ==0 ) {
                    glob.test_num=TEST_PUTVALUE;
                } else if ( strcmp ( optarg,"atomic_add" ) ==0 ) {
                    glob.test_num=TEST_ATOMIC_ADD;
                } else if ( strcmp ( optarg,"atomic_cas" ) ==0 ) {
                    glob.test_num=TEST_ATOMIC_CAS;
                } else if ( strcmp ( optarg,"execute" ) ==0 ) {
                    glob.test_num=TEST_EXECUTE;
                } else if ( strcmp ( optarg,"execute_with_handle" ) ==0 ) {
                    glob.test_num=TEST_EXECUTE_WITH_HANDLE;
                } else if ( strcmp ( optarg,"execute_on_all" ) ==0 ) {
                    glob.test_num=TEST_EXECUTE_ON_ALL;
                } else if ( strcmp ( optarg,"execute_on_all_with_handle" ) ==0 ) {
                    glob.test_num=TEST_EXECUTE_ON_ALL_WITH_HANDLE;
                } else if ( strcmp ( optarg,"for_loop_whandle" ) ==0 ) {
                    glob.test_num=TEST_FOR_LOOP_WHANDLE;
                } else if ( strcmp ( optarg,"spawn_at" ) ==0 ) {
                    glob.test_num=TEST_SPAWN_AT;
                } else if ( strcmp ( optarg,"for_loop_whandle_nested" ) ==0 ) {
                    glob.test_num=TEST_FOR_LOOP_WHANDLE_NESTED;
                } else if ( strcmp ( optarg,"for_loop" ) ==0 ) {
                    glob.test_num=TEST_FOR_LOOP;
                } else if ( strcmp ( optarg,"for_loop_nested" ) ==0 ) {
                    glob.test_num=TEST_FOR_LOOP_NESTED;
                } else if ( strcmp ( optarg,"yield" ) ==0 ) {
                    glob.test_num=TEST_YIELD;
                } else if ( strcmp ( optarg,"memcpy" ) ==0 ) {
                    glob.test_num=TEST_MEMCPY;
                } else if ( strcmp ( optarg,"for_each") ==0) {
                    glob.test_num=TEST_FOR_EACH;
                } else if ( strcmp ( optarg, "execute_on_node") ==0) {
                    glob.test_num=TEST_EXECUTE_ON_NODE;
                }
                   else {
                    printf ( "\nERROR: test not recognized\n" );
                    usage();
                }
                //printf("glob.test_num:%d\n",glob.test_num);
                break;
            case 'n':
                glob.num_oper = strtol_suffix ( optarg );
                assert(glob.num_oper>0);
                break;
            case 'i':
                glob.num_iterations = strtol_suffix ( optarg );
                if(glob.num_iterations==0)
                    ERRORMSG("iterations must be > 0\n");
                break;
            case 'c':
                glob.elem_bytes = strtol_suffix ( optarg );
                assert(glob.elem_bytes>0);
                break;
            case 'k':
                glob.non_blocking = strtol_suffix ( optarg );
                break;
            case 'v':
                glob.check = true;
                break;
            case 'r':
                glob.random_elems = true;
                break;
            case 'o':
                glob.random_offsets = true;
                break;
            case 'z':
                glob.zero_flag = true;
                break;
            case 's':
                if ( strcmp ( optarg,"GMT_SPAWN_LOCAL" ) ==0 ) {
                    glob.spawn_policy=GMT_SPAWN_LOCAL;
                } else if ( strcmp ( optarg,"GMT_SPAWN_REMOTE" ) ==0 ) {
                    glob.spawn_policy=GMT_SPAWN_REMOTE;
                } else if ( strcmp ( optarg,"GMT_SPAWN_PARTITION_FROM_ZERO" ) ==0 ) {
                    glob.spawn_policy=GMT_SPAWN_PARTITION_FROM_ZERO;
                } else if ( strcmp ( optarg,"GMT_SPAWN_PARTITION_FROM_RANDOM" ) ==0 ) {
                    glob.spawn_policy=GMT_SPAWN_PARTITION_FROM_RANDOM;
                } else if ( strcmp ( optarg,"GMT_SPAWN_PARTITION_FROM_HERE" ) ==0 ) {
                    glob.spawn_policy=GMT_SPAWN_PARTITION_FROM_HERE;
                } else if ( strcmp ( optarg,"GMT_SPAWN_SPREAD" ) ==0 ) {
                    glob.spawn_policy=GMT_SPAWN_SPREAD;
                }else{
                    printf ( "\nERROR: spawn policy not recognized.\n"
                            "Must be one of: GMT_SPAWN_LOCAL GMT_SPAWN_REMOTE GMT_SPAWN_PARTITION\n" );
                    usage();
                }

                break;
            case 'a':
                if ( strcmp ( optarg,"GMT_ALLOC_LOCAL" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_LOCAL;
                } else if ( strcmp ( optarg,"GMT_ALLOC_PARTITION_FROM_ZERO" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_PARTITION_FROM_ZERO;
                } else if ( strcmp ( optarg,"GMT_ALLOC_PARTITION_FROM_RANDOM" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_PARTITION_FROM_RANDOM;
                } else if ( strcmp ( optarg,"GMT_ALLOC_PARTITION_FROM_HERE" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_PARTITION_FROM_HERE;
                } else if ( strcmp ( optarg,"GMT_ALLOC_REMOTE" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_REMOTE;
                } else if ( strcmp ( optarg,"GMT_ALLOC_REPLICATE" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_REPLICATE;
                } else if ( strcmp ( optarg,"GMT_ALLOC_ZERO" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_ZERO;
                } else if ( strcmp ( optarg,"GMT_ALLOC_RAM" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_RAM;
                } else if ( strcmp ( optarg,"GMT_ALLOC_SHM" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_SHM;
                } else if ( strcmp ( optarg,"GMT_ALLOC_SSD" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_SSD;
                } else if ( strcmp ( optarg,"GMT_ALLOC_DISK" ) ==0 ) {
                    glob.alloc_type=GMT_ALLOC_DISK;
                }else{
                    printf ( "\nERROR:  alloc policy not recognized.\n"
                            "Must be one of: GMT_ALLOC_LOCAL GMT_ALLOC_REMOTE GMT_ALLOC_PARTITION GMT_ALLOC_RANDOM\n" );
                    usage();
                }
                break;
            case 'p':
                if ( strcmp ( optarg,"GMT_NON_PREEMPTABLE" ) ==0 ) {
                  glob.preempt_policy =GMT_NON_PREEMPTABLE;
                }// default is GMT_PREEMPTABLE
                break;
            case '?':
            default:
                usage ();
        }
    }

    /* default to all non-blocking */
    if(glob.non_blocking == 0)
        glob.non_blocking = glob.num_oper;

    if (    glob.test_num == NO_VALUE ||
            glob.num_iterations == 0 ||
            glob.num_oper == 0 ) usage();

}

void print_test_params(){
    char alloc_str[64];
    char spawn_str[64];
    char preempt_str[64];
    alloc_type_str(glob.alloc_type, alloc_str);
    spawn_policy_str(glob.spawn_policy, spawn_str);
    preempt_policy_str(glob.preempt_policy, preempt_str);


    printf ("\n************************** GMT test parameters ********************\n");
    printf ( "num_iterations: %lu - num_oper: %lu - non_blocking: %lu\n"
            "elem_size: %lu check: %d policies: %s %s %s\n",
            glob.num_iterations, glob.num_oper, glob.non_blocking,
            glob.elem_bytes, glob.check, alloc_str,spawn_str, preempt_str);
}

typedef struct local_args_tag{
    uint64_t node_alloc_size;
    gmt_data_t glocal;
} local_args_t;

void init_replica(uint64_t node_alloc_size){
    uint64_t i;
    uint8_t check_value = BYTE_VALUE;
    for( i = 0; i < BLOCK_VALUES_SIZE; i++){
        block_values[i]=check_value;
    }

    uint32_t done = 0, to_write = 0; uint32_t index = 0;
    while( done < node_alloc_size ){
        to_write = MIN(node_alloc_size - done, BLOCK_VALUES_SIZE);
        gmt_put ( glob.glocal, index, block_values, 1);
        done += to_write;
        index++;
    }
}

void init_pernode(const void *_args, void *ret, uint32_t *retsz, gmt_handle_t handle){
    _unused(ret); _unused(retsz); _unused(handle);
    uint64_t i;
    local_args_t *args = (local_args_t*) _args;
    uint8_t check_value = BYTE_VALUE;
    for( i = 0; i < BLOCK_VALUES_SIZE; i++){
        block_values[i]=check_value;
    }

    uint32_t done = 0, to_write = 0; uint32_t index = 0;
    while( done < args->node_alloc_size ){
        to_write = MIN(args->node_alloc_size - done, BLOCK_VALUES_SIZE);
        gmt_put( args->glocal, index, block_values, 1);
        done += to_write;
        index++;
    }
}


/* allocate elems*/
void alloc_func(const void* _arg, uint32_t sz, void* _ret, uint32_t* _retsize, gmt_handle_t handle ){
  _unused(_ret);_unused(_retsize);_unused(handle);_unused(sz);
  assert(_arg!= NULL);
  glob_t *arg=(glob_t*)_arg;

  uint64_t node_alloc_size=arg->num_oper*arg->elem_bytes*arg->num_iterations;
  uint64_t dataset_size = MIN( node_alloc_size*16, MAX_DATASET );
  elemsInfo_t info;

  if( arg->test_num != TEST_ALLOC
      && arg->test_num !=TEST_YIELD
      && arg->test_num !=TEST_FILE_WRITE){
    /* allocate gdata because most tests need it*/
    TestUtils_allocateElems(arg->elem_bytes, node_alloc_size, dataset_size,
                            arg->num_iterations,arg->random_elems, arg->random_offsets, &info);
    info.gdata = gmt_alloc ( node_alloc_size/sizeof(uint64_t), sizeof(uint64_t),
                             arg->alloc_type | GMT_ALLOC_ZERO, NULL);
    assert(node_alloc_size % sizeof(uint64_t) == 0);
    info.nElems = node_alloc_size/sizeof(uint64_t);
  }

  /* update elemsInfo structure for this node */
  //GMT_DEBUG_PRINTF("%s done\n",__func__);
  gmt_put(arg->ginfo, node_id ,&info, 1);
  //_unused(iter);
}

void allocate_test_data(){

    /* allocat elems in a node*/

    /* allocate a elemsInfo_t for each node */
    glob.ginfo = gmt_alloc( gmt_num_nodes(), sizeof(elemsInfo_t), GMT_ALLOC_PARTITION_FROM_HERE, NULL);
    gmt_execute_on_all(alloc_func, &glob, sizeof(glob), GMT_PREEMPTABLE);
//    gmt_execute_pernode (alloc_func, &glob, sizeof(glob), 0, NULL, NULL, GMT_PREEMPTABLE );

}

void free_func(const void* _arg, uint32_t sz, void* _ret, uint32_t* _retsize, gmt_handle_t handle ){
  _unused(_ret);_unused(_retsize);_unused(handle);_unused(sz);
  //printf("%s node %u \n",__func__,node_id);
  glob_t *arg=(glob_t*)_arg;
  elemsInfo_t info;
  //printf("n %u %s get offset %lu\n",node_id,__func__,sizeof(info)*node_id);
  gmt_get(arg->ginfo,node_id,&info, 1);

  if( arg->test_num != TEST_ALLOC
      && arg->test_num !=TEST_YIELD
      && arg->test_num !=TEST_FILE_WRITE ){
    /*each node frees its gdata*/
    gmt_free(info.gdata);
    TestUtils_freeElems(&info);
  }
}

void free_test_data(){
    /*
    if(glob.spawn_policy == GMT_SPAWN_LOCAL){
        free_func(0,&glob);
    }else{
    */
        /* free the elemsInfo_t for each node */
    if(glob.ginfo != GMT_DATA_NULL){
//        gmt_execute_pernode (free_func , &glob, sizeof(glob), 0, NULL , NULL, GMT_PREEMPTABLE);
        gmt_execute_on_all  (free_func, &glob, sizeof(glob), GMT_PREEMPTABLE);
        gmt_free(glob.ginfo);
    }

}

void perform_tests(){

    printf("%s \n",__func__);
    print_test_params();

    /* allocate data */
    allocate_test_data();

    /* arguments for tasks*/
    arg_t arg;
    arg.non_blocking = glob.non_blocking;
    arg.check = glob.check;
    arg.zero_flag = glob.zero_flag;
    arg.ginfo = glob.ginfo;
    arg.glocal = glob.glocal;
    arg.num_iterations = glob.num_iterations;
    arg.num_oper = glob.num_oper;
    arg.elem_bytes = glob.elem_bytes;
    arg.alloc_type = glob.alloc_type;
    arg.preempt_policy = glob.preempt_policy;
    arg.spawn_policy = glob.spawn_policy;

    printf("******************* GMT test results ****************\n");

    switch ( glob.test_num ) {
            break;
        case TEST_ALLOC:
            DO_TEST (test_alloc, &arg, sizeof(arg));
            break;
        case TEST_EXECUTE:
            DO_TEST (test_execute, &arg, sizeof(arg));
            break;
        case TEST_EXECUTE_ON_ALL:
            DO_TEST (test_execute_on_all, &arg, sizeof(arg));
            break;
        case TEST_FOR_LOOP_WHANDLE:
            DO_TEST (test_for_loop_whandle, &arg, sizeof(arg));
            break;
        case TEST_EXECUTE_WITH_HANDLE:
            DO_TEST (test_execute_with_handle, &arg, sizeof(arg));
            break;
        case TEST_EXECUTE_ON_ALL_WITH_HANDLE:
            DO_TEST (test_execute_on_all_with_handle, &arg, sizeof(arg));
            break;
        case TEST_FOR_LOOP_WHANDLE_NESTED:
            DO_TEST (test_for_loop_whandle_nested, &arg, sizeof(arg));
            break;
        case TEST_FOR_LOOP:
            DO_TEST (test_for_loop, &arg, sizeof(arg));
            break;
        case TEST_FOR_LOOP_NESTED:
            DO_TEST (test_for_loop_nested, &arg, sizeof(arg));
        case TEST_YIELD:
            DO_TEST (test_yield, &arg, sizeof(arg));
            break;
        case TEST_MEMCPY:
            DO_TEST (test_memcpy, &arg, sizeof(arg));
            break;
        case TEST_GET:
            DO_TEST (test_get, &arg, sizeof(arg));
            break;
        case TEST_LOCAL_PTR:
            DO_TEST (test_local_ptr, &arg, sizeof(arg));
            break;
        case TEST_PUT:
            DO_TEST (test_put, &arg, sizeof(arg));
            break;
        case TEST_PUTVALUE:
            DO_TEST (test_putvalue, &arg, sizeof(arg));
            break;
        case TEST_ATOMIC_CAS:
              DO_TEST (test_atomic_cas, &arg, sizeof(arg));
            break;
        case TEST_ATOMIC_ADD:
              DO_TEST (test_atomic_add, &arg, sizeof(arg));
            break; /* always break here */
        case TEST_FOR_EACH:
              DO_TEST (test_for_each, &arg, sizeof(arg));
            break;
        case TEST_EXECUTE_ON_NODE:
              DO_TEST (test_execute_on_node, &arg, sizeof(arg));
            break;
        default:
            usage();
    }

    free_test_data();
}


int gmt_main ( uint64_t argc, char * argv[] ) {

    timing_init();

    /* standard configuration*/
    glob.tot_bytes=1; /* to avoid division by zero */
    glob.test_num=TEST_FOR_LOOP_WHANDLE;
    glob.num_oper=1;
    glob.elem_bytes = 8;
    glob.num_iterations=1;
    glob.check = false;
    glob.alloc_type = GMT_ALLOC_PARTITION_FROM_RANDOM;
    glob.spawn_policy = GMT_SPAWN_PARTITION_FROM_RANDOM;
    glob.preempt_policy = GMT_PREEMPTABLE;
    glob.non_blocking = 0;
    glob.random_elems= false;
    glob.random_offsets= false;
    glob.ginfo = GMT_DATA_NULL;
    glob.glocal = GMT_DATA_NULL;

    /* fill glob struct with command line parameters*/
    test_parse_args ( argc, argv );


    double start = my_timer();
    perform_tests();
    double end = my_timer();
    printf("total time: %f\n",end-start);

    // gmt_profile(0);
    gmt_timing(true);
    return 0;

}
