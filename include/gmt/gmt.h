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

/** 
 * @defgroup GMT_module GMT
 * @brief Global Memory and Threading.
 */

/**
 * @file gmt.h
 * @ingroup  GMT_module
 *
 * Application programming interface of GMT.  
 * This file contain the definition of all the application level functions.
 * NOTE: functions not defined in this file are internal functions and 
 * should not be called from outside the GMT runtime.
 *
 */

#ifndef __GMT_H__
#define __GMT_H__

#if defined(__cplusplus)
#include <cstdint>
#else
#define __STDC_LIMIT_MACROS
#include <stdint.h>
#endif

#if !defined(EXTERNALAPI)
#include "gmt/debug.h"
#include "gmt/gmt_config.h"
#endif

/** Type for the GMT array 
 * @ingroup  GMT_module
 * */
typedef int64_t gmt_data_t;
/** Type NULL value for gmt_data_t 
 * @ingroup  GMT_module
 * */
#define GMT_DATA_NULL -1l

/** Type for handle used to check completion of async task creation primitives 
 * @ingroup  GMT_module
 * */
typedef uint32_t gmt_handle_t;
/** NULL value for gmt_handle_t 
 * @ingroup  GMT_module
 * */
#define GMT_HANDLE_NULL (~0u)


/* @cond INTERNAL */

/** 
 * Function prototype the user needs to implement to start a program
 * This is the equivalent on int main(int argc, char * argv[])
 * @param[in] argc contains the number of arguments 
 * @param[in] argv is an array of pointers to the various input 
 *            arguments (terminated with \0)
 * @ingroup  GMT_module
 */

#ifdef __cplusplus
extern "C" int gmt_main(uint64_t argc, char *argv[]);
#else
extern int gmt_main(uint64_t argc, char *argv[]);
#endif

/** 
 * Function prototype to implement an iteration of the for_loop primitives
 *
 * @param[in] start_it index of the first iteration 
 * @param[in] num_it number of iterations to perform 
 * @param[in] args arguments data structure pointer passed to this iteration
 *            (all the iterations have the same args)
 * @param[in] handle ::gmt_handle_t used to check for completion of all
 *            the tasks associated with this handle (could be NULL)
 * @ingroup  GMT_module
 */
typedef void (*gmt_for_loop_func_t) (uint64_t start_it, uint64_t num_it,
                                     const void *args, gmt_handle_t handle);

/** 
 * Function prototype to implement an iteration of the for_each primitive
 *
 * @param[in] gmt_array ::gmt_data_t that this iteration is going to work on 
 *            (each iteration will have the same gmt_array)
 * @param[in] start_el element at which this iteration
 *            will start (each iteration will have it different)
 * @param[in] num_el number of elements  to work on
 *            will start
 * @param[in] args arguments data structure pointer passed to this iteration
 *            (all the iterations have the same args)
 * @param[in] handle ::gmt_handle_t used to check for completion of all
 *            the tasks associated with this handle (could be NULL)
 * @ingroup  GMT_module
 */
typedef void (*gmt_for_each_func_t) (gmt_data_t gmt_array, uint64_t start_el,
                                     uint64_t num_el,
                                     const void *args, gmt_handle_t handle);

/** 
 * Function prototype to implement the task body for execute primitives
 *
 * @param[in]  args arguments data structure pointer
 * @param[out] ret return buffer pointer (the buffer is already allocated)
 * @param[out] ret_size size of data written in the return buffer 
 *
 * NOTE: The maximum argument data structure size is gmt_max_args_per_task() and 
 * the maximum return data structure size is ::MAX_RETURN_SIZE.
 *
 * @ingroup  GMT_module
 */

typedef void (*gmt_execute_func_t) (const void *args, uint32_t args_size,
                                    void *ret, uint32_t * ret_size,
                                    gmt_handle_t handle);

/* @endcond */

/**
 *  Allocation policies for GMT memory
 *   
 *  Allocation policies used by gmt_alloc()
 *  @ingroup  GMT_module
 */
typedef enum alloc_type_t {
    /* policy */
    /**< Allocate the array in the local node. */
    GMT_ALLOC_LOCAL = 0,
    /**< Allocate the array evenly among nodes starting from node 0. 
     * It is guaranteed that an element is never split across nodes. */
    GMT_ALLOC_PARTITION_FROM_ZERO = 1,
    /**< Allocate the array evenly among nodes starting from a random node. */
    GMT_ALLOC_PARTITION_FROM_RANDOM = 2,
    /**< Allocate the array evenly among nodes starting from the allocating node. 
     * It is guaranteed that an element is never split across nodes. */
    GMT_ALLOC_PARTITION_FROM_HERE = 3,
    /**< Allocate the array in all nodes except the local node. */
    GMT_ALLOC_REMOTE = 4,
    /**< Allocate a replicated array in all nodes. Each replica has the same 
     * size */
    GMT_ALLOC_REPLICATE = 5,
    GMT_ALLOC_PERNODE = 6, /**< This is not used */
    /**< Initialize to all bytes to ZERO  */
    GMT_ALLOC_ZERO = 8,
    /**< Allocate on RAM */
    GMT_ALLOC_RAM = 0,
    /**< Allocate on shared memory (permanent only if a state name is given) */
    GMT_ALLOC_SHM = 16,
    /**< Allocate on SSD  (always permanent) */
    GMT_ALLOC_SSD = 32,
    /**< Allocate on DISK (always permanent) */
    GMT_ALLOC_DISK = 48
} alloc_type_t;

/**
 *  Spawn policies for GMT tasks.
 *   
 *  Spawn policies used by gmt_for_ 
 *  
 *  @ingroup  GMT_module
 */
typedef enum spawn_policy_tag {
    GMT_SPAWN_LOCAL,     /**< Spawn tasks in the local node. */
    GMT_SPAWN_REMOTE,    /**< Spawn tasks in all nodes except the local. */
    GMT_SPAWN_PARTITION_FROM_ZERO, /**< Spawn tasks evenly among nodes starting from node 0. */
    GMT_SPAWN_PARTITION_FROM_RANDOM,    /**< Spawn tasks evenly among nodes starting from a 
                              random node. */
    GMT_SPAWN_PARTITION_FROM_HERE, /**< Spawn tasks evenly among nodes starting
                                        from the calling node. */
    GMT_SPAWN_SPREAD    /**< Spawn tasks evenly across nodes 0 to N-1. */
} spawn_policy_t;

/**
 *  Preemption policies for a spawning GMT tasks.
 *   
 *  Select the preemption spawn policies used when creating tasks with 
 *  gmt_execute(), gmt_execute_nb(), gmt_execute_pernode(), gmt_execute_pernode_nb(), 
 *  @ingroup  GMT_module
 */
typedef enum preempt_policy_tag {
    GMT_PREEMPTABLE,     /**< create a standard GMT task with its stack and 
                              context which they can and will preempt to allow 
                              other tasks to run */
    GMT_NON_PREEMPTABLE  /**< create a fast spawning not preemptable GMT task 
                              that executes on an existing stack and context 
                              until it terminates. Tasks created with this 
                              policy cannot call any global GMT primitive 
                              (but they can call gmt_local_ptr()), hence they 
                              must exclusively use local data and cannot 
                              spawn other tasks */
} preempt_policy_t;

/* @cond INTERNAL */

/** 
  * Handful define used to print a message such that we know which node, worker 
  * and task has generated the message
  * @ingroup  GMT_module
  **/
#define  GMT_DEBUG_PRINTF(format, ...) {                                     \
    printf("[n:%u w:%u t:%u] ",gmt_node_id(),gmt_worker_id(),gmt_task_id()); \
    printf(format, ## __VA_ARGS__);                                          \
}

/** 
  * Handful define used to print an ERROR message and terminate, this is the 
  * safest way to terminate a GMT program on error as the _exit is called 
  * immediately.
  * @ingroup  GMT_module
  **/
#define GMT_ERROR_MSG(format, ...) {                                           \
    printf("\n[n:%u w:%u t:%u] [ERROR] in function %s() file \"%s\" "          \
            "line %d:\n\t\t",                                                  \
            gmt_node_id(), gmt_worker_id(), gmt_task_id(), __func__, __FILE__ ,\
            __LINE__ );                                                        \
    printf(format, ## __VA_ARGS__);                                            \
    printf("\n");                                                              \
    fflush(stdout);                                                            \
    _exit(EXIT_FAILURE); }

#if defined(__cplusplus)
extern "C" {
#endif

    int gmt_get_comm_buffer_size();
    /* Maximum size of the arguments for ::gmt_for() and ::gmt_for_each()
     * */
    uint32_t gmt_max_args_per_task();
    uint32_t gmt_max_tasks_per_worker();
    uint32_t gmt_max_return_size();

    /**
     * Returns the id of the node (MPI rank) where the task is executed
     *
     * @return the MPI rank
     * @ingroup GMT_module
     */
    uint32_t gmt_node_id();

    /**
     * Returns the number of nodes
     *
     * @return the number of nodes (MPI processes)
     * @ingroup GMT_module
     */
    uint32_t gmt_num_nodes();

    /**
     * Returns the id of the worker currently running that portion of the code
     *
     * @return the id of the worker (0 to gmt_num_workers())
     * @ingroup GMT_module
     */
    uint32_t gmt_worker_id();

    /**
     * Returns the number of workers running in the node
     *
     * @return the number of workers running in the node
     * @ingroup GMT_module
     */
    uint32_t gmt_num_workers();

    /**
     * Returns the id of the task currently running 
     *
     * @return the id of the running task 
     * @ingroup GMT_module
     */
    uint32_t gmt_task_id();

    /**
     * Returns True if the task currently running is non_preemptable
     *
     * @return a bool indicating whether the task is non_preemptable. 
     * @ingroup GMT_module
     */
    bool gmt_task_is_non_preemptable();

    /**
     * Sets the seed for the random number generator.
     * The seed is set for each worker in the current node. Each worker has a 
     * different random number generator.
     *
     * @param[in] seed seed value
     * @ingroup GMT_module
     */
    void gmt_srand(uint64_t seed);

    /**
     * Returns a random number for the random number generator of this worker.
     *
     * @return a random number
     * @ingroup GMT_module
     */
    uint64_t gmt_rand();

    /**
     * Returns the current time from the beginning of UTC time in seconds 
     *
     * @returns a double value in seconds (resolution microseconds)
     * @ingroup GMT_module
     */
    double gmt_timer();

    /**************************************************************************/
    /*                                                                        */
    /*                              ALLOC/FREE                                */
    /*                                                                        */
    /**************************************************************************/

    //@{
    /** 
     * Allocates a GMT array of num_elems each of size bytes_per_elem. 
     * Allocation is performed using the ::alloc_type_t policy.
     * For all the allocation policies that results in allocation on multiple
     * nodes, it is guaranteed that an element is never split across nodes.
     * If an array_name is given the array is preserved in the current GMT state
     * (selectable starting the process with --gmt-state=state_name) and it can
     * be attached in another process (which starts with the same GMT state) by 
     * using ::gmt_attach(array_bane). 
     * If the array_name is NULL then the array is not going 
     * to be visible to another process even if attaching to the same GMT state.
     * If the process is started without a GMT state, then even if an array_name
     * is given the gmt_array is not going to be preserved (there is no state 
     * where to do it).
     * To remove a named array from a GMT state is enough to perform a 
     * ::gmt_free.
     * Non blocking '_nb' waits completion with ::gmt_wait_data()
     *
     * @param[in] num_elems to allocate.
     * @param[in] bytes_per_elem size in bytes of each element to allocate
     * @param[in] alloc_type allocation policy ::alloc_type_t
     * @param[in] array_name name of this array
     *
     * @ingroup GMT_module
     */
    gmt_data_t gmt_alloc(uint64_t num_elems, uint64_t bytes_per_elem,
                         alloc_type_t alloc_type, const char *array_name);

    gmt_data_t gmt_alloc_nb(uint64_t num_elems, uint64_t bytes_per_elem,
                         alloc_type_t alloc_type, const char *array_name);
    /** 
     * Returns true if the array associated with the gmt_array is
     * acctualy allocated.
     * @returns true if allocated
     */
    bool gmt_is_allocated(gmt_data_t gmt_array);
    //@}

    /**
     * Function that returns the gmt_data_t present in the sate with
     * array_name
     *
     * @param[in]  array_name user defined GMT array name
     *
     * @returns    ::gmt_data_t in the current GMT state with name
     *             array_name. If a GMT state is not given or the array_name
     *             does not exist or is not assigned ::GMT_DATA_NULL is returned
     */
    gmt_data_t gmt_attach(const char *array_name);

    /** 
     * Free a GMT array allocated with ::gmt_alloc
     *
     * @param[in] gmt_array the ::gmt_data_t of the array
     * @ingroup GMT_module
     */
    void gmt_free(gmt_data_t gmt_array);

    /** 
     * Returns the global identifier of a GMT array (gid). 
     *
     * @param[in] gmt_array input GMT Array
     * @returns the gid of a GMT array
     */
    uint32_t gmt_get_gid(gmt_data_t gmt_array);

    /** 
     * Returns the local pointer to a given element of a GMT array. Return NULL if node does not 
     * have any physical data for this gmt array
     *
     * @param[in] gmt_array GMT array 
     * @param[in] elem_offset offset in number of elements in the GMT array
     * @return pointer to the GMT array at offset elem_offset or NULL if the node
     * does note have local data for this array.
     *
     * @ingroup GMT_module
     */
    void *gmt_get_local_ptr(gmt_data_t gmt_array, uint64_t elem_offset);

    /** 
     * Returns the element size in bytes for this gmt array
     *
     * @return the number of bytes per element of the GMT array
     *
     * @ingroup GMT_module
     */
    uint64_t gmt_get_elem_bytes(gmt_data_t gmt_array);

    /** 
     * Returns the allocation policy 
     *
     * @return the allocation policy of the GMT array
     *
     * @ingroup GMT_module
     */
    alloc_type_t gmt_get_alloc_type(gmt_data_t gmt_array);


    /**************************************************************************/
    /*                                                                        */
    /*                      PUT/PUT_VALUE/GET/COPY                            */
    /*                                                                        */
    /**************************************************************************/

    //@{
    /** 
     * Copy memory from local memory to a GMT array (element version).
     * Non blocking `_nb' waits completion with ::gmt_wait_data()
     *
     * @param[in] gmt_array destination GMT array 
     * @param[in] elem_offset offset in number of elements in the GMT array
     * @param[in] elem pointer to the local element
     * @param[in] num_elem number of elements to copy
     *
     * @ingroup GMT_module
     */
    void gmt_put(gmt_data_t gmt_array, uint64_t elem_offset,
                 const void *elem, uint64_t num_elem);
    void gmt_put_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                    const void *elem, uint64_t num_elem);
    //@}

    //@{
    /** 
     * Writes an element into a GMT array passing it by value. Can only be used 
     * for array containing elements different than 8,4,2 or 1 bytes.
     * Non blocking '_nb' waits completion with ::gmt_wait_data()
     *
     * @param[in] gmt_array destination GMT array 
     * @param[in] elem_offset offset in number of elements in the GMT array
     * @param[in] value element to write 
     *
     * @ingroup GMT_module
     */
    void gmt_put_value(gmt_data_t gmt_array, uint64_t elem_offset,
                       const uint64_t value);
    void gmt_put_value_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                          const uint64_t value);
    //@}

    //@{
    /** 
     * Copy memory from a GMT array to the local memory. (bytes version)
     * Non blocking '_nb' waits completion with ::gmt_wait_data()
     *
     * @param[in]  gmt_array source GMT array
     * @param[in]  elem_offset offset in number of elements in the GMT array
     * @param[out] elem pointer to the local element
     * @param[in]  num_elem number of elements to copy
     *
     * @ingroup GMT_module
     */
    void gmt_get(gmt_data_t gmt_array, uint64_t elem_offset,
                 void *elem, uint64_t num_elem);
    void gmt_get_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                    void *elem, uint64_t num_elem);
    //@}

    /** 
     * Copy memory from a GMT array to another (element version).
     *
     * @param[in] src source GMT array 
     * @param[in] src_elem_offset offset in number of elements in the 
     *    source array
     * @param[in] dst destination GMT array 
     * @param[in] dst_elem_offset offset in number of elements in the 
     *    destination array
     * @param[in]  num_elem number of elements to copy
     *
     * @ingroup GMT_module
     */
    void gmt_memcpy(gmt_data_t src, uint64_t src_elem_offset,
                    gmt_data_t dst, uint64_t dst_elem_offset,
                    uint64_t num_elem);

    /**
     * Waits for completion of any non blocking put/get/atomic data operation on
     * ::gmt_data_t such as ::gmt_put_nb(), ::gmt_put_value_nb(), 
     * ::gmt_get_nb(), ::gmt_get_value_nb(), ::gmt_atomic_add_nb(),
     * ::gmt_atomic_cas_nb() and ::gmt_put_value_nb() 
     *
     *
     * @ingroup GMT_module
     */
    void gmt_wait_data();

    /**************************************************************************/
    /*                                                                        */
    /*                               ATOMICS                                  */
    /*                                                                        */
    /**************************************************************************/

    //@{
    /** 
     * Perform an atomic add into an element of a GMT array 
     * (similar to sync_fetch_and_add). Can only be used on arrays containing
     * elements of 8,4,2 or 1 bytes.
     * Non blocking '_nb' waits completion with ::gmt_wait_data()
     *
     * @param[in] gmt_array GMT array
     * @param[in] elem_offset offset in number of elements in the GMT array
     * @param[in] value value to add 
     *
     * @return value of the variable before the operation
     *
     * @ingroup GMT_module
     */
    int64_t gmt_atomic_add(gmt_data_t gmt_array, uint64_t elem_offset,
                           int64_t value);

     void gmt_atomic_add_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                           int64_t value, int64_t * ret_value_ptr);
    //@}
    
    //@{
    //
    //
    /** 
     * Perform an atomic compare and swap into a GMT array
     * (similar to sync_compare_and_swap). Can only be used 
     * for array containing elements of 8,4,2 or 1 bytes.
     *
     * @param[in]  gmt_array GMT array
     * @param[in]  elem_offset offset in number of elements in the GMT array
     * @param[in]  old_value old value 
     * @param[in]  new_value new value to compare against old value
     *
     * @return value of the variable before the operation
     *
     * @ingroup GMT_module
     */
    int64_t gmt_atomic_cas(gmt_data_t gmt_array, uint64_t elem_offset,
                           int64_t old_value, int64_t new_value);

    void gmt_atomic_cas_nb(gmt_data_t gmt_array, uint64_t elem_offset,
                           int64_t old_value, int64_t new_value,
                           int64_t * ret_value_ptr);
    //@{

    /**************************************************************************/
    /*                                                                        */
    /*                             FOR/FOR_EACH                               */
    /*                                                                        */
    /**************************************************************************/

    //@{
    /** 
     * GMT Parallel for each primitive. Starts a task with body `func' for each
     * `elem_per_task' elements. The function is most likely executed on 
     * the node that owns those elements.
     *
     * The ::gmt_for_each() blocks the calling task until all the created tasks 
     * are not completed.
     *
     * The ::gmt_for_each_nb() allows the calling task to continue execution and
     * to wait later for the completion of all the created tasks by using 
     * ::gmt_wait_for_nb().
     *
     * The ::gmt_for_each_with_handle() allows the calling task to terminate 
     * (and free the resources) before the created tasks complete. A handle is 
     * passed through the arguments to identify the sub-tree of tasks associated
     * with the same handle. At the end, it is required to wait for the 
     * termination of the tasks associated with a given handle calling 
     * ::gmt_wait_handle(). A valid handle must be requested with
     * ::gmt_get_handle().  
     *
     * WARNING: these primitives DO NOT GUARANTEE that the task will always be
     * executed on the node that owns the elements. In high-workload scenarios 
     * nodes might be full and the tasks will be executed locally. In case of 
     * unexpected local execution, the ::gmt_get_local_ptr() will 
     * return NULL (if the policy is not GMT_ALLOC_REPLICATE ).
     * When ::gmt_get_local_ptr() returns NULL remote data can only 
     * be accessed using ::gmt_put() or 
     * ::gmt_get() primitives. To execute on a specific node safely use 
     * the primitives of the family ::gmt_execute_on_node(), 
     * ::gmt_execute_on_data() or ::gmt_execute_on_all();;
     *
     * @param[in] gmt_array GMT array used to locate where the tasks will start
     * @param[in] elems_per_task number elements each task is going to work on.
     * @param[in] function body of type ::gmt_for_each_func_t.
     * @param[in] args arguments data structure pointer.
     * @param[in] args_bytes size in bytes of the arguments data structure.
     * @param[in] handle ::gmt_handle_t used only by 
     *            ::gmt_for_each_with_handle().
     *
     * @ingroup GMT_module
     */
    void gmt_for_each(gmt_data_t gmt_array, uint64_t elems_per_task,
        uint64_t elems_offset, uint64_t num_elems,
        gmt_for_each_func_t func, const void *args,
        uint64_t args_bytes);
    void gmt_for_each_nb(gmt_data_t gmt_array, uint64_t elems_per_task,
        uint64_t elems_offset, uint64_t num_elems,
        gmt_for_each_func_t func, const void *args,
        uint64_t args_bytes);
    void gmt_for_each_with_handle(gmt_data_t gmt_array,
        uint64_t elems_per_task, uint64_t elems_offset, uint64_t num_elems,
        gmt_for_each_func_t func, const void *args,
        uint64_t args_bytes, gmt_handle_t handle);
    //@}

    //@{
    /** 
     * GMT Parallel for loop primitive. Creates a number of tasks equal to 
     * num_it/step_it and tries to distribute them according to 
     * the spawn policy ::spawn_policy_t. 
     *
     * The ::gmt_for_loop() blocks the calling task until all the 
     * created tasks are not completed.
     *
     * The ::gmt_for_loop_nb() allows the calling task 
     * to continue execution and to wait later for the completion 
     * of all the created tasks by using ::gmt_wait_for_nb().
     *
     * The ::gmt_for_loop_with_handle() allows the calling task to terminate 
     * (and free the resources) before the created tasks complete. A handle is 
     * passed through the arguments to identify the sub-tree of tasks 
     * associated with the same handle. At the end, it is required to wait 
     * for the termination of the tasks associated with a given handle calling 
     * ::gmt_wait_handle(). A valid handle must be requested with
     * ::gmt_get_handle().  
     * 
     * WARNING: these primitives DO NOT GUARANTEE that the distribution policy 
     * will always be satisfied. In high-workload scenarios nodes might 
     * be full and the tasks will be executed locally. In case of 
     * unexpected local execution, the ::gmt_get_local_ptr() will 
     * return NULL (if the policy is not GMT_ALLOC_REPLICATE ).
     * When ::gmt_get_local_ptr() returns NULL remote data can only 
     * be accessed using ::gmt_put() or 
     * ::gmt_get() primitives. To execute on a specific node safely use 
     * the primitives of the family ::gmt_execute_on_node(), 
     * ::gmt_execute_on_data() or ::gmt_execute_on_all();;
     *
     * @param[in] num_it number of iterations in the loop.
     * @param[in] step_it number of iteration to be executed by each task.
     * @param[in] function body of a single loop iteration.
     * @param[in] args arguments data structure pointer.
     * @param[in] args_bytes size in bytes of the arguments data structure.
     *            ( maximum is gmt_max_args_per_task() ).
     * @param[in] policy ::spawn_policy_t.
     * @param[in] handle ::gmt_handle_t used only by 
     *            ::gmt_for_loop_with_handle().
     *
     * @ingroup GMT_module
     */
    void gmt_for_loop(uint64_t num_it, uint32_t step_it,
                      gmt_for_loop_func_t func, const void *args,
                      uint32_t args_bytes, spawn_policy_t policy);
    void gmt_for_loop_nb(uint64_t num_it, uint32_t step_it,
                         gmt_for_loop_func_t func, const void *args,
                         uint32_t args_bytes, spawn_policy_t policy);
    void gmt_for_loop_with_handle(uint64_t num_it, uint32_t step_it,
                                  gmt_for_loop_func_t func, const void *args,
                                  uint32_t args_bytes, spawn_policy_t policy,
                                  gmt_handle_t handle);

    void gmt_for_loop_on_node(
        uint32_t rnid, uint64_t num_it, uint32_t it_per_task,
        gmt_for_loop_func_t func, const void *args, uint32_t args_bytes);

    void gmt_for_loop_on_node_nb(
        uint32_t rnid, uint64_t num_it, uint32_t it_per_task,
        gmt_for_loop_func_t func, const void *args, uint32_t args_bytes);

    void gmt_for_loop_on_node_with_handle(
        uint32_t rnid, uint64_t num_it, uint32_t it_per_task,
        gmt_for_loop_func_t func, const void *args, uint32_t args_bytes,
        const gmt_handle_t handle);

    //@}

    /**
     * Wait the completion of gmt_for_loop_nb() and gmt_for_each_nb() 
     *
     * @ingroup GMT_module
     */
    void gmt_wait_for_nb();

    /**
     * Request a gmt_handle_t handle to be used with 
     * ::gmt_for_each_with_handle() and ::gmt_for_loop_with_handle()
     *
     * @return handle gmt_handle_t handle
     * @ingroup GMT_module
     */
    gmt_handle_t gmt_get_handle();

    /** 
     * Wait for completion of the tasks created with 
     * primitives associated with a given handle.
     *
     * @param[in] handle ::gmt_handle_t handle
     *
     * @ingroup GMT_module
     */
    void gmt_wait_handle(gmt_handle_t handle);

    /**************************************************************************/
    /*                                                                        */
    /*                               EXECUTE                                  */
    /*                                                                        */
    /**************************************************************************/

    //@{
    /** 
     * Executes a single function 'func' in the node 'node_id'.
     * The gmt_try_execute_on_node* returns false when the node 
     * cannot execute because is too busy.
     *
     * @param[in] node_id of the node where we want to function to be executed
     * @param[in] func body of the function that we want to execute
     * @param[in] args arguments data structure pointer passed to the function
     * @param[in] args_bytes size in bytes of the arguments data structure 
     *            ( max is gmt_max_args_per_task()) 
     * @param[in] ret_buf return buffer which must be pre-allocated 
     * @param[out] ret_size when the operation completes it points to the 
     *    number of bytes written into the return buffer.
     *    If ret_size != NULL then ret_buf is expected != NULL.
     * @param[in] preempt_policy task preemption policy.
     * @param[in] handle ::gmt_handle_t handle
     * @returns false if the node cannot execute the task
     *
     * @ingroup GMT_module
     */
    void gmt_execute_on_node_with_handle(uint32_t node_id,
        gmt_execute_func_t func,
        const void *args, uint32_t args_bytes,
        void *ret_buf, uint32_t * ret_size,
        preempt_policy_t policy,
        gmt_handle_t handle);

    void gmt_execute_on_node(uint32_t node_id, gmt_execute_func_t func,
        const void *args, uint32_t args_bytes,
        void *ret_buf, uint32_t * ret_size,
        preempt_policy_t policy);

    void gmt_execute_on_node_nb(uint32_t node_id, gmt_execute_func_t func,
        const void *args, uint32_t args_bytes,
        void *ret_buf, uint32_t * ret_size,
        preempt_policy_t policy);

    bool gmt_try_execute_on_node_with_handle(uint32_t node_id,
        gmt_execute_func_t func,
        const void *args, uint32_t args_bytes,
        void *ret_buf, uint32_t * ret_size,
        preempt_policy_t policy,
        gmt_handle_t handle)
      __attribute_warn_unused_result__;

    bool gmt_try_execute_on_node(uint32_t node_id, gmt_execute_func_t func,
        const void *args, uint32_t args_bytes,
        void *ret_buf, uint32_t * ret_size,
        preempt_policy_t policy)
      __attribute_warn_unused_result__;

    bool gmt_try_execute_on_node_nb(uint32_t node_id, gmt_execute_func_t func,
        const void *args, uint32_t args_bytes,
        void *ret_buf, uint32_t * ret_size,
        preempt_policy_t policy)
      __attribute_warn_unused_result__;

    //@{
    /** 
     * Executes a single function 'func' in each and every node. The primitive 
     * will not return until each node is ready to execute the task.
     * 
     * @param[in] func body of the function that we want to execute
     * @param[in] args arguments data structure pointer passed to the function
     * @param[in] args_bytes size in bytes of the arguments data structure 
     *            ( max is gmt_max_args_per_task()) 
     * @param[in] preempt_policy task preemption policy.
     *
     * @ingroup GMT_module
     */
     void gmt_execute_on_all(gmt_execute_func_t func,
                             const void *args, uint32_t args_bytes,
                             preempt_policy_t policy);

     void gmt_execute_on_all_with_handle(gmt_execute_func_t func,
                             const void *args, uint32_t args_bytes,
                             preempt_policy_t policy, gmt_handle_t handle);

     void gmt_execute_on_all_nb(gmt_execute_func_t func,
                             const void *args, uint32_t args_bytes,
                             preempt_policy_t policy);

    //@} 

    //@{
    /** 
     * Executes a single function 'func' in the node that owns the element at
     * 'elem_offset' of the gmt_data_t gmt_array. 
     * We wait for the completion of the non-blocking version * '_nb' with 
     * ::gmt_wait_execute_nb(). The gmt_try_execute_* returns false when the 
     * remote node cannot execute because is too busy.
     *
     * @param[in] gmt_array GMT array used to locate where the task will start.
     * @param[in] elem_offset offset (in number of elements) used to locate
     *            where the task will start.
     * @param[in] func body of the function that we want to execute.
     * @param[in] args arguments data structure pointer passed to the function.
     * @param[in] args_bytes size in bytes of the arguments data structure.
     * @param[in] ret_buf return buffer, which must be pre-allocated.
     * @param[out] ret_size when the operation completes it points to 
     *    the number of bytes written into the return buffer. 
     *    If ret_size != NULL then ret_buf is expected != NULL.
     * @param[in] preempt_policy task preemption policy.
     * @returns false if the node cannot execute the task
     *
     * @ingroup GMT_module
     */
     void gmt_execute_on_data_with_handle(gmt_data_t gmt_array,
         uint64_t elem_offset,
         gmt_execute_func_t func,
         const void *args, uint32_t args_bytes,
         void *ret_buf, uint32_t * ret_size,
         preempt_policy_t policy,
         gmt_handle_t handle);

     void gmt_execute_on_data(gmt_data_t gmt_array, uint64_t elem_offset,
         gmt_execute_func_t func, const void *args,
         uint32_t args_bytes, void *ret_buf,
         uint32_t * ret_size, preempt_policy_t policy);

     void gmt_execute_on_data_nb(gmt_data_t gmt_array, uint64_t elem_offset,
         gmt_execute_func_t func, const void *args,
         uint32_t args_bytes, void *ret_buf,
         uint32_t * ret_size, preempt_policy_t policy);
 

     bool gmt_try_execute_on_data_with_handle(gmt_data_t gmt_array,
         uint64_t elem_offset,
         gmt_execute_func_t func,
         const void *args, uint32_t args_bytes,
         void *ret_buf, uint32_t * ret_size,
         preempt_policy_t policy,
         gmt_handle_t handle)
       __attribute_warn_unused_result__;

     bool gmt_try_execute_on_data(gmt_data_t gmt_array, uint64_t elem_offset,
         gmt_execute_func_t func, const void *args,
         uint32_t args_bytes, void *ret_buf,
         uint32_t * ret_size, preempt_policy_t policy)
     __attribute_warn_unused_result__;

     bool gmt_try_execute_on_data_nb(gmt_data_t gmt_array,
         uint64_t elem_offset,
         gmt_execute_func_t func, const void *args,
         uint32_t args_bytes, void *ret_buf,
         uint32_t * ret_size, preempt_policy_t policy)
       __attribute_warn_unused_result__;
 
    //@}
    //o
    /**
     * Wait the completion of ::gmt_execute_on_node_nb() and 
     * ::gmt_execute_on_data_nb() 
     * @ingroup GMT_module
     */
    void gmt_wait_execute_nb();

    /**
     * Print the stack trace for debugging purposes
     */
    void gmt_print_stack_trace();

    /**
     * Release the worker (CPU) to schedule the next runnable task.
     *
     * @ingroup GMT_module
     */
    void gmt_yield();


    /**
     * Counts the number of elements allocated in the local node
     * @returns the number of local elements
     */
    uint64_t gmt_count_local_elements(gmt_data_t gmt_array);

    //@{ 
    /**
     * Print, Print&reset or Reset timing and profile statistics of GMT 
     *
     * @param[in]  flag :  0 = print ; 1 = print and reset ; -1 = reset
     */

    void gmt_timing(int flag);
    void gmt_profile(int flag);
    //@}

#if defined(__cplusplus)
}
#endif

#ifdef GMT_INLINE
#undef GMT_INLINE
#define GMT_INLINE static inline

#include "gmt_for.c"
#include "gmt_execute.c"
#include "gmt_malloc.c"
#include "gmt_misc.c"
#include "gmt_put_get.c"

#else
#define GMT_INLINE
#endif

/* @endcond */

#endif
