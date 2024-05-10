/*
 * Global Memory and Threading (GMT)
 *
 * Copyright © 2024, Battelle Memorial Institute
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

#include "gmt/thread_affinity.h"
#include "gmt/debug.h"

hwloc_cpuset_t * workers_cpuset_hwloc; // array of the computed cpusets for each worker (hwloc)
cpu_set_t * workers_cpuset; // array of the computed cpusets for each worker (as per sched.h of libc)

hwloc_cpuset_t * helpers_cpuset_hwloc; // array of the computed cpusets for each helper (hwloc)
cpu_set_t * helpers_cpuset; // array of the computed cpusets for each helper (as per sched.h of libc)

hwloc_cpuset_t communicator_cpuset_hwloc; // cpuset of the communicator server (hwloc)
cpu_set_t * communicator_cpuset; // cpuset of the communicator server (as per sched.h of libc)

uint32_t available_cores; // number of physical cores available in the system (does not count hyperthreading)
hwloc_topology_t topology; // topology of the current system
hwloc_cpuset_t current_process_cpuset_hwloc; // cpuset of the current proces (usually depends on the mpi binding)
cpu_set_t * current_process_cpuset; // cpuset of the current proces (usually depends on the mpi binding)

/*
    Obtain a map containing singlified cpusets of each hwloc_obj_osdev_type_e in the system.
    Currently:
      HWLOC_OBJ_OSDEV_BLOCK
          Operating system block device, or non-volatile memory device. For instance "sda" or "dax2.0" on Linux.  
      HWLOC_OBJ_OSDEV_GPU
          Operating system GPU device. For instance ":0.0" for a GL display, "card0" for a Linux DRM device.
      HWLOC_OBJ_OSDEV_NETWORK
          Operating system network device. For instance the "eth0" interface on Linux.
      HWLOC_OBJ_OSDEV_DMA
          Operating system dma engine device. For instance the "dma0chan0" DMA channel on Linux.
      HWLOC_OBJ_OSDEV_OPENFABRICS
          Operating system openfabrics device. For instance the "mlx4_0" InfiniBand HCA, "hfi1_0" Omni-Path interface, or "bxi0" Atos/Bull BXI HCA on Linux.
      HWLOC_OBJ_OSDEV_COPROC
          Operating system co-processor device. For instance "opencl0d0" for a OpenCL device, "cuda0" for a CUDA device.
*/
int singlified_osdev_cpuset(hwloc_obj_osdev_type_t type, hwloc_cpuset_t** array_cpuset, int* len) {
    int n = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_OS_DEVICE);
    if(n == -1 ){     // the selected HWLOC_OBJ_OS_DEVICE is at multiple depths in the topology of the system
        exit(-1);
    }

    int internal_len = 0;
    for (int i = 0; i < n; i++) {
        hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_OS_DEVICE, i);
        if(obj->attr->osdev.type == type){
            internal_len++;
        }
    }
    *len = internal_len;

    *array_cpuset = (hwloc_cpuset_t*) malloc(sizeof(hwloc_cpuset_t)*internal_len);
    int internal_index=0;
    for (int i = 0; i < n; i++) {
        hwloc_obj_t obj = hwloc_get_obj_by_type(topology, HWLOC_OBJ_OS_DEVICE, i);
        if(obj->attr->osdev.type == type){
            hwloc_obj_t ancestor = obj;
            while (ancestor->cpuset == NULL) {
                ancestor = ancestor->parent;
                if (ancestor == NULL){  // something bad happened, no NUMA for the current OS_DEVICE?
                    break;
                }
            }
            if (ancestor != NULL){ // if == NULL it means that something bad happended
                hwloc_cpuset_t cpy = hwloc_bitmap_dup(ancestor->cpuset);
                hwloc_bitmap_singlify_per_core(topology, cpy, 0); // 0 keeps the first PU in the core
                if(obj->subtype != NULL && !strcmp(obj->subtype, "Slingshot")){ // if we are doing NET devices we have to prefere Slingshot
                    internal_len=1;
                    free(*array_cpuset);
                    *array_cpuset = (hwloc_cpuset_t*) malloc(sizeof(hwloc_cpuset_t));
                    *len=1;
                    (*array_cpuset)[0] = cpy;
                    return 0;
                }
                (*array_cpuset)[internal_index] = cpy;
                internal_index++;
            }
        }
    }

    return 0;
}

/*
    returns the cpuset of the core that is near the network interfaces (if multiple avaialble, it uses the least significant bit)
    it checks first for openfabric OS_DEV than it will fallback to NETWORK cards
    this function is used to assign the nearest (wrt the network interface) core to be assigned to the comm_server
    if no interface card is found between the available cores it falls back with assigning the most significant bit to the comm_server
    if multiple cards are available in the avaiable_cores_cpuset the first one matching is returned
*/
hwloc_cpuset_t near_network(hwloc_cpuset_t available_cores_cpuset){
    if(hwloc_bitmap_iszero(available_cores_cpuset)){
        exit(-1);
    }

    hwloc_cpuset_t output = hwloc_bitmap_alloc();
    int len;
  
    //  check for HWLOC_OBJ_OSDEV_OPENFABRICS objects
    hwloc_cpuset_t* openfabrics_map;
    singlified_osdev_cpuset(HWLOC_OBJ_OSDEV_OPENFABRICS, &openfabrics_map, &len);
    for(int i=0; i<len; i++){
        if(hwloc_bitmap_iszero(openfabrics_map[i])){
            continue;
        }
        if(hwloc_bitmap_intersects(available_cores_cpuset, openfabrics_map[i]) == 1){
            // the input_cpuset intersects with the openfabric OS_DEVICE
            hwloc_bitmap_and(output, available_cores_cpuset, openfabrics_map[i]);
            int first_core = hwloc_bitmap_first(output);
            hwloc_bitmap_only(output, first_core);
            return output;
        }
    }


    // check for HWLOC_OBJ_OSDEV_NETWORK objects
    hwloc_cpuset_t* network_map;
    singlified_osdev_cpuset(HWLOC_OBJ_OSDEV_NETWORK, &network_map, &len);
    for(int i=0; i<len; i++){
        if(hwloc_bitmap_iszero(network_map[i])){
            continue;
        }
        if(hwloc_bitmap_intersects(available_cores_cpuset, network_map[i]) == 1){
            // the input_cpuset intersects with the network OS_DEVICE
            hwloc_bitmap_and(output, available_cores_cpuset, network_map[i]);
            int first_core = hwloc_bitmap_first(output);
            hwloc_bitmap_only(output, first_core);
            return output;
        }
    }


    // NO MATCHED NET INTERFACES ==> fall back to last core in the cpuset
    int last_core = hwloc_bitmap_last(available_cores_cpuset);
    hwloc_bitmap_only(output, last_core );
    return output;
}

/*
    This function initializes the hwloc context, obtaining the topology of the system and setting the PID of the topology.
    It sets the "available_cores" variable that is used within config.h
*/
void explore_architecture(){
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);                                                             

    current_process_cpuset_hwloc = hwloc_bitmap_alloc();
    int error = hwloc_get_cpubind(topology, current_process_cpuset_hwloc, HWLOC_CPUBIND_PROCESS);
    if(error == -1){
        // failed to retrive cpubind
        printf("FAILED TO RETRIEVE CURRENT PROCESS CPUBIND\n");
        exit(-1);
    }
    
    hwloc_bitmap_singlify_per_core(topology, current_process_cpuset_hwloc, 0); // remove multiple PUs from the cores in the cpuset (keeps the first)

    // clone the cpuset in glibc sched affinity format 
    current_process_cpuset = (cpu_set_t*) malloc(sizeof(cpu_set_t));                      
    hwloc_cpuset_to_glibc_sched_affinity(topology, current_process_cpuset_hwloc, current_process_cpuset, sizeof(cpu_set_t));

    int weight = hwloc_bitmap_weight(current_process_cpuset_hwloc);
    if(weight < 0){
        // infinitely set bitmap (e.g., mpi bind-to none) => get the entire board
        hwloc_const_bitmap_t full = hwloc_topology_get_topology_cpuset(topology);
        hwloc_bitmap_t tmp = hwloc_bitmap_dup(full);
        hwloc_bitmap_singlify_per_core(topology, tmp, 0);                       
        weight = hwloc_bitmap_weight(tmp);
    }
    available_cores = (uint32_t) weight;

    // needed to detect the network devices
    hwloc_topology_init(&topology);
    hwloc_topology_set_io_types_filter(topology, HWLOC_TYPE_FILTER_KEEP_ALL);
    hwloc_topology_load(topology);        
}

uint32_t get_num_cores_hwloc(){
    return available_cores;
}

/*
    This function populates the following variables containing the different masks (cpusets) for the different cores:
    -   workers_cpuset[_hwloc]
    -   helpers_cpuset[_hwloc]
    -   communicator_cpuset[_hwloc]
*/
void affinity_masks_init(){
    if(config.num_cores  < config.num_workers + config.num_helpers + 1 || config.num_cores < 3 ){ // not enough cores for gmt (1 worker + 1 helper + 1 comm_server)
        printf("ERROR: The current configuration is not valid. Workers=%d, Helpers=%d, Comm_server=1 but available_cores=%d\n", config.num_workers, config.num_helpers, available_cores);
        exit(-1);  // not enough cores
    }

    // alloc space for the cpu_set and hwloc_cpuset for each worker/helper/comm_server
    workers_cpuset_hwloc = (hwloc_cpuset_t*) malloc(sizeof(hwloc_cpuset_t) * config.num_workers);
    workers_cpuset = (cpu_set_t*) malloc(sizeof(cpu_set_t) * config.num_workers );
    helpers_cpuset_hwloc = (hwloc_cpuset_t*) malloc(sizeof(hwloc_cpuset_t) * config.num_helpers);
    helpers_cpuset = (cpu_set_t*) malloc(sizeof(cpu_set_t) * config.num_helpers );
    communicator_cpuset = (cpu_set_t*) malloc(sizeof(cpu_set_t) * 1);

    /* 
        to print the current setup
        those cpusets store the list of cores used by workers/helpers/comm_server inside a single cpuset
        i.e. if we have two workers and those two workers are pinned to a single core
        the "complete_workers" is a mask containing the two cores
    */
    hwloc_cpuset_t complete_workers;
    hwloc_cpuset_t complete_helpers; 
    hwloc_cpuset_t complete_wasted;
    
    if(config.affinity_policy_id == NO_SMT_POLICY){
        // helpers/workers/comm_server will all point to the same cpuset (i.e. the process cpuset removed of SMT cores)
        communicator_cpuset_hwloc = hwloc_bitmap_dup(current_process_cpuset_hwloc);
        hwloc_cpuset_to_glibc_sched_affinity(topology, communicator_cpuset_hwloc, communicator_cpuset, sizeof(cpu_set_t));
        
        for(int i=0; i<config.num_workers; i++){
            workers_cpuset_hwloc[i] = hwloc_bitmap_dup(current_process_cpuset_hwloc);
            hwloc_cpuset_to_glibc_sched_affinity(topology, workers_cpuset_hwloc[i], &workers_cpuset[i], sizeof(cpu_set_t));
        }

        for(int i=0; i<config.num_helpers; i++){
            helpers_cpuset_hwloc[i] = hwloc_bitmap_dup(current_process_cpuset_hwloc);
            hwloc_cpuset_to_glibc_sched_affinity(topology, helpers_cpuset_hwloc[i], &helpers_cpuset[i], sizeof(cpu_set_t));
        }

        // workers and helpers will use all the cores in the process binding, no wasted cores (we are not pinning)
        complete_workers = hwloc_bitmap_dup(current_process_cpuset_hwloc);
        complete_helpers = hwloc_bitmap_dup(current_process_cpuset_hwloc);
        complete_wasted = hwloc_bitmap_alloc();
        hwloc_bitmap_zero(complete_wasted);

    }else if(config.affinity_policy_id == PIN_POLICY){
        // Compute the optimal position of the communication server i.e. near the fastest network interface
        communicator_cpuset_hwloc = near_network(current_process_cpuset_hwloc);
        hwloc_cpuset_to_glibc_sched_affinity(topology, communicator_cpuset_hwloc, communicator_cpuset, sizeof(cpu_set_t));

        // compute the remaining cpuset after assining the communicator server
        auto remaining_cpuset_hwloc = hwloc_bitmap_alloc();
        hwloc_bitmap_andnot(remaining_cpuset_hwloc, current_process_cpuset_hwloc, communicator_cpuset_hwloc);

        unsigned int id;
        int array_index=0;
        uint32_t assigned=0;
        complete_workers = hwloc_bitmap_alloc();
        complete_helpers = hwloc_bitmap_alloc();
        complete_wasted = hwloc_bitmap_alloc();

        hwloc_bitmap_foreach_begin(id, remaining_cpuset_hwloc) // iterate over the remaining cores inside the cpuset
            if(assigned < config.num_workers){ // here we are working with the workers
                workers_cpuset_hwloc[array_index] = hwloc_bitmap_alloc();
                hwloc_bitmap_only(workers_cpuset_hwloc[array_index], id);
                hwloc_bitmap_set(complete_workers, id);
                hwloc_cpuset_to_glibc_sched_affinity(topology, workers_cpuset_hwloc[array_index], &workers_cpuset[array_index], sizeof(cpu_set_t));
                array_index++;
                assigned++;
            }else if(assigned >= config.num_workers && assigned < config.num_workers+config.num_helpers  )  { // here we are placing the helpers
                if(assigned == config.num_workers){
                    array_index=0;
                }
                helpers_cpuset_hwloc[array_index] = hwloc_bitmap_alloc();
                hwloc_bitmap_only(helpers_cpuset_hwloc[array_index], id);
                hwloc_bitmap_set(complete_helpers, id);
                hwloc_cpuset_to_glibc_sched_affinity(topology, helpers_cpuset_hwloc[array_index], &helpers_cpuset[array_index], sizeof(cpu_set_t));
                array_index++;
                assigned++;
            }else{ // those are unutilized cores if the user does manually specify the number of workers/helpers for some reason
                hwloc_bitmap_set(complete_wasted, id);
                assigned++;
                array_index++;  
            }
        hwloc_bitmap_foreach_end();

    }else{
        printf("ERROR: Trying to create cpumasks for unknown affinity policy");
        exit(-1);
    }

    // print the computed configuration
    char * core_comm_server, *cores_workers, *cores_helpers, *cores_wasted, *cores_currentproc;
    hwloc_bitmap_list_asprintf(&core_comm_server, communicator_cpuset_hwloc);
    hwloc_bitmap_list_asprintf(&cores_currentproc, current_process_cpuset_hwloc);
    hwloc_bitmap_list_asprintf(&cores_wasted, complete_wasted);
    hwloc_bitmap_list_asprintf(&cores_workers, complete_workers);
    hwloc_bitmap_list_asprintf(&cores_helpers, complete_helpers);
    printf("### Node[%d]: process: %s | comm_server: %s | workers: %s | helpers: %s | wasted: %s \n", 
                node_id,
                cores_currentproc,
                core_comm_server,
                cores_workers,
                cores_helpers,
                cores_wasted
    );  
}

/*
    This function sets the affinity mask of the provided thread to a pre-computed value depending on the selected affinity policy.
    It works both for the workers and helpers (using the pre-defined arrays workers_cpuset and helpers_cpuset)
*/
void set_thread_affinity(uint32_t tid)
{        
    // this function uses the TID given by get_thread_id() therefore values of TID are in the range of 0 to NUM_CORES
    if(tid >= available_cores){
        DEBUG0(printf("#   num threads exceeds the number of cores\n"););
        exit(-1);
    }

    char* cpuset;
    if(tid < config.num_workers){ // this is a worker
        if(node_id == 0){
            hwloc_bitmap_list_asprintf(&cpuset, workers_cpuset_hwloc[tid]);
            DEBUG0(printf("#   Node[%d]: PIN [get_thread_id: %d, gettid:%d, pthread_id:%lu] TO CORE %s (WORKER) \n", node_id, tid, sys_tid, pthread_self(), cpuset););
        }

        if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &workers_cpuset[tid])) {
            printf("Error setting affinity\n");
        }
    }else{ // this is a helper
        int relative_tid = (tid - config.num_workers); // get the relative tid in the helpers cpusets
        if(node_id == 0){
            hwloc_bitmap_list_asprintf(&cpuset, helpers_cpuset_hwloc[relative_tid]);
            DEBUG0(printf("#   Node[%d]: PIN [get_thread_id: %d, gettid:%d, pthread_id:%lu] TO CORE %s (HELPER) \n", node_id, tid, sys_tid, pthread_self(), cpuset););
        }
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &helpers_cpuset[relative_tid])) {
            printf("Error setting affinity\n");
        }
    }
}

/* 
    This function is used to pin the communication server to the core closest to the network interface (previously computed).
*/
void comm_server_set_thread_affinity(){
    if(node_id==0){
        char* cpuset;
        hwloc_bitmap_list_asprintf(&cpuset, communicator_cpuset_hwloc);
        DEBUG0(printf("#   Node[%d]: PIN [get_thread_id: %d, gettid:%d, pthread_id:%lu] TO CORE %s (COMM_SERVER) \n", node_id, get_thread_id(), syscall(SYS_gettid), pthread_self(), cpuset););
    }

    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), communicator_cpuset)) {
        printf("Error setting affinity\n");
    }
}
