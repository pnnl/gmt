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


#ifndef __THREAD_AFFINITY_H__
#define __THREAD_AFFINITY_H__

#include <sys/syscall.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <hwloc.h>
#include <hwloc/glibc-sched.h>
#include "gmt/config.h"


#define LEGACY_PIN_POLICY 0 // pin threads based on a linear enumeration of the cores (be aware of strided numbering scheme on recent CPU architectures)
#define NO_SMT_POLICY 1     // disable Simultaneous Multithreading in the cores of the current process binding
#define PIN_POLICY 2        // pins the threads to a specific core in the current process binding (positioning the comm_server near the network card)

// cpuset of the workers
extern hwloc_cpuset_t * workers_cpuset_hwloc;
extern cpu_set_t * workers_cpuset;

// cpuset of the helpers
extern hwloc_cpuset_t * helpers_cpuset_hwloc;
extern cpu_set_t * helpers_cpuset;

// cpuset of the communicator
extern hwloc_cpuset_t communicator_cpuset_hwloc;
extern cpu_set_t * communicator_cpuset;

extern uint32_t available_cores; // number of available cores seen by the process binding
extern hwloc_topology_t topology; // the topology of the current system seen by the process

extern cpu_set_t * current_process_cpuset; // binding of the current process (bitmap)
extern hwloc_cpuset_t current_process_cpuset_hwloc; 

void set_thread_affinity(uint32_t tid);
void affinity_masks_init();
void explore_architecture();
int singlified_osdev_cpuset(hwloc_obj_osdev_type_t type, hwloc_cpuset_t** array_cpuset, int* len);
hwloc_cpuset_t near_network(hwloc_cpuset_t input_cpuset);
void comm_server_set_thread_affinity();
uint32_t get_num_cores_hwloc();

#endif
