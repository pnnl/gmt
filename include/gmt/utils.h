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

#ifndef __UTILS_H__
#define __UTILS_H__

/* This file is a generic utils file that together with utils.c should
   be possible to compile standalone without knowing anything about the project
   where it was located */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/syscall.h>
#include <sys/resource.h>
#include <string.h>
#include <errno.h>


#define ntohll(x) (((int64_t)(ntohl((int32_t)(((x) << 32) >> 32))) << 32) | \
                           (uint32_t)ntohl(((int32_t)((x) >> 32))))
#define htonll(x) ntohll(x)

#define	MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define CEILING(x,y) (((x) + (y) - 1) / (y))
#define FROM_32_TO_64(LOW,HIGH) (((uint64_t)LOW  & 0xffffffffUL) \
        | (((uint64_t)HIGH & 0xffffffffUL) << 32))

#define upper_bits( N, X )  ( X / N )
#define lower_bits( N, X )  ( X & ( N - 1 ) )
#define IS_PO2(x)  ((x != 0) && ((x & (~x + 1)) == x))

/* macro sequence to round to next power of two */
#define power2(x)    (   (x) | (   (x) >> 1) )
#define power4(x)    ( power2(x) | ( power2(x) >> 2) )
#define power8(x)    ( power4(x) | ( power4(x) >> 4) )
#define power16(x)   ( power8(x) | ( power8(x) >> 8) )
#define power32(x)   (power16(x) | (power16(x) >>16) )
#define power64(x)   (power32(x) | (power32(x) >>32) )
#define NEXT_POW2(x) (power64((uint64_t)x-1) + 1)

#define __align(x) __attribute__((aligned(x)))


#if defined(__cplusplus)
extern "C" {
#endif
void* safe_malloc(size_t n, int line);
void* safe_calloc(size_t n, size_t s, int line);
double my_timer();
#if defined(__cplusplus)
}
#endif
int get_num_cores();
void get_shmem_bytes(long *total, long *used, long *avail);
void pin_thread(int core);
int select_core(int tid, int cores, int stride);
int arch_get_cpu();
void arch_set_cpu(int cpu);
uint64_t strtol_suffix(char *optarg);
void set_res_limits(uint64_t used);
void print_line(int num);

extern uint64_t _total_malloc;

#define _malloc(n)   safe_malloc((n), __LINE__)
#define _calloc(n,s) safe_calloc((n), s, __LINE__)

#if DISABLE_INLINE
#define UTIL_INLINE
#else
#define UTIL_INLINE extern inline
#endif

UTIL_INLINE unsigned long long rdtsc(void) __attribute__((no_instrument_function));
UTIL_INLINE unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc":"=a"(lo), "=d"(hi));
    return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

UTIL_INLINE void *arch_get_sp() __attribute__((no_instrument_function));
UTIL_INLINE void *arch_get_sp()
{
    void *ret;
 __asm("movq %%rsp,%0 ":"=r"(ret));
    return ret;
}

#endif
