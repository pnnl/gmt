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

#include "gmt/utils.h"

uint64_t _total_malloc = 0;

void *safe_malloc(size_t n, int line)
{
    void *p = malloc(n);
    if (p == NULL) {
        fprintf(stderr, "[%s:%ul]Out of memory(%ld bytes)\n",
                __FILE__, line, (unsigned long)n);
        fflush(stderr);
        fflush(stdout);
        _exit(EXIT_FAILURE);
    }
    _total_malloc += n;
    return p;
}

void *safe_calloc(size_t n, size_t s, int line)
{
    void *p = calloc(n,s);
    if (p == NULL) {
        fprintf(stderr, "[%s:%ul]Out of memory(%ld bytes)\n",
                __FILE__, line, (unsigned long)n);
        fflush(stderr);
        fflush(stdout);
        _exit(EXIT_FAILURE);
    }
    _total_malloc += n;
    return p;
}



void print_line(int n)
{
    int i;
    for (i = 0; i < n; i++)
        putchar('-');
    putchar('\n');
}

uint64_t strtol_suffix(char *optarg)
{
    char *suffix;

    /* hexadecimal number */
    if (*optarg == '0' || *optarg == 'x' || *optarg == 'X') {
        uint64_t val = strtol(optarg, &suffix, 16);
        if (*suffix != '\0') {
            printf("\nERROR: (k,K,m,M,g,G,t, or T) postfix or (0x,x or X) "
                   "prefix: %c\n\n", *suffix);
            return -1;
        } else
            return val;
        return (uint64_t) strtol(optarg, NULL, 16);
    }
    uint64_t value = strtol(optarg, &suffix, 10);
    if (*suffix) {
        switch (*suffix) {
        case 'k':
            value *= ((uint64_t) 1000);
            break;
        case 'K':
            value *= ((uint64_t) 1024);
            break;
        case 'm':
            value *= ((uint64_t) 1000 * 1000);
            break;
        case 'M':
            value *= ((uint64_t) 1024 * 1024);
            break;
        case 'g':
            value *= ((uint64_t) 1000 * 1000 * 1000);
            break;
        case 'G':
            value *= ((uint64_t) 1024 * 1024 * 1024);
            break;
        case 't':
            value *= ((uint64_t) 1000 * 1000 * 1000 * 1000);
            break;
        case 'T':
            value *= ((uint64_t) 1024 * 1024 * 1024 * 1024);
            break;
        default:
            printf("\nERROR: (k,K,m,M,g,G,t, or T) postfix or (0x,x or X) "
                   "prefix: %c\n\n", *suffix);
            return (uint64_t) - 1;
        }
    }
    return value;
}

double my_timer(void)
{
    struct timeval tv;
    struct timezone tz;
    gettimeofday(&tv, &tz);
    return ((double)tv.tv_sec + (double)0.000001 * (double)tv.tv_usec);
}

int get_num_cores()
{
    char *data;
    FILE *fp;
    int sz = 1024;

    fp = popen((const char *)"grep processor /proc/cpuinfo |"
               " awk '{print $3}' | tail -n 1", "r");
    if (fp == NULL) {
        printf("command not executed\n");
        exit(1);
    }

    data = (char *)malloc(sizeof(char) * (sz + 1));
    data = fgets(data, sz, fp);
    pclose(fp);
    int ret = atoi(data) + 1;
    free(data);
    return ret;
}

void get_shmem_bytes(long *total, long *used, long *avail)
{
    FILE *ls = popen("df -k -P | grep shm", "r");
    char buf[1024], *bptr;
    bptr = fgets(buf, sizeof(buf), ls);
    if (bptr == NULL) {
        printf("ERROR IN get_shmem_bytes function\n");
        exit(1);
    }
    char tmp[256];
    sscanf(buf, "%s %ld %ld %ld", tmp, total, used, avail);
    *total *= 1024;
    *used *= 1024;
    *avail *= 1024;
    pclose(ls);
}

int select_core(int tid, int cores, int stride)
{
    return (((tid * stride) % cores) + (int)(tid / (cores / stride))) % cores;
}

void pin_thread(int core)
{

    cpu_set_t cpu_mask;
    CPU_ZERO(&cpu_mask);
    CPU_SET(core, &cpu_mask);

    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_mask), &cpu_mask)) {
        printf("Error setting affinity\n");
    }
}

int arch_get_cpu()
{
    int i;
    cpu_set_t mask;
    sched_getaffinity(syscall(SYS_gettid), sizeof(cpu_set_t), &mask);

    for (i = 0; i < CPU_SETSIZE; i++) {
        if (CPU_ISSET(i, &mask)) {
            return i;
        }
    }
    return -1;
}

void arch_set_cpu(int cpu)
{
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    sched_setaffinity(syscall(SYS_gettid), sizeof(cpu_set_t), &mask);
}

/* set virtual address soft limit */
void set_res_limits(uint64_t used)
{
    struct rlimit rlim;
    if (getrlimit(RLIMIT_AS, &rlim) != 0) {
        printf("%s error in getrlimit(): %s", __func__, strerror(errno));
        exit(1);
    }

    if ((long long)rlim.rlim_max != -1 &&
        ((long long)rlim.rlim_max) < (long long)used) {
        printf
            ("%s error: virtual address space limit of %lub while %lub needed."
             "( check \"ulimit -a\" or \"cat /proc/PID/status\") .\n", __func__,
             rlim.rlim_max, used);
        exit(1);
    }

    if (rlim.rlim_cur != rlim.rlim_max)
        rlim.rlim_cur = rlim.rlim_max;
    if (setrlimit(RLIMIT_AS, &rlim) != 0) {
        printf("%s error in setrlimit(): %s", __func__, strerror(errno));
        exit(1);
    }
}
