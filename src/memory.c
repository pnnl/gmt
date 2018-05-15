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

#include <errno.h>
#include <stdlib.h>
#include <dirent.h>
#include <stdbool.h>
#include "gmt/memory.h"

memory_t mem;

static void create_state_dir(char *dir)
{
    struct stat s;
    int err = stat(dir, &s);
    if (err == -1 && errno == ENOENT)
        return;

    /* creating directory if it does not exist */
    char tmp[PATH_MAX];
    char cmd[PATH_MAX];

    sprintf(tmp, "%s/GMT_STATES/%s", dir, config.state_name);
    err = stat(tmp, &s);
    if (err == -1 && errno == ENOENT) {
        printf("Creating dir state\n");
        sprintf(cmd, "mkdir -p %s", tmp);
        if (system(cmd) == -1) {
            ERRORMSG("%s failed\n", cmd);
        }
    }
}

static void load_state(char *path, bool is_shm)
{
    char tmp[PATH_MAX];
    DIR *d;
    struct dirent *dir;
    sprintf(tmp, "%s/GMT_STATES/%s", path, config.state_name);
    d = opendir(tmp);
    if (d) {
        while ((dir = readdir(d)) != NULL) {
            if (strcmp(dir->d_name, ".") == 0 || strcmp(dir->d_name, "..") == 0)
                continue;

            char name[PATH_MAX];
            uint32_t node;
            sscanf(dir->d_name, "n%d-%s", &node, name);
            if (node != node_id)
                continue;

            gmt_data_t gdata = gmt_attach(name);
            if (gdata) {
                ERRORMSG("Restore failed GMT array with name \"%s\" "
                         "already exists in state \"%s\"", name,
                         config.state_name);
            }
            int flag = O_RDONLY;
            if (config.state_prot == (PROT_READ | PROT_WRITE))
                flag = O_RDWR;
            int fd;
            if (is_shm) {
                sprintf(tmp, "GMT_STATES/%s/%s", config.state_name, dir->d_name);
                fd = shm_open(tmp, flag, (S_IREAD | S_IWRITE));
            } else {
                sprintf(tmp, "%s/GMT_STATES/%s/%s", path, config.state_name,
                        dir->d_name);
                fd = open(tmp, flag, (S_IREAD | S_IWRITE));
            }
            if (fd == -1)
                ERRORMSG("ERROR opening GMT permanent array %s", tmp);
            gentry_t *h = mmap(0, sizeof(gentry_t), PROT_READ,
                                     MAP_SHARED | MAP_POPULATE, fd, 0);
            _assert(h != MAP_FAILED);
            flag = MAP_SHARED | MAP_FIXED;
            if (config.state_populate)
                flag = flag | MAP_POPULATE;

            gentry_t *ga = &mem.gentry[GD_GET_ID(h->gmt_array)];
            memcpy(ga, h, sizeof(gentry_t));
            munmap(h, sizeof(gentry_t));
            uint64_t nbytes = ga->nbytes_tot + sizeof(gentry_t);
            ga->data = mmap(ga->data, nbytes, config.state_prot, flag, fd, 0);
            if (ga->data == MAP_FAILED)
                ERRORMSG("ERROR map GMT permanent array");

            ga->data += sizeof(gentry_t);
            ga->name = _malloc(strlen(name) + 1);
            memcpy(ga->name, name, strlen(name) + 1);
            printf("node %d - RESTORE NAME:%s\n", node_id, ga->name);

            close(fd);
        }
        closedir(d);
    }
}

void mem_init()
{
    if (GMT_MAX_ALLOC_PER_NODE > (1l << (GD_ID_END_BIT - GD_ID_START_BIT)))
        ERRORMSG("GMT_MAX_ALLOC_PER_NODE too large to encode in gmt_data_t");
    if (num_nodes > (1l << (GD_NODE_END_BIT - GD_NODE_START_BIT)) ||
        num_nodes > (1l << (GD_SNODE_END_BIT - GD_SNODE_START_BIT)))
        ERRORMSG("num_nodes too large to encode in gmt_data_t");



    if (config.state_name[0] != '\0' && config.state_prot == (PROT_READ | PROT_WRITE)) {
        create_state_dir("/dev/shm/");
        create_state_dir(config.ssd_path);
        create_state_dir(config.disk_path);
    }

    /* create memory entry table and initialize to zero */
    uint32_t num = GMT_MAX_ALLOC_PER_NODE * num_nodes;
    mem.gentry = (gentry_t *) _calloc(num, sizeof(gentry_t));
    
    if (config.state_name[0] != '\0') {
        load_state("/dev/shm/", true);
        load_state(config.ssd_path, false);
        load_state(config.disk_path, false);
    }

    /* reinitialize the pool for allocations ids (omitting ids used 
     *during the restore of the state) */
    mem_id_pool_init(&mem.mem_id_pool);
    uint32_t i;
    for (i = 0; i < GMT_MAX_ALLOC_PER_NODE; i++) {
        gentry_t *ga = &mem.gentry[i];
        _assert(ga->nbytes_tot == 0);
        if (ga->nbytes_tot == 0)
            mem_id_pool_push(&mem.mem_id_pool, i);
    }
    mem.num_used_allocs = 0;
}

void mem_destroy()
{
    uint64_t unallocated_mem = 0;
    uint32_t i;
    uint32_t nentries = num_nodes * GMT_MAX_ALLOC_PER_NODE;
    for (i = 0; i < nentries; i++) {
        gentry_t *ga = &mem.gentry[i];
        if (ga->nbytes_tot > 0 && ga->is_tmp) {            
            unallocated_mem += ga->nbytes_tot;
            if (node_id == 0)
                printf("Warning node %d - GMT_ARRAY name=%s - gid=%d "
                "allocated at exit %ld bytes\n", node_id, ga->name, i, 
                ga->nbytes_tot);
            mem_free(ga->gmt_array);
        }
    }

    if (node_id == 0 && unallocated_mem != 0)
        printf
            ("GMT WARNING - %ld bytes of GMT non permanent allocated space"
             " are still allocated at exit!\n", unallocated_mem);

    free(mem.gentry);
    mem_id_pool_destroy(&mem.mem_id_pool);
}
