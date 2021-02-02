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

#ifndef __MEMORY_H__
#define __MEMORY_H__

#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "gmt/config.h"
#include "gmt/debug.h"
#include "gmt/utils.h"
#include "gmt/timing.h"
#include "gmt/queue.h"
#include "gmt/network.h"
#include "gmt/gmt.h"

/*
 * workaround for old (or non-) Linux platforms
 */
#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif


/************************ MACROS FOR GMT_DATA_T ****************************/
/* set and get the local gmt_array id */
#define GD_ID_START_BIT      0
#define GD_ID_END_BIT        20
#define GD_ID_MASK           ((( 1l << GD_ID_END_BIT) - 1) - (( 1l << GD_ID_START_BIT) - 1))
#define GD_SET_ID(g,i)       ((g) = ((g) & ~GD_ID_MASK) | ((((uint64_t) (i)) << GD_ID_START_BIT) & GD_ID_MASK))
#define GD_GET_ID(g)         (uint32_t) (((g) & GD_ID_MASK) >> GD_ID_START_BIT)

/* set and get id of the node that performed the allocation for this gmt_array */
#define GD_NODE_START_BIT    20
#define GD_NODE_END_BIT      30
#define GD_NODE_MASK         ((( 1l << GD_NODE_END_BIT) - 1) - (( 1l << GD_NODE_START_BIT) - 1))
#define GD_SET_NODE(g,i)     ((g) = ((g) & ~GD_NODE_MASK) | ((((uint64_t) (i)) << GD_NODE_START_BIT) & GD_NODE_MASK))
#define GD_GET_NODE(g)       (uint32_t) (((g) & GD_NODE_MASK) >> GD_NODE_START_BIT)

/* this macro gets the global allocation id combining gmt_array id and 
 * node id that performed the allocation  */
#define GD_GET_GID(g)       (GD_GET_NODE(g) * (GMT_MAX_ALLOC_PER_NODE) + GD_GET_ID(g))

#define GD_TYPE_START_BIT    30
#define GD_TYPE_END_BIT      36
#define GD_TYPE_MASK         ((( 1l << GD_TYPE_END_BIT) - 1) - (( 1l << GD_TYPE_START_BIT) - 1))
#define GD_SET_TYPE(g,i)     ((g) = ((g) & ~GD_TYPE_MASK) | ((((uint64_t) (i)) << GD_TYPE_START_BIT) & GD_TYPE_MASK))
#define GD_GET_TYPE(g)       (uint32_t) (((g) & GD_TYPE_MASK) >> GD_TYPE_START_BIT)
/* distribution policy - subsection of type (3 bits)*/
#define GD_GET_TYPE_DISTR(g) ((GD_GET_TYPE(g)) &  ((1l << 3) -1))
/* array is zero on allocated - subsection of type (1 bit) */
#define GD_GET_TYPE_ZERO(g)  (((GD_GET_TYPE(g)) >> 3) & 1l)
/* array permanence - subsection of type (2 bits) */
#define GD_TYPE_MEDIA_MASK    (((1l << 6) - 1) - ((1l << 4) - 1))
#define GD_GET_TYPE_MEDIA(g)  ((GD_GET_TYPE(g)) & GD_TYPE_MEDIA_MASK)

/* starting node where offset "0" of this allocation is located */
#define GD_SNODE_START_BIT    36
#define GD_SNODE_END_BIT      46
#define GD_SNODE_MASK         ((( 1l << GD_SNODE_END_BIT) - 1) - (( 1l << GD_SNODE_START_BIT) - 1))
#define GD_SET_SNODE(g,i)     ((g) = ((g) & ~GD_SNODE_MASK) | ((((uint64_t) (i)) << GD_SNODE_START_BIT) & GD_SNODE_MASK))
#define GD_GET_SNODE(g)       (uint32_t) (((g) & GD_SNODE_MASK) >> GD_SNODE_START_BIT)
/*****************************************************************************/

#define GMT_NO_LOCAL_DATA       INT64_MAX
#define GMT_FILE_BLOCK_SIZE     (1024*1024)

DEFINE_QUEUE_MPMC(mem_id_pool, uint64_t, GMT_MAX_ALLOC_PER_NODE);

typedef struct gentry_t {
    /* number of bytes total for this gmt_array */
    uint64_t nbytes_tot;
    /* number of bytes on this node for this gmt_array */
    uint64_t nbytes_loc;
    /* number of bytes of each block partition 
     *(all the nodes have the same value) */
    uint64_t nbytes_block;
    /* global offset of this gmt_array owned by this node */
    uint64_t goffset_bytes;
    /* number of bytes of each element */
    uint64_t nbytes_elem;
    /* pointer to the actual data in this node */
    uint8_t *data;
    /* gmt_data_t associated with this entry */
    gmt_data_t gmt_array;
    /* name of this gmt_array */
    char *name;
    /* flag used for allocations that are temporary, used for final cleaning */
    bool is_tmp;
} gentry_t;

typedef struct memory_t {
    mem_id_pool_t mem_id_pool;
    gentry_t *gentry;

    long shmem_size_total;
    long shmem_size_used;
    long shmem_size_avail;
    
    uint32_t num_used_allocs;
} memory_t;

extern memory_t mem;

void mem_init();
void mem_destroy();

INLINE uint32_t mem_get_alloc_id()
{
    uint64_t id = 0;
    
    if (__sync_add_and_fetch(&mem.num_used_allocs, 1) >=
        GMT_MAX_ALLOC_PER_NODE ) {
       ERRORMSG("maximum number of global allocations "
                 " supported reached - GMT_MAX_ALLOC_PER_NODE %d\n",
                 GMT_MAX_ALLOC_PER_NODE);

    }
    while (!mem_id_pool_pop(&mem.mem_id_pool, &id));        

    int gid = node_id * GMT_MAX_ALLOC_PER_NODE + id;
    _assert(mem.gentry[gid].nbytes_elem == 0);
    _unused(gid);
    return id;
}

/* this function either alloc or free a ga entry on a file (shm, ssd, disk)
  if alloc = true is an allocation otherwise a free */
INLINE void af_on_file(char *path, gentry_t * ga, bool is_alloc)
{
    char filename[PATH_MAX];
    int fd;
    /* this is a shared memory allocation */
    if (path == NULL) {
        /* because shared memory allocation without a state or a r/w permission
         * on a state or a name are handled as normal malloc, if we are here 
         * the following 3 _asserts must be true */
        _assert(ga->name != NULL);
        _assert(config.state_name[0] != '\0');
        _assert(config.state_prot == (PROT_READ | PROT_WRITE));
        sprintf(filename, "GMT_STATES/%s/n%d-%s", config.state_name, node_id,
                ga->name);

        if (is_alloc) {
            fd = shm_open(filename, (O_CREAT | O_EXCL | O_RDWR),
                          (S_IREAD | S_IWRITE));
            ga->is_tmp = false;
        } else
            fd = shm_unlink(filename);

    } else {
        /* here we are either on a DISK or SSD, in both cases we check if 
         * the path provided (by the user) actually exists */
        if (path[0] == '\0' || (access(path, F_OK) == -1))
            ERRORMSG("SSD/DISK path =>>%s<<= is null or does not exist\n",
                     path);
        sprintf(filename, "%s", path);

        /* we check if a state name is provided and if a name for this gmt_array
         * has been provided by the user. If yes we check for r/w permissions 
         * and generate the appropriate name for the final folder */
        bool on_state = false;
        if (config.state_name[0] != '\0' && ga->name != NULL) {
            if (config.state_prot != (PROT_READ | PROT_WRITE))
                ERRORMSG("no permissions to create/delete file from state");
            sprintf(filename, "%s/GMT_STATES/%s", filename, config.state_name);
            on_state = false;
        }

        /* if a name is not provided by the user (because we have to create 
         * a file anyway) we pick a __#gmt_array name. We also know this
         * is not going to end up in the state folder */
        if (ga->name == NULL)
            sprintf(filename, "%s/n%d-__%ld", filename, node_id, ga->gmt_array);
        else
            sprintf(filename, "%s/n%d-%s", filename, node_id, ga->name);

        if (is_alloc) {
            /* we open and create the file */
            fd = open(filename, (O_CREAT | O_RDWR), (S_IREAD | S_IWRITE));
            ga->is_tmp = !on_state;

        } else {
            char cmd[PATH_MAX];
            sprintf(cmd, "rm -f %s", filename);
            fd = system(cmd);
        }
    }
    if (fd == -1) {
        perror("ERROR:");
        ERRORMSG("Error creating/deleting array on file");
    }

    if (is_alloc) {
        /* we set the file size to the number of bytes to allocate, plus
         * a space for the memory_entry_t data. We will use the memory_entry_t
         * data when reopening this file for restore */
        uint64_t tbytes = ga->nbytes_loc + sizeof(gentry_t);
        /* this will always be like a calloc */
        if (ftruncate(fd, tbytes)) {
            perror("ERROR: ftruncate");
            ERRORMSG("ftruncate failed");
        }

        /* we map the file (either on disk, ssd or shm) to the ga->data, we copy
         * the memory_entry_t info in front and and move the ga->data pointer to
         * the first available byte for the "user" */
        ga->data = (uint8_t *) mmap(0, tbytes,
                                    PROT_READ | PROT_WRITE | PROT_EXEC,
                                    MAP_SHARED | MAP_POPULATE, fd, 0);
        if (ga->data == NULL)
            ERRORMSG("malloc() error - trying to allocate %ld GB tot\n",
                     tbytes / (1024 * 1024 * 1024l));
        _assert(ga->data != NULL);
        memcpy(ga->data, ga, sizeof(gentry_t));
        ga->data += sizeof(gentry_t);

        /* we close the file descriptor, because now the file is mappend 
         *with mmap */
        close(fd);
    }
}

INLINE void alloc_data(gentry_t * ga)
{
    if (ga->data != NULL)
        ERRORMSG("DATA is not null\n");

    if (GD_GET_TYPE_MEDIA(ga->gmt_array) == GMT_ALLOC_RAM ||
        /* if allocation is in SHM but there is not array name or there is not 
         * state name or there is not permission -> this is handled like a 
         * normal malloc */
        (GD_GET_TYPE_MEDIA(ga->gmt_array) == GMT_ALLOC_SHM
         && (ga->name == NULL || (config.state_prot != (PROT_READ | PROT_WRITE))
             || config.state_name[0] == '\0'))) {

        if (ga->nbytes_loc == 0)
            return;

        if (GD_GET_TYPE_ZERO(ga->gmt_array))
            ga->data = (uint8_t *) calloc(1, ga->nbytes_loc);
        else
            ga->data = (uint8_t *) malloc(ga->nbytes_loc);
        //_DEBUG("allocbytes %ld\n", ga->nbytes_loc);
        ga->is_tmp = true;
        if (ga->data == NULL)
            ERRORMSG("malloc() error - trying to allocate %ld bytes in RAM\n",
                     ga->nbytes_loc);
        return;
    }

    switch (GD_GET_TYPE_MEDIA(ga->gmt_array)) {
    case GMT_ALLOC_SHM:
        af_on_file(NULL, ga, true);
        break;
    case GMT_ALLOC_DISK:
        af_on_file(config.disk_path, ga, true);
        break;
    case GMT_ALLOC_SSD:
        af_on_file(config.ssd_path, ga, true);
        break;
    }
}

INLINE void block_partition(uint32_t nid, uint64_t num_elems,
                            uint64_t nbytes_elem, gmt_data_t gmt_array,
                            uint64_t * nbytes_loc,
                            uint64_t * nbytes_block, uint64_t * goffset_bytes)
{
    switch (GD_GET_TYPE_DISTR(gmt_array)) {
    case GMT_ALLOC_LOCAL:
        *nbytes_block = num_elems * nbytes_elem;
        if (nid == GD_GET_NODE(gmt_array))
            *nbytes_loc = num_elems * nbytes_elem;
        else
            *nbytes_loc = 0;
        *goffset_bytes = 0;
        break;
    case GMT_ALLOC_REPLICATE:
        *nbytes_block = num_elems * nbytes_elem;
        *nbytes_loc = num_elems * nbytes_elem;
        *goffset_bytes = 0;
        break;

    case GMT_ALLOC_PARTITION_FROM_ZERO:
    case GMT_ALLOC_PARTITION_FROM_HERE:
    case GMT_ALLOC_PARTITION_FROM_RANDOM:
        {
            int64_t blocks = num_elems / num_nodes;
            if (blocks * num_nodes != (int64_t) num_elems)
                blocks++;
            *nbytes_block = blocks * nbytes_elem;

            uint32_t start_node = GD_GET_SNODE(gmt_array);
            uint32_t node_id_off;
            if (nid >= start_node)
                node_id_off = nid - start_node;
            else
                node_id_off = (num_nodes - start_node + nid);

            int64_t blocks_loc = blocks;
            if ((node_id_off + 1) * blocks > (int64_t) num_elems)
                blocks_loc -= ((node_id_off + 1) * blocks) - num_elems;

            if (blocks_loc > 0) {
                *nbytes_loc = blocks_loc * nbytes_elem;
                *goffset_bytes = node_id_off * blocks * nbytes_elem;
                /* check that reserved value is not used */
                _assert(*goffset_bytes <= GMT_NO_LOCAL_DATA);
            } else {
                *nbytes_loc = 0;
                *goffset_bytes = GMT_NO_LOCAL_DATA;
            }
        }
        break;

    case GMT_ALLOC_REMOTE:
        /* if running with one node this is just like GMT_ALLOC_LOCAL */
        if (num_nodes == 1) {
            *nbytes_block = num_elems * nbytes_elem;
            *nbytes_loc = num_elems * nbytes_elem;
            *goffset_bytes = 0;
            /* check that reserved value is not used */
            _assert(*goffset_bytes <= GMT_NO_LOCAL_DATA);
        } else {
            int64_t blocks = num_elems / (num_nodes - 1);
            if (blocks * (num_nodes - 1) != (int64_t) num_elems)
                blocks++;
            *nbytes_block = blocks * nbytes_elem;

            if (nid != GD_GET_NODE(gmt_array)) {
                uint32_t node_id_off;
                if (nid < GD_GET_NODE(gmt_array))
                    node_id_off = nid;
                else
                    node_id_off = (nid - 1);

                int64_t blocks_loc = blocks;
                if ((node_id_off + 1) * blocks > (int64_t) num_elems)
                    blocks_loc -= ((node_id_off + 1) * blocks) - num_elems;

                if (blocks_loc > 0) {
                    *nbytes_loc = blocks_loc * nbytes_elem;
                    *goffset_bytes = node_id_off * blocks * nbytes_elem;
                    /* check that reserved value is not used */
                    _assert(*goffset_bytes <= GMT_NO_LOCAL_DATA);
                } else {
                    *nbytes_loc = 0;
                    *goffset_bytes = GMT_NO_LOCAL_DATA;
                }
            } else {
                *nbytes_loc = 0;
                *goffset_bytes = GMT_NO_LOCAL_DATA;
            }
        }
        break;

    case GMT_ALLOC_PERNODE:
    default:
        ERRORMSG("Allocation policy (%d) unknown!\n",
                 (int)GD_GET_TYPE_DISTR(gmt_array));
        break;
    }
//     _DEBUG("nbytes_loc %ld - nbytes_block %ld - goffset_bytes %ld\n",
//            *nbytes_loc, *nbytes_block, *goffset_bytes);
}

INLINE void mem_alloc(gmt_data_t gmt_array, uint64_t num_elems,
                      uint64_t nbytes_elem, const char *array_name,
                      int name_len)
{

    uint32_t gid = GD_GET_GID(gmt_array);
    _assert(gid < num_nodes * GMT_MAX_ALLOC_PER_NODE);
    gentry_t *ga = &mem.gentry[gid];

    _assert(ga->nbytes_tot == 0);

    ga->gmt_array = gmt_array;
    ga->nbytes_elem = nbytes_elem;

    if (array_name != NULL && name_len != 0) {
        /* if we have permission to modify the state check if name already 
         * exist */
        if (config.state_prot == (PROT_READ | PROT_WRITE) &&
            config.state_name[0] != '\0'){
            gmt_data_t gdata = gmt_attach(array_name);
            if (gdata != GMT_DATA_NULL)
                ERRORMSG("Allocation failed GMT array with name \"%s\" "
                         "already exists in state \"%s\"", array_name,
                         config.state_name);
        }

        /* allocate and assign name */
        ga->name = (char *)_malloc(name_len + 1);
        _assert(ga->name != NULL);
        memcpy(ga->name, array_name, name_len);
        ga->name[name_len] = '\0';
    } else {
        ga->name = NULL;
    }

    block_partition(node_id, num_elems, nbytes_elem, gmt_array,
                    &ga->nbytes_loc, &ga->nbytes_block, &ga->goffset_bytes);

    alloc_data(ga);

    /* this write will mark the ga entry available to 
     * memory_get_memory_entry */
    ga->nbytes_tot = num_elems * nbytes_elem;
}

INLINE gentry_t *mem_get_gentry(gmt_data_t gmt_array)
{
    if (gmt_array == GMT_DATA_NULL)
        ERRORMSG("Trying to access GMT_DATA_NULL\n");

    uint32_t gid = GD_GET_GID(gmt_array);
    _assert(gid < num_nodes * GMT_MAX_ALLOC_PER_NODE);

    gentry_t *ga = &mem.gentry[gid];
    _assert(ga != NULL);

    if(ga->nbytes_tot == 0) {
      ERRORMSG("Trying to access an array that is not currently allocated.\n");
    }

    return ga;
}

INLINE void mem_check_word_elem_size(gentry_t* ga) {
  if (ga->nbytes_elem != 8 &&
      ga->nbytes_elem != 4 &&
      ga->nbytes_elem != 2 &&
      ga->nbytes_elem != 1) {
        ERRORMSG("Cannot use gmt_put_value_*() or gmt_atomic_*() on an"
            " array with elements of sizes other than 8, 4, 2, or 1 bytes.\n");
    }
}

INLINE void mem_check_last_byte(gentry_t* ga, uint64_t last_byte) 
{
    if(ga->nbytes_tot == 0) {
      ERRORMSG("Trying to access an array that is not currently allocated.\n");
    }

    if (last_byte > 0 && last_byte > ga->nbytes_tot) {
        ERRORMSG("Trying to access byte %ld in GMT array %d -name:%s- "
                 "of size %ld\n", last_byte, ga->gmt_array, ga->name,
                 ga->nbytes_tot);
    }
}

/* returns true or false if this node has some data on the gmt array
 * ga at global offset goffset_bytes, in case return is true the value of
 * the local offset in bytes is written in loffset_p */
INLINE bool mem_gmt_data_is_local(gentry_t * ga, gmt_data_t gmt_array,
    uint64_t goffset_bytes, int64_t * loffset_p)
{

  _assert(ga != NULL);
  if (GD_GET_TYPE_DISTR(gmt_array) == GMT_ALLOC_LOCAL) {
    if(GD_GET_NODE(gmt_array) == node_id) {
      *loffset_p = (int64_t) goffset_bytes;
      return true;
    }  else {
      return false;
    }
  }

  /* In case of GMT_ALLOC_REMOTE ga->offset_bytes 
     falls outside the allocating node boundaries */
  _assert((int64_t) goffset_bytes >= 0 && (int64_t) ga->goffset_bytes >= 0);
  int64_t loffset = (int64_t) goffset_bytes - (int64_t) ga->goffset_bytes;
  if (loffset_p != NULL)
    *loffset_p = loffset;
  return (loffset >= 0 && loffset < (int64_t) ga->nbytes_loc);
}

/* get a local pointer once we are know the local offset */
INLINE uint8_t *mem_get_loc_ptr(gentry_t * ga,
                                uint64_t loffset_bytes, uint64_t size)
{
    if (loffset_bytes + size > ga->nbytes_loc) {
        ERRORMSG("Trying to access local byte %ld "
                 "in GMT array %d -name:%s- with local bytes %ld"
                 "(out of bound)\n",
                 loffset_bytes + size, GD_GET_GID(ga->gmt_array), ga->name,
                 ga->nbytes_loc);
    }
    uint8_t *ptr = &(ga->data[loffset_bytes]);
    return ptr;
}

INLINE uint64_t gmt_nelems_tot(gmt_data_t data) {
  if (data == GMT_DATA_NULL) return 0;
  else return  mem_get_gentry(data)->nbytes_tot / mem_get_gentry(data)->nbytes_elem;
}

INLINE void mem_free(gmt_data_t gmt_array)
{
    if (gmt_array == GMT_DATA_NULL)
        ERRORMSG("trying to free GMT_DATA_NULL\n");

    uint32_t gid = GD_GET_GID(gmt_array);
    _assert(gid < num_nodes * GMT_MAX_ALLOC_PER_NODE);

    gentry_t *ga = mem_get_gentry(gmt_array);
    _assert(ga != NULL);

    if (ga->gmt_array == GMT_DATA_NULL)
        ERRORMSG("gmt_free() already called for this array\n");

    if (GD_GET_TYPE_MEDIA(ga->gmt_array) == GMT_ALLOC_RAM ||
        (GD_GET_TYPE_MEDIA(ga->gmt_array) == GMT_ALLOC_SHM
         && (ga->name == NULL || (config.state_prot != (PROT_READ | PROT_WRITE))
             || config.state_name[0] == '\0'))) {
        free(ga->data);
        //_DEBUG("free\n");
    } else {
        switch (GD_GET_TYPE_MEDIA(ga->gmt_array)) {
        case GMT_ALLOC_SHM:
            af_on_file(NULL, ga, false);
            break;
        case GMT_ALLOC_DISK:
            af_on_file(config.disk_path, ga, false);
            break;
        case GMT_ALLOC_SSD:
            af_on_file(config.ssd_path, ga, false);
            break;
        }
    }

    if (ga->name != NULL)
        free(ga->name);
    ga->data = NULL;
    ga->nbytes_tot = 0;
    ga->nbytes_loc = 0;
    ga->nbytes_block = 0;
    ga->nbytes_elem = 0;
    ga->is_tmp = false;
    ga->gmt_array = GMT_DATA_NULL;

    if (GD_GET_NODE(gmt_array) == node_id) {
        uint32_t id = GD_GET_ID(gmt_array);
        mem_id_pool_push(&mem.mem_id_pool, id);
        __sync_sub_and_fetch(&mem.num_used_allocs, 1);
    }
}

INLINE void mem_locate_gmt_data_remote(gentry_t * ga,
                                       uint64_t goffset_bytes,
                                       uint32_t * rnid,
                                       uint64_t * roffset_bytes)
{

  _assert(ga!=NULL);
  if (goffset_bytes > ga->nbytes_tot) {
    ERRORMSG("Trying to access byte %ld "
        "in GMT array %d -name:%s- of size %ld - (out of bound)\n",
        goffset_bytes, GD_GET_GID(ga->gmt_array), ga->name,
        ga->nbytes_tot);
  }

  uint32_t _rnode_id;
  uint64_t _roffset_bytes = 0;

  switch (GD_GET_TYPE_DISTR(ga->gmt_array)) {
    case GMT_ALLOC_LOCAL:
      _rnode_id = GD_GET_NODE(ga->gmt_array);
      _roffset_bytes = goffset_bytes;
      break;
      /* the array was partitioned */
    case GMT_ALLOC_PARTITION_FROM_HERE:
    case GMT_ALLOC_PARTITION_FROM_RANDOM:
    case GMT_ALLOC_PARTITION_FROM_ZERO:
      {
        uint32_t tmp_rnode_id = goffset_bytes / ga->nbytes_block;
        _rnode_id = tmp_rnode_id + GD_GET_SNODE(ga->gmt_array);
        if (_rnode_id >= num_nodes)
          _rnode_id = _rnode_id - num_nodes;
        _roffset_bytes = goffset_bytes - ga->nbytes_block * (tmp_rnode_id);
      }
      break;
    case GMT_ALLOC_REPLICATE:
      {
        _rnode_id = node_id;
            _roffset_bytes = goffset_bytes;
        }
        break;

    case GMT_ALLOC_REMOTE:
        {
            _rnode_id = goffset_bytes / ga->nbytes_block;
            _assert(_rnode_id < num_nodes - 1);
            _roffset_bytes = goffset_bytes - (ga->nbytes_block * (_rnode_id));
            /* Take into account the hole of the allocator node */
            if (_rnode_id >= GD_GET_NODE(ga->gmt_array))
                _rnode_id++;
            _assert(_rnode_id != node_id);
        }
        break;
    case GMT_ALLOC_PERNODE:
    default:
        ERRORMSG("Allocation policy unknown!\n");
        break;
    }

    /* in this routine we are already sure rnode is not node_id, if this happens 
     * then we have a problem */
    _assert(_rnode_id != node_id);
    *rnid = _rnode_id;
    if (roffset_bytes != NULL)
        *roffset_bytes = _roffset_bytes;
}

INLINE void mem_put(uint8_t * ptr, const void *data, uint64_t num_bytes)
{
    _assert(ptr != NULL);
    _assert(data != NULL);
    memcpy(ptr, data, num_bytes);
}

INLINE void mem_put_value(uint8_t * ptr, uint64_t data, uint8_t num_bytes)
{
    _assert(ptr != NULL);
    switch (num_bytes) {
    case 1:
        *((uint8_t *) ptr) = (uint64_t) data;
        break;
    case 2:
        *((uint16_t *) ptr) = (uint64_t) data;
        break;
    case 4:
        *((uint32_t *) ptr) = (uint64_t) data;
        break;
    case 8:
        *((uint64_t *) ptr) = (uint64_t) data;
        break;
    default:
        ERRORMSG("memory put value size %d not handled\n", num_bytes);
    }
}

INLINE int64_t mem_atomic_cas(uint8_t * ptr, int64_t old_value,
                              int64_t new_value, uint8_t size)
{
    _assert(ptr != NULL);
    switch (size) {
    case 1:
        return (int64_t) __sync_val_compare_and_swap((int8_t *) ptr,
                                                     (int8_t) old_value,
                                                     (int8_t) new_value);
        break;
    case 2:
        return (int64_t) __sync_val_compare_and_swap((int16_t *) ptr,
                                                     (int16_t) old_value,
                                                     (int16_t) new_value);
        break;
    case 4:
        return (int64_t) __sync_val_compare_and_swap((int32_t *) ptr,
                                                     (int32_t) old_value,
                                                     (int32_t) new_value);
        break;
    case 8:
        return (int64_t) __sync_val_compare_and_swap((int64_t *) ptr,
                                                     old_value, new_value);
        break;
    default:
        ERRORMSG("memory atomic_cas size %d not supported\n", size);
        break;
    }
}

INLINE int64_t mem_atomic_add(uint8_t * ptr, int64_t value, uint8_t size)
{
    _assert(ptr != NULL);
    switch (size) {
    case 1:
        return (int64_t) __sync_fetch_and_add((int8_t *) ptr, (int8_t) value);
        break;
    case 2:
        return (int64_t) __sync_fetch_and_add((int16_t *) ptr, (int16_t) value);
        break;
    case 4:
        return (int64_t) __sync_fetch_and_add((int32_t *) ptr, (int32_t) value);
        break;
    case 8:
        return (int64_t) __sync_fetch_and_add((int64_t *) ptr, value);
        break;
    default:
        ERRORMSG("memory atomic_add size %d not supported\n", size);
        break;
    }
}

#endif
