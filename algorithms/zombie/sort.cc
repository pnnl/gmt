#include <regex>
#include <string>
#include <vector>
#include <climits>

#include "gmt/gmt.h"
#include "gmt/memory.h"
#include "sort.h"

/********** ARGS **********/
typedef struct args_t {
  gmt_data_t data;
  gmt_data_t outdata;
  uint64_t num_tasks;
  uint64_t blocksize;
  uint64_t columns[SORT_COLS];
} args_t;

typedef struct CS_args_t {
  gmt_data_t data;
  uint64_t columns[SORT_COLS];
} CS_args_t;


typedef struct Bound_args_t {
  gmt_data_t data;
  uint64_t lb;
  uint64_t ub;
  uint64_t columns[SORT_COLS];
  uint64_t keyRow[SORT_COLS];
} Bound_args_t;


/********** FIND METHODS **********/
void _LowerBound(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  Bound_args_t * my_args = (Bound_args_t *) args;
  uint64_t lb = my_args->lb;
  uint64_t ub = my_args->ub;
  uint64_t index = lowerbound((uint64_t *) my_args->keyRow, my_args->data, get_comparator(my_args->columns), lb, ub);

  if ((index >= lb) && (index < ub)) {
     ((uint64_t *) ret)[0] = index;
     * ret_size = sizeof(uint64_t);
} }

void _UpperBound(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  Bound_args_t * my_args = (Bound_args_t *) args;
  uint64_t lb = my_args->lb;
  uint64_t ub = my_args->ub;
  uint64_t index = upperbound((uint64_t *) my_args->keyRow, my_args->data, get_comparator(my_args->columns), lb, ub);

  if ((index >= lb) && (index < ub)) {
     ((uint64_t *) ret)[0] = index;
     * ret_size = sizeof(uint64_t);
} }

uint64_t gmt_lower_bound(std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  return gmt_lower_bound_limit(keyRow, data, columns, 0, gmt_nelems_tot(data));
}

uint64_t gmt_lower_bound_limit(std::vector <uint64_t> & keyRow,
  gmt_data_t data, uint64_t * columns, uint64_t lb, uint64_t ub) {

  if (GD_GET_TYPE_DISTR(data) == GMT_ALLOC_REPLICATE) {
     return lowerbound(keyRow.data(), data, get_comparator(columns), lb, ub);

  } else {

     uint32_t lb_node, ub_node;
     uint64_t roffset, ret = ub;
     uint32_t num_nodes = gmt_num_nodes(), retSize = sizeof(uint64_t);

     gentry_t * const ga = mem_get_gentry(data);
     uint64_t lb_byte_offset = lb * ga->nbytes_elem;
     uint64_t ub_byte_offset = ub * ga->nbytes_elem;

     Bound_args_t args;
     args.lb = lb;
     args.ub = ub;
     args.data = data;
     memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));
     memcpy(args.keyRow, keyRow.data(), keyRow.size() * sizeof(uint64_t));

     mem_locate_gmt_data_remote(ga, lb_byte_offset, & lb_node, & roffset);
     mem_locate_gmt_data_remote(ga, ub_byte_offset, & ub_node, & roffset);
     if (ub_node < lb_node) ub_node += num_nodes;

     for (uint32_t node = lb_node; node <= ub_node; node ++)
       gmt_execute_on_node_nb(node % num_nodes, _LowerBound, & args,
                sizeof(Bound_args_t), & ret, & retSize, GMT_PREEMPTABLE);

     gmt_wait_execute_nb();
     return ret;
} }

uint64_t gmt_upper_bound(std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  return gmt_upper_bound_limit(keyRow, data, columns, 0, gmt_nelems_tot(data));
}

uint64_t gmt_upper_bound_limit(std::vector <uint64_t> & keyRow,
  gmt_data_t data, uint64_t * columns, uint64_t lb, uint64_t ub) {

  if (GD_GET_TYPE_DISTR(data) == GMT_ALLOC_REPLICATE) {
     return upperbound(keyRow.data(), data, get_comparator(columns), lb, ub);

  } else {

     uint32_t lb_node, ub_node;
     uint64_t roffset, ret = ub;
     uint32_t num_nodes = gmt_num_nodes(), retSize = sizeof(uint64_t);

     gentry_t * const ga = mem_get_gentry(data);
     uint64_t lb_byte_offset = lb * ga->nbytes_elem;
     uint64_t ub_byte_offset = ub * ga->nbytes_elem;

     Bound_args_t args;
     args.lb = lb;
     args.ub = ub;
     args.data = data;
     memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));
     memcpy(args.keyRow, keyRow.data(), keyRow.size() * sizeof(uint64_t));

     mem_locate_gmt_data_remote(ga, lb_byte_offset, & lb_node, & roffset);
     mem_locate_gmt_data_remote(ga, ub_byte_offset, & ub_node, & roffset);
     if (ub_node < lb_node) ub_node += num_nodes;

     for (uint32_t node = lb_node; node <= ub_node; node ++)
       gmt_execute_on_node_nb(node % num_nodes, _UpperBound, & args,
                sizeof(Bound_args_t), & ret, & retSize, GMT_PREEMPTABLE);

     gmt_wait_execute_nb();
     return ret;
} }

std::pair <uint64_t, uint64_t> gmt_equal_range(std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  return gmt_equal_range_limit(keyRow, data, columns, 0, gmt_nelems_tot(data));
}


std::pair <uint64_t, uint64_t> gmt_equal_range_limit(std::vector <uint64_t> & keyRow,
     gmt_data_t data, uint64_t * columns, uint64_t lb, uint64_t ub) {
  uint64_t llb = gmt_lower_bound_limit(keyRow, data, columns, lb, ub);
  uint64_t uub = gmt_upper_bound_limit(keyRow, data, columns, lb, ub);
  return std::make_pair(llb, uub);
}


/********** SORT **********/
void merge_block(uint64_t it, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  args_t * my_args = (args_t *) args;

  gmt_data_t data = my_args->data;
  gmt_data_t outdata = my_args->outdata;
  uint64_t num_tasks = my_args->num_tasks;
  uint64_t blocksize = my_args->blocksize;
  uint64_t num_workers = gmt_num_nodes() * gmt_num_workers();

  gentry_t * ga = mem_get_gentry(data);
  uint64_t my_task, my_task_worker_id;
  uint64_t tew = num_workers % num_tasks;             // number of tasks with extra worker
  uint64_t wpt = CEILING(num_workers, num_tasks);     // workers per task

  if ((tew == 0) || (it < wpt * tew)) {
     my_task = it / wpt;
     my_task_worker_id = it % wpt;
  } else {
     it -= wpt * tew;
     wpt --;

     my_task = tew + it / wpt;
     my_task_worker_id = it % wpt;
  }

  uint64_t start = my_task * blocksize;
  uint64_t mid   = start + (blocksize >> 1);
  uint64_t end   = MIN(start + blocksize, gmt_nelems_tot(data));

  merge_block_section(my_task_worker_id, wpt, ga->nbytes_elem,
       start, mid, end, outdata, data, my_args->columns, get_comparator(my_args->columns));
}


gmt_data_t merge_sorted_blocks(uint64_t num_blocks, uint64_t blocksize, gmt_data_t data, uint64_t columns[SORT_COLS]) {
  gentry_t * ga = mem_get_gentry(data);
  uint64_t num_elems = gmt_nelems_tot(data);
  uint64_t num_workers = gmt_num_nodes() * gmt_num_workers();
  gmt_data_t outdata = gmt_alloc(num_elems, ga->nbytes_elem, GMT_ALLOC_PARTITION_FROM_ZERO, "outdata");

  args_t args;   
  args.data = data;
  args.outdata = outdata;
  args.blocksize = blocksize;
  memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));

  while (num_blocks > 1) {
     args.blocksize = args.blocksize << 1;

     if (num_blocks % 2 == 1) {      // if previous number of blocks is odd, then last block has no partner
        num_blocks = num_blocks / 2 + 1;
        args.num_tasks = num_blocks - 1;
        uint64_t start = args.num_tasks * args.blocksize;     // copy last block to outdata
        gmt_memcpy_nb(args.data, start, args.outdata, start, num_elems - start);
     } else {
        num_blocks = num_blocks / 2;
        args.num_tasks = num_blocks;
     }

     gmt_for_loop(num_workers, 1, merge_block, & args, sizeof(args_t), GMT_SPAWN_SPREAD);

     gmt_wait_data();
     gmt_wait_execute_nb();
     std::swap(args.data, args.outdata);
  }

  gmt_free(args.outdata);
  return args.data;
}


void sort_blocks(uint64_t i, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  args_t * my_args = (args_t *) args;

  gmt_data_t data = my_args->data;
  uint64_t blocksize = my_args->blocksize;
  gentry_t * ga = mem_get_gentry(my_args->data);

  uint64_t start = i * blocksize;
  uint64_t end = MIN(start + blocksize, gmt_nelems_tot(data));
  if (start >= end) return;

  uint64_t num_elems = end - start;
  uint64_t num_bytes = ga->nbytes_elem;
  uint8_t * indata  = (uint8_t *) malloc(num_elems * num_bytes);
  uint8_t * outdata = (uint8_t *) malloc(num_elems * num_bytes);
  uint64_t ** rows  = (uint64_t **) malloc(num_elems * sizeof(uint64_t *));

// construct array of indata row pointers
  gmt_get(data, start, (void *) indata, num_elems);
  for (uint64_t i = 0; i < num_elems; i ++) rows[i] = (uint64_t *) (indata + i * num_bytes);

// sort row pointers
  std::sort(rows, rows + num_elems, get_comparator(my_args->columns));

// permute rows to outdata
  for (uint64_t i = 0; i < num_elems; i++) memcpy(outdata + i * num_bytes, rows[i], num_bytes);
  gmt_put(data, start, (void *) outdata, num_elems);

  free(rows);
  free(indata);
  free(outdata);
}


gmt_data_t gmt_sort(gmt_data_t data, uint64_t * columns) {
  uint64_t num_workers = gmt_num_nodes() * gmt_num_workers();
  uint64_t blocksize = CEILING(gmt_nelems_tot(data), num_workers);

  args_t my_args;
  my_args.data = data;
  my_args.blocksize = blocksize;
  memcpy(my_args.columns, columns, SORT_COLS * sizeof(uint64_t));

  gmt_for_loop(num_workers, 1, sort_blocks, & my_args, sizeof(my_args), GMT_SPAWN_SPREAD);
  return merge_sorted_blocks(num_workers, blocksize, data, columns);
}


/********** CHECK SORT **********/
void _checkSort(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  CS_args_t * my_args = (CS_args_t *) args;
  checksort(my_args->data, get_comparator(my_args->columns));
}


void checkSort(std::string name, gmt_data_t data, uint64_t * columns) {
  uint32_t ret, retSize;

  CS_args_t args;
  args.data = data;
  memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));

  for (uint64_t i = 0; i < gmt_num_nodes(); i ++)
    gmt_execute_on_node_nb(i, _checkSort, & args, sizeof(CS_args_t), & ret, & retSize, GMT_PREEMPTABLE);

  gmt_wait_execute_nb();
  printf("sort check complete\n");
}
