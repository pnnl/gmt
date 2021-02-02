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
  uint64_t num_cols;
  uint64_t num_rows;
  uint64_t num_tasks;
  uint64_t block_size;
  uint64_t num_workers;
  uint64_t columns[SORT_COLS];
  uint64_t num_workers_per_task;
} args_t;

typedef struct CS_args_t {
  gmt_data_t data;
  uint64_t columns[SORT_COLS];
} CS_args_t;


typedef struct Find_args_t {
  gmt_data_t data;
  uint64_t num_rows;
  uint64_t columns[SORT_COLS];
  uint64_t keyRow[SORT_COLS];
} Find_args_t;


/********** FIND METHODS **********/
void _LowerBound(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  Find_args_t * my_args = (Find_args_t *) args;
  uint64_t index = lowerbound((uint64_t *) my_args->keyRow, my_args->data, get_comparator(my_args->columns));

  if (index < my_args->num_rows) {
     ((uint64_t *) ret)[0] = index;
     * ret_size = sizeof(uint64_t);
} }

void _UpperBound(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  Find_args_t * my_args = (Find_args_t *) args;
  uint64_t index = upperbound((uint64_t *) my_args->keyRow, my_args->data, get_comparator(my_args->columns));

  if (index < my_args->num_rows) {
     ((uint64_t *) ret)[0] = index;
     * ret_size = sizeof(uint64_t);
} }

uint64_t gmt_lower_bound(std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  uint64_t lb = gmt_nelems_tot(data);
  uint32_t lbSize = sizeof(uint64_t);

  if (GD_GET_TYPE_DISTR(data) == GMT_ALLOC_REPLICATE) {
     return lowerbound(keyRow.data(), data, get_comparator(columns));

  } else {

     Find_args_t args;
     args.data = data;
     args.num_rows = gmt_nelems_tot(data);
     memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));
     memcpy(args.keyRow, keyRow.data(), keyRow.size() * sizeof(uint64_t));

     for (uint32_t node = 0; node < gmt_num_nodes(); node ++)
         gmt_execute_on_node_nb(node, _LowerBound, & args, sizeof(Find_args_t), & lb, & lbSize, GMT_PREEMPTABLE);

    gmt_wait_for_nb();
    return lb;
} }

uint64_t gmt_upper_bound(std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  uint64_t ub = gmt_nelems_tot(data);
  uint32_t ubSize = sizeof(uint64_t);

  if (GD_GET_TYPE_DISTR(data) == GMT_ALLOC_REPLICATE) {
     return upperbound(keyRow.data(), data, get_comparator(columns));

  } else {

     Find_args_t args;
     args.data = data;
     args.num_rows = gmt_nelems_tot(data);
     memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));
     memcpy(args.keyRow, keyRow.data(), keyRow.size() * sizeof(uint64_t));

     for (uint32_t node = 0; node < gmt_num_nodes(); node ++)
         gmt_execute_on_node_nb(node, _UpperBound, & args, sizeof(Find_args_t), & ub, & ubSize, GMT_PREEMPTABLE);

     gmt_wait_for_nb();
    return ub;
} }

std::pair <uint64_t, uint64_t> gmt_equal_range(
    std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  return std::make_pair( gmt_lower_bound(keyRow, data, columns), gmt_upper_bound(keyRow, data, columns) );
}


/********** SORT **********/
void merge_block(uint64_t it, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  args_t * my_args = (args_t *) args;

  gmt_data_t indata    = my_args->data;
  gmt_data_t outdata   = my_args->outdata;
  uint64_t num_cols    = my_args->num_cols;
  uint64_t num_rows    = my_args->num_rows;
  uint64_t num_tasks   = my_args->num_tasks;
  uint64_t block_size  = my_args->block_size;
  uint64_t num_workers = my_args->num_workers;

  uint64_t num_workers_per_task = my_args->num_workers_per_task;
  uint64_t my_task = it / num_workers_per_task;

  // last task may have fewer coworkers
  uint64_t num_coworkers = num_workers_per_task;
  if (my_task == num_tasks - 1) num_coworkers = num_workers - (my_task * num_workers_per_task);

  uint64_t my_task_worker_id = it % num_coworkers;

  uint64_t start = my_task * block_size;
  uint64_t mid   = start + (block_size >> 1);
  uint64_t end   = MIN(start + block_size, num_rows);

  if (end <= mid) {     // no right side, so task worker 0 copies block to outdata
     if (my_task_worker_id == 0) gmt_memcpy(indata, start, outdata, start, end - start);
  } else {
     merge_block_section(my_task_worker_id, num_coworkers,
          num_cols, start, mid, end, outdata, indata, get_comparator(my_args->columns));
} }


gmt_data_t merge_sorted_blocks(uint64_t num_workers, uint64_t block_size, uint64_t num_rows, uint64_t num_cols, gmt_data_t data, uint64_t columns[SORT_COLS]) {
  gmt_data_t outdata = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "outdata");

  args_t args;   
  args.data = data;
  args.outdata = outdata;
  args.num_cols = num_cols;
  args.num_rows = num_rows;
  args.num_workers = num_workers;
  args.block_size = block_size << 1;
  memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));

  for ( ; args.block_size < (num_rows << 1); args.block_size <<= 1) {
      args.num_tasks = CEILING(num_rows, args.block_size);
      args.num_workers_per_task = CEILING(num_workers, args.num_tasks);

      gmt_for_loop(num_workers, 1, merge_block, & args, sizeof(args_t), GMT_SPAWN_SPREAD);
      std::swap(args.data, args.outdata);
  }

  gmt_free(args.outdata);
  return args.data;
}


void sort_blocks(uint64_t i, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  args_t * my_args = (args_t *) args;

  gmt_data_t data = my_args->data;
  uint64_t block_size = my_args->block_size;

  uint64_t start = i * block_size;
  uint64_t end = MIN(start + block_size, my_args->num_rows);
  if (start >= end) return;

  uint64_t num_rows  = end - start;
  uint64_t num_cols  = my_args->num_cols;
  uint64_t ** rows   = (uint64_t **) malloc(num_rows * sizeof(uint64_t *));
  uint64_t * indata  = (uint64_t *) malloc(num_rows * num_cols * sizeof(uint64_t));
  uint64_t * outdata = (uint64_t *) malloc(num_rows * num_cols * sizeof(uint64_t));

// construct array of indata row pointers
  gmt_get(data, start, (void *) indata, num_rows);
  for (uint64_t i = 0; i < num_rows; i ++) rows[i] = indata + i * num_cols;

// sort row pointers
  std::sort(rows, rows + num_rows, get_comparator(my_args->columns));

// permute rows to outdata
  for (uint64_t i = 0; i < num_rows; i++) memcpy(outdata + i * num_cols, rows[i], num_cols * sizeof(uint64_t));
  gmt_put(data, start, (void *) outdata, num_rows);

  free(rows);
  free(indata);
  free(outdata);
}


gmt_data_t gmt_sort(gmt_data_t data, uint64_t * columns) {
  gentry_t * ga  = mem_get_gentry(data);
  uint64_t num_workers = gmt_num_nodes() * gmt_num_workers();
  uint64_t num_cols = ga->nbytes_elem / sizeof(uint64_t);
  uint64_t num_rows = ga->nbytes_tot / ga->nbytes_elem;

  args_t my_args;
  my_args.data = data;
  my_args.num_cols = num_cols;
  my_args.num_rows = num_rows;
  my_args.block_size = CEILING(num_rows, num_workers);
  memcpy(my_args.columns, columns, SORT_COLS * sizeof(uint64_t));

  gmt_for_loop(num_workers, 1, sort_blocks, & my_args, sizeof(my_args), GMT_SPAWN_SPREAD);
  return merge_sorted_blocks(num_workers, my_args.block_size, num_rows, num_cols, data, columns);
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

  gmt_wait_for_nb();
  printf("sort check complete\n");
}
