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
  uint64_t block_size;
  uint64_t columns[SORT_COLS];
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

    gmt_wait_execute_nb();
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

     gmt_wait_execute_nb();
    return ub;
} }

std::pair <uint64_t, uint64_t> gmt_equal_range(
    std::vector <uint64_t> & keyRow, gmt_data_t data, uint64_t * columns) {
  return std::make_pair( gmt_lower_bound(keyRow, data, columns), gmt_upper_bound(keyRow, data, columns) );
}


/********** SORT **********/
void merge_block(uint64_t it, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  args_t * my_args = (args_t *) args;

  gmt_data_t data = my_args->data;
  gmt_data_t outdata = my_args->outdata;
  uint64_t num_tasks = my_args->num_tasks;
  uint64_t block_size = my_args->block_size;
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

  uint64_t start = my_task * block_size;
  uint64_t mid   = start + (block_size >> 1);
  uint64_t end   = MIN(start + block_size, gmt_nelems_tot(data));

  merge_block_section(my_task_worker_id, wpt, ga->nbytes_elem,
       start, mid, end, outdata, data, get_comparator(my_args->columns));
}


gmt_data_t merge_sorted_blocks(uint64_t block_size, gmt_data_t data, uint64_t columns[SORT_COLS]) {
  gentry_t * ga = mem_get_gentry(data);
  uint64_t num_bytes = ga->nbytes_elem;
  uint64_t num_elems = gmt_nelems_tot(data);
  uint64_t num_workers = gmt_num_nodes() * gmt_num_workers();
  gmt_data_t outdata = gmt_alloc(num_elems, num_bytes, GMT_ALLOC_PARTITION_FROM_ZERO, "outdata");


  args_t args;   
  args.data = data;
  args.outdata = outdata;
  args.block_size = block_size << 1;
  memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));

  for ( ; args.block_size < (num_elems << 1); args.block_size <<= 1) {
      uint64_t num_tasks = CEILING(num_elems, args.block_size);
      uint64_t start = (num_tasks - 1) * args.block_size;         // of last task
      uint64_t mid = start + (args.block_size >> 1);              // of last task
      uint64_t end = MIN(start + args.block_size, num_elems);     // of last task

      if (end <= mid) {                                           // last task has no right hand side
         gmt_memcpy_nb(data, start, outdata, start, end - start);
         num_tasks --;
      }

      args.num_tasks = num_tasks;
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
  uint64_t block_size = my_args->block_size;
  gentry_t * ga = mem_get_gentry(my_args->data);

  uint64_t start = i * block_size;
  uint64_t end = MIN(start + block_size, gmt_nelems_tot(data));
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

  args_t my_args;
  my_args.data = data;
  my_args.block_size = CEILING(gmt_nelems_tot(data), num_workers);
  memcpy(my_args.columns, columns, SORT_COLS * sizeof(uint64_t));

  gmt_for_loop(num_workers, 1, sort_blocks, & my_args, sizeof(my_args), GMT_SPAWN_SPREAD);
  return merge_sorted_blocks(my_args.block_size, data, columns);
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
