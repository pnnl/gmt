#include "main.h"

std::map <uint64_t, std::pair <uint64_t, uint64_t> > localServer;

typedef struct {
  uint64_t id;
  uint64_t num_hops;
  gmt_data_t size_1;
  gmt_data_t servers;
  gmt_data_t horizon_1;
  uint64_t hops_offset;
  gmt_data_t reverseEdges;
} bfs_args_t;


void localServers(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  bfs_args_t * my_args = (bfs_args_t *) args;
  gentry_t * ga = mem_get_gentry(my_args->servers);
  uint64_t num_rows = gmt_nelems_tot(my_args->servers);
  uint64_t num_cols = ga->nbytes_elem / sizeof(uint64_t);
  uint64_t hops_column = my_args->hops_offset / sizeof(uint64_t);

  std::vector <uint64_t> buffer(num_rows * num_cols);
  gmt_get(my_args->servers, 0, (void *) buffer.data(), num_rows);

  for (uint64_t i = 0; i < num_rows; i ++) {
    uint64_t * row = & buffer[i * num_cols];
    localServer[row[0]] = std::make_pair(i, row[hops_column]);
} }


void BFS(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  bfs_args_t * my_args = (bfs_args_t *) args;
  gmt_data_t edges = my_args->reverseEdges;
  std::vector <uint64_t> edgeRow = {0, my_args->id};
  std::pair <uint64_t, uint64_t> edgeRange = gmt_equal_range(edgeRow, edges, __cols1);

  uint64_t num_neighbors = edgeRange.second - edgeRange.first;
  if (num_neighbors == 0) return;

  std::vector <uint64_t> my_horizon;
  std::vector <uint64_t> neighbors(num_neighbors);
  gmt_get_bytes(edges, edgeRange.first, (void *) neighbors.data(), num_neighbors, 0, sizeof(uint64_t));

  for (uint64_t neighbor : neighbors) {                     // for each neighbor of v
    uint64_t ndx = localServer[neighbor].first;
    if (localServer[neighbor].second != MAX_HORIZONS) continue;

    localServer[neighbor].second = my_args->num_hops;
    uint64_t hops = gmt_atomic_min(my_args->servers, ndx, (int64_t) my_args->num_hops, my_args->hops_offset);

    if (hops == MAX_HORIZONS) my_horizon.push_back(neighbor);
  }

  if (my_horizon.size() > 0) {
     uint64_t start = gmt_atomic_add(my_args->size_1, 0, (int64_t) my_horizon.size());
     gmt_put(my_args->horizon_1, start, (void *) my_horizon.data(), my_horizon.size());
} } 


/********** Breadth First Searches **********/
void BFS_Setup(Graph_t & graph) {
  double time1 = my_timer();
  Table Netflow = * graph["Netflow"];
  Table Servers = * graph["Servers"];

// reverse edges
  Table Reverse;
  Reverse.schema   = Netflow.schema;
  Reverse.num_cols = Netflow.num_cols;
  Reverse.num_rows = Netflow.num_rows;
  Reverse.data = gmt_alloc(Reverse.num_rows, Reverse.num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "");

  gmt_memcpy(Netflow.data, 0, Reverse.data, 0, Reverse.num_rows);
  sortTable(& Reverse, __cols10);

// initialize hops column
  gmt_data_t servers = Servers.data;
  uint64_t num_servers = Servers.num_rows;

  std::vector <uint64_t> initValues(num_servers, MAX_HORIZONS);
  uint64_t hops_offset = findColumn("hops", Servers.schema) * sizeof(uint64_t);
  gmt_put_bytes(servers, 0, (void *) initValues.data(), num_servers, hops_offset, sizeof(uint64_t));

// identify bad guys and set up initial horizon
  std::set<uint64_t> bad_guys;
  gmt_data_t size_0 = gmt_alloc(1, sizeof(uint64_t), GMT_ALLOC_LOCAL, "size 0");
  gmt_data_t size_1 = gmt_alloc(1, sizeof(uint64_t), GMT_ALLOC_LOCAL, "size 1");
  gmt_data_t horizon_0 = gmt_alloc(num_servers, sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "horizon_0");
  gmt_data_t horizon_1 = gmt_alloc(num_servers, sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "horizon_1");

  uint64_t zero = 0;
  uint64_t * s0 = (uint64_t *) mem_get_gentry(size_0)->data;
  uint64_t * s1 = (uint64_t *) mem_get_gentry(size_1)->data;

  * s0 = 0;
  * s1 = 0;

// identify bad guys: 1) mark visited, 2) place on horizon, 3) set hops to zero
  while (* s0 < NUM_BAD_GUYS) {
    uint64_t ndx = drand48() * num_servers;
    uint64_t id = gmt_get_value(servers, ndx);
    if (bad_guys.insert(id).second == false) continue;

    gmt_put_value(horizon_0, * s0, id);
    gmt_put_bytes(servers, ndx, (void *) & zero, 1, hops_offset, sizeof(uint64_t));
    (* s0) ++;
  }

  bfs_args_t args;
  args.num_hops = 0;
  args.size_1 = size_1;
  args.servers = servers;
  args.horizon_1 = horizon_1;
  args.hops_offset = hops_offset;
  args.reverseEdges = Reverse.data;

// initialize a local server map on each processor for speed
  gmt_execute_on_all(localServers, & args, sizeof(bfs_args_t), GMT_PREEMPTABLE);

// while less than exceeded maximun number of horizons AND horizon 0 is not empty
  while ((args.num_hops < MAX_HORIZONS) && (* s0 > 0)) {
    args.num_hops ++;

    for (uint64_t i = 0; i < * s0; i ++) {           // for each vertex on horizon 0
      args.id = gmt_get_value(horizon_0, i);
      uint64_t ndx = localServer[args.id].first;     // ... execute BFS on its home processor
      gmt_execute_on_data_nb(servers, ndx, BFS, & args, sizeof(bfs_args_t), NULL, NULL, GMT_PREEMPTABLE);
    }

    gmt_wait_execute_nb();
    std::swap(horizon_0, horizon_1);
    * s0 = * s1;
    * s1 = 0;
  }

  printf("Time to set up BFS = %lf\n", my_timer() - time1);
}
