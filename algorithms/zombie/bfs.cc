#include "main.h"

std::map <uint64_t, std::pair <uint64_t, uint64_t> > localServer;

typedef struct {
  uint64_t id;
  gmt_data_t edges;
  gmt_data_t servers;
  uint64_t hops_offset;
  uint64_t num_hops;
  gmt_data_t size;
  gmt_data_t horizon;
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

  gmt_data_t edges = my_args->edges;
  gmt_data_t servers = my_args->servers;
  uint64_t num_hops = my_args->num_hops;
  uint64_t hops_offset = my_args->hops_offset;
  std::vector <uint64_t> edgeRow = {my_args->id};
  std::pair <uint64_t, uint64_t> edgeRange = gmt_equal_range(edgeRow, edges, __cols0);

  uint64_t num_neighbors = edgeRange.second - edgeRange.first;
  if (num_neighbors == 0) return;

  std::vector <uint64_t> my_horizon;
  std::vector <uint64_t> neighbors(num_neighbors);
  uint64_t my_ndx, my_hops, last_neighbor = ULLONG_MAX;
  gmt_get_bytes(edges, edgeRange.first, (void *) neighbors.data(), num_neighbors, 8, 8);

  for (uint64_t neighbor : neighbors) {                                 // for each neighbor of v
    if (neighbor == last_neighbor) continue;
    last_neighbor = neighbor;

    uint64_t my_ndx, my_hops;
    std::tie(my_ndx, my_hops) = localServer[neighbor];

    // if shorter path or equivalent already found, continue
    if (my_hops <= num_hops) continue;

    localServer[neighbor].second = num_hops;
    uint64_t hops = gmt_atomic_min(servers, my_ndx, (int64_t) num_hops, hops_offset);
    if (my_ndx == 1) {
       uint64_t value;
       gmt_get_bytes(servers, my_ndx, (void *) & value, 1, hops_offset, sizeof(uint64_t));
    }

    // if first to find shortest path, push onto local horizon
    if (hops > num_hops) my_horizon.push_back(neighbor);
  }

  if (my_horizon.size() > 0) {
     uint64_t start = gmt_atomic_add(my_args->size, 0, (int64_t) my_horizon.size());
     gmt_put(my_args->horizon, start, (void *) my_horizon.data(), my_horizon.size());
} } 


/********** Number of Hops From Bad Guys **********/
void numberHops(std::set <uint64_t> & bad_guys, uint64_t hops_offset, Table & Edges, Table & Servers) {

// set up initial horizon
  gmt_data_t size_0    = gmt_alloc(1, sizeof(uint64_t), GMT_ALLOC_LOCAL, "size 0");
  gmt_data_t size_1    = gmt_alloc(1, sizeof(uint64_t), GMT_ALLOC_LOCAL, "size 1");
  gmt_data_t horizon_0 = gmt_alloc(Servers.num_rows, sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "horizon_0");
  gmt_data_t horizon_1 = gmt_alloc(Servers.num_rows, sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "horizon_1");

  bfs_args_t args;
  args.edges = Edges.data;
  args.servers = Servers.data;
  args.hops_offset = hops_offset;

// place bad guy on horizon and set their number of hops to zero
  uint64_t id, size = 0, zero = 0;

  for (uint64_t ndx : bad_guys) {
    gmt_get_bytes(Servers.data, ndx, (void *) & id, 1, 0, 8);                 // get bad guy id
    gmt_put_bytes(Servers.data, ndx, (void *) & zero, 1, hops_offset, 8);     // set hops to zero
    gmt_put(horizon_0, size, (void *) & id, 1);                               // put id on horizon
    size ++;
  }

  gmt_put(size_0, 0, (void *) & size, 1);
  gmt_put(size_1, 0, (void *) & zero, 1);

// initialize a local server map on each processor for speed
  gmt_execute_on_all(localServers, & args, sizeof(bfs_args_t), GMT_PREEMPTABLE);

  for (uint64_t num_hops = 1; num_hops < MAX_HORIZONS; num_hops ++) {
    gmt_get(size_0, 0, (void *) & size, 1);
    if (size == 0) break;                            // dead end

    args.size = size_1;
    args.horizon = horizon_1;
    args.num_hops = num_hops;

    for (uint64_t i = 0; i < size; i ++) {           // for each vertex on horizon 0
      args.id = gmt_get_value(horizon_0, i);
      uint64_t ndx = localServer[args.id].first;     // ... execute BFS on its home processor
      gmt_execute_on_data_nb(Servers.data, ndx, BFS, & args, sizeof(bfs_args_t), NULL, NULL, GMT_PREEMPTABLE);
    }

    gmt_wait_execute_nb();
    std::swap(size_0, size_1);
    std::swap(horizon_0, horizon_1);
    gmt_put(size_1, 0, (void *) & zero, 1);
  }

  gmt_free(size_0);    gmt_free(size_1);
  gmt_free(horizon_0); gmt_free(horizon_1);
}


void BFS_Setup(Graph_t & graph) {
  double time1 = my_timer();
  Table Netflow = * graph["Netflow"];
  Table Servers = * graph["Servers"];

// reverse edges
  Table Reverse = reverseEdgeTable(Netflow);

// initialize hops column
  gmt_data_t servers = Servers.data;
  uint64_t num_servers = Servers.num_rows;

  std::vector <uint64_t> initValues(num_servers, MAX_HORIZONS);
  uint64_t hopsTo_offset   = findColumn("hopsTo",   Servers.schema) * sizeof(uint64_t);
  uint64_t hopsFrom_offset = findColumn("hopsFrom", Servers.schema) * sizeof(uint64_t);
  gmt_put_bytes(servers, 0, (void *) initValues.data(), num_servers, hopsTo_offset,   sizeof(uint64_t));
  gmt_put_bytes(servers, 0, (void *) initValues.data(), num_servers, hopsFrom_offset, sizeof(uint64_t));

// identify bad guys
  std::set <uint64_t> bad_guys;
  while (bad_guys.size() < NUM_BAD_GUYS) bad_guys.insert( drand48() * num_servers );

// path from X to bad_guys ... equivalent to finding path from bad guys to X using reverse edges
   numberHops(bad_guys, hopsTo_offset, Reverse, Servers);

// path from bad_guys to X
   numberHops(bad_guys, hopsFrom_offset, Netflow, Servers);

  printf("Time to set up BFS = %lf\n", my_timer() - time1);
}
