#include "main.h"

typedef struct MBG_args_t {
  Table edges;
  Table vertices;
} MBG_args_t;


void MarkBadGuys(gmt_data_t bad_guys, uint64_t i, uint64_t num_elem, const void * args, gmt_handle_t handle) {
  MBG_args_t * my_args = (MBG_args_t *) args;

  uint64_t root;
  std::set <uint64_t> visited;
  std::vector < std::vector <uint64_t> > horizons(MAX_HORIZONS);

  uint64_t hopsCol = findColumn("hops", my_args->vertices.schema);
  std::vector <uint64_t> edgeRow(my_args->edges.num_cols, ULLONG_MAX);
  std::vector <uint64_t> vertexRow(my_args->vertices.num_cols, ULLONG_MAX);

  gmt_get(bad_guys, i, (void *) & root, 1);
  horizons[0].push_back(root);
  visited.insert(root);

  for (uint64_t h = 0; h < MAX_HORIZONS; h ++) {
    if (horizons[h].size() == 0) break;                              // dead end

    for (uint64_t v : horizons[h]) {                                 // for each vertex in the horizon
      edgeRow[1] = v;
      uint64_t last_dst = ULLONG_MAX;
      std::pair <uint64_t, uint64_t> edgeRange = gmt_equal_range(edgeRow, my_args->edges.data, __cols1);

      uint64_t num_rows = edgeRange.second - edgeRange.first;
      if (num_rows == 0) continue;

      std::vector <uint64_t> edgeBuffer(num_rows * my_args->edges.num_cols);
      gmt_get(my_args->edges.data, edgeRange.first, (void *) edgeBuffer.data(), num_rows);

      for (uint64_t j = 0; j < num_rows; j ++) {                     // for each edge of v
        uint64_t dst = edgeBuffer[j * my_args->edges.num_cols];
        if (dst == last_dst) continue; else last_dst = dst;          // skip edges to same dst

        if (visited.insert(dst).second) {                            // neighbor is unvisited
           vertexRow[0] = dst;
           uint64_t ndx = gmt_lower_bound(vertexRow, my_args->vertices.data, __cols0);
           gmt_get(my_args->vertices.data, ndx, (void *) vertexRow.data(), 1);

           if ((h + 1) < vertexRow[hopsCol]) {                              // found a shorter path to a bad guy
              vertexRow[hopsCol] = h + 1;                                   // ... update hops !!! FIX RACE CONDITION
              gmt_put(my_args->vertices.data, ndx, (void *) vertexRow.data(), 1);
              if (h < MAX_HORIZONS - 1) horizons[h + 1].push_back(dst);     // ... and keep going
} } } } } }


/********** Breadth First Searches **********/
void BFS_Setup(Graph_t & graph) {
  double time1 = my_timer();
  Table Netflow = * graph["Netflow"];
  Table Servers = * graph["Servers"];

  Table Reverse;
  Reverse.schema   = Netflow.schema;
  Reverse.num_cols = Netflow.num_cols;
  Reverse.num_rows = Netflow.num_rows;
  Reverse.data = gmt_alloc(Reverse.num_rows, Reverse.num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "");

  gmt_memcpy(Netflow.data, 0, Reverse.data, 0, Reverse.num_rows);
  sortTable(& Reverse, __cols10);

// initialize bad guys for BFS
  std::set <uint64_t> bad_guys_set;
  std::vector <uint64_t > row(Servers.num_cols);
  uint64_t hopsCol = findColumn("hops", Servers.schema);
  gmt_data_t bad_guys = gmt_alloc(NUM_BAD_GUYS, sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "bad_guys");

  while (bad_guys_set.size() < NUM_BAD_GUYS) {
    uint64_t ndx = drand48() * Servers.num_rows;
    gmt_get(Servers.data, ndx, (void *) row.data(), 1);
    if (bad_guys_set.insert(row[0]).second == false) continue;     // if server already choosen, continue

    row[hopsCol] = 0;
    gmt_put(Servers.data, ndx, (void *) row.data(), 1);
    gmt_put(bad_guys, bad_guys_set.size() - 1, (void *) row.data(), 1);
  }

  MBG_args_t args;
  args.edges = Reverse;
  args.vertices = Servers;

// find vertices that reach a bad guy in at most max hops
  gmt_for_each(bad_guys, 1, 0, NUM_BAD_GUYS, MarkBadGuys, & args, sizeof(MBG_args_t));

  gmt_free(bad_guys);
  gmt_free(Reverse.data);
  printf("Time to set up BFS = %lf\n", my_timer() - time1);
}
