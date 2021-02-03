#include "main.h"
#include <numeric>

int gmt_main (uint64_t argc, char * argv[]) {
  if (argc != 7) {printf("Usage: <netfow file> <lb ub> <event file> <lb ub>\n"); return 1;}

  double time0 = gmt_timer();
  double time1 = gmt_timer();

/**********  SCHEMAS **********/
  gmt_data_t netflowTableSchema = gmt_alloc(11, SCHEMA_ELEMS_BYTES, GMT_ALLOC_REPLICATE, "netfowSchema");
  assign_schema_pair("src_device",  UINT,  0, netflowTableSchema);
  assign_schema_pair("dst_device",  UINT,  1, netflowTableSchema);
  assign_schema_pair("epoch_time",  UINT,  2, netflowTableSchema);
  assign_schema_pair("duration",    UINT,  3, netflowTableSchema);
  assign_schema_pair("protocol",    UINT,  4, netflowTableSchema);
  assign_schema_pair("src_port",    UINT,  5, netflowTableSchema);
  assign_schema_pair("dst_port",    UINT,  6, netflowTableSchema);
  assign_schema_pair("src_packets", UINT,  7, netflowTableSchema);
  assign_schema_pair("dst_packets", UINT,  8, netflowTableSchema);
  assign_schema_pair("src_bytes",   UINT,  9, netflowTableSchema);
  assign_schema_pair("dst_bytes",   UINT, 10, netflowTableSchema);

  gmt_data_t eventTableSchema = gmt_alloc(10, SCHEMA_ELEMS_BYTES, GMT_ALLOC_REPLICATE, "eventSchema");
  assign_schema_pair("event_id",            UINT,  0, eventTableSchema);
  assign_schema_pair("log_host",            UINT,  1, eventTableSchema);
  assign_schema_pair("epock_time",          UINT,  2, eventTableSchema);
  assign_schema_pair("user_name",           CHARS, 3, eventTableSchema);
  assign_schema_pair("domain_name",         CHARS, 4, eventTableSchema);
  assign_schema_pair("logon_id",            CHARS, 5, eventTableSchema);
  assign_schema_pair("process_name",        CHARS, 6, eventTableSchema);
  assign_schema_pair("process_id",          CHARS, 7, eventTableSchema);
  assign_schema_pair("parent_process_name", CHARS, 8, eventTableSchema);
  assign_schema_pair("parent_process_id",   CHARS, 9, eventTableSchema);

/**********  READ NETFLOW FILES **********/
  RF_args_t netflowArgs;
  gmt_data_t netflowReturn;
  uint32_t netflowReturnSize;
  std::string prefix = argv[1];

  netflowArgs.lb = std::stoull(argv[2]);
  netflowArgs.ub = std::stoull(argv[3]);
  netflowArgs.schema = netflowTableSchema;
  memcpy(netflowArgs.prefix, prefix.c_str(), prefix.size() + 1);
  memcpy(netflowArgs.columns, __cols01, SORT_COLS * sizeof(uint64_t));     // sort by src and dst

  gmt_execute_on_node_nb(0, readTableFiles, & netflowArgs,
      sizeof(RF_args_t), & netflowReturn, & netflowReturnSize, GMT_PREEMPTABLE);

/**********  READ EVENT FILES **********/
  RF_args_t eventArgs;
  gmt_data_t eventReturn;
  uint32_t eventReturnSize;

  prefix = argv[4];
  eventArgs.lb = std::stoull(argv[5]);
  eventArgs.ub = std::stoull(argv[6]);
  eventArgs.schema = eventTableSchema;
  memcpy(eventArgs.prefix, prefix.c_str(), prefix.size() + 1);
  memcpy(eventArgs.columns, __cols01, SORT_COLS * sizeof(uint64_t));     // sort by event id and log host

  gmt_execute_on_node_nb(1, readTableFiles, & eventArgs,
      sizeof(RF_args_t), & eventReturn, & eventReturnSize, GMT_PREEMPTABLE);

  gmt_wait_for_nb();      // wait for all reads to complete

/**********  CONSTRUCT NETWORK EDGE TABLE **********/
  Table Netflow; 
  Netflow.data = netflowReturn;
  Netflow.schema = netflowTableSchema;
  Netflow.num_rows = gmt_nelems_tot(Netflow.data);
  Netflow.num_cols = gmt_nelems_tot(netflowTableSchema);

/**********  CONSTRUCT EVENT EDGE TABLE **********/
  Table Events;
  Events.data = eventReturn;
  Events.schema = eventTableSchema;
  Events.num_rows = gmt_nelems_tot(Events.data);
  Events.num_cols = gmt_nelems_tot(eventTableSchema);

  printf("Time to read files = %lf\n", my_timer() - time1);
  time1 = my_timer();

/**********  CREATE NETFLOW SERVER TABLE **********/
  std::string serverTableName = "Servers";
  gmt_data_t serverTableSchema = gmt_alloc(2, SCHEMA_ELEMS_BYTES, GMT_ALLOC_REPLICATE, "serverSchema");
  assign_schema_pair("server", UINT,  0, serverTableSchema);
  assign_schema_pair("hops",   UINT,  1, serverTableSchema);
  Table Servers = uniqueTable(__cols01, serverTableSchema, Netflow, serverTableName);

  printf("Time to construct server table = %lf\n", my_timer() - time1);
  time1 = my_timer();

/**********  CREATE EVENT SPECIFIC EDGE TABLES **********/
  std::string bootTableName = "Boots";
  std::string processesTableName = "Processes";
  std::vector <uint64_t> bootKey = {4608};
  std::vector <uint64_t> processKey = {4688};
  Table Boots = selectTable(bootKey, __cols0, __cols_, Events, bootTableName);              // select events
  Table Processes = selectTable(processKey, __cols0, __cols_, Events, processesTableName);  // select processes

  printf("Time to construct event specific tables = %lf\n", my_timer() - time1);

/**********  CREATE GRAPH **********/
  Graph_t graph;
  graph.insert(std::pair <std::string, Table *> ("Netflow", & Netflow));
  graph.insert(std::pair <std::string, Table *> ("Events", & Events));
  graph.insert(std::pair <std::string, Table *> ("Servers", & Servers));
  graph.insert(std::pair <std::string, Table *> ("Boots", & Boots));
  graph.insert(std::pair <std::string, Table *> ("Processes", & Processes));

  printf("Time to construct graph = %lf\n", my_timer() - time0);

  printf("     Number of Netflow records = %lu\n", Netflow.num_rows);
  printf("     Number of Event   records = %lu\n", Events.num_rows);
  printf("     Number of Network servers = %lu\n", Servers.num_rows);
  printf("     Number of Boot    events  = %lu\n", Boots.num_rows);
  printf("     Number of Process events  = %lu\n", Processes.num_rows);

  BFS_Setup(graph);

  // checkSort("Netflow", Netflow.data, __cols01);
  // checkSort("Events", Events.data, __cols01);
  // checkSort("Boots", Boots.data, __cols01);
  // checkSort("Servers", Servers.data, __cols0);
  // checkSort("Processes", Processes.data, __cols01);
  // printTable(100, "Netflow", Netflow);
  // printTable(100, "Events", Events);
  // printTable(100, "Servers", Servers);
  // printTable(100, "Boots", Boots);
  // printTable(100, "Processes", Processes);

  gmt_free(Netflow.data);
  gmt_free(Events.data);
  gmt_free(Servers.data);
  gmt_free(Boots.data);
  gmt_free(Processes.data);
  gmt_free(Netflow.schema);
  gmt_free(Events.schema);
  gmt_free(Servers.schema);
}
