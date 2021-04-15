#include "main.h"

/******************** ARGS ********************/
typedef struct rf_args_t {
  uint64_t lb;
  gmt_data_t data;
  gmt_data_t schema;
  gmt_data_t starts;
  gmt_data_t addresses;
  char prefix[PREFIX_SIZE];
} rf_args_t;

typedef struct UT_args_t {
  Table table;
  gmt_data_t schema;
  uint64_t columns[SORT_COLS];
} UT_args_t;

/******************** UTILITIES ********************/
void assign_schema_pair(std::string name, SchemaType type, uint64_t ndx, gmt_data_t table) {
  uint64_t entry[SCHEMA_NAME_WORDS + 1];
  memcpy(entry, name.c_str(), name.size() + 1);
  entry[SCHEMA_NAME_WORDS] = type;

  gmt_put(table, ndx, (void *) entry, 1);
}

std::vector <SchemaType> getSchemaTypes(uint64_t num_cols, gmt_data_t schema) {
  uint64_t coltype[SCHEMA_NAME_WORDS + 1];
  std::vector <SchemaType> schemaTypes(num_cols);

  for (uint64_t j = 0; j < num_cols; j ++) {
      gmt_get(schema, j, (void *) coltype, 1);
      schemaTypes[j] = (SchemaType) coltype[SCHEMA_NAME_WORDS];     // type is last word
  }

  return schemaTypes;
}

uint64_t findColumn(std::string name, gmt_data_t & schema) {
  gentry_t * ga = mem_get_gentry(schema);

  uint64_t ndx = 0;
  char * begin = (char *) ga->data;
  char * end   = (char *) (ga->data + ga->nbytes_loc);

  for (char * ptr = begin; ptr < end; ptr += SCHEMA_ELEMS_BYTES) {
      std::string colName(ptr);
      if (name == colName) return ndx; else ndx ++;
  }
  return ULLONG_MAX;
}

void printTable(uint64_t n, std::string name, Table & table) {
  uint64_t num_rows = table.num_rows;
  uint64_t num_cols = table.num_cols;
  uint64_t elem[SCHEMA_NAME_WORDS + 1];

  printf("Table: %s\n", name.c_str());
  printf("  Rows: %lu, Cols: %lu\n", num_rows, num_cols);
  printf("  Schema:\n");

  for (uint64_t i = 0; i < num_cols; i ++) {
      gmt_get(table.schema, i, (void *) elem, 1);
      printf("       %s\n", (char *) elem);
  }

  printf("  Records:\n");
  if ((n == 0) || (n > num_rows)) n = num_rows;
  std::vector <SchemaType> schematypes(num_cols);
  schematypes = getSchemaTypes(num_cols, table.schema);

  for (uint64_t i = 0; i < n; i ++) {
      std::vector <uint64_t>  record(num_cols);
      gmt_get(table.data, i, (void *) record.data(), 1);

      for (uint64_t j = 0; j < num_cols; j ++)
          printf("       %s\t", Uint_to_String(record[j], schematypes[j]).c_str());

      printf("\n");
} }


/******************** READ TABLE ********************/
void readTableFile(uint64_t it, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  rf_args_t * my_args = (rf_args_t *) args;

  gmt_data_t schema = my_args->schema;
  uint64_t num_cols = gmt_nelems_tot(schema);
  std::string filename = my_args->prefix + std::to_string(my_args->lb + it);
  std::vector <SchemaType> schemaTypes = getSchemaTypes(num_cols, my_args->schema);

  std::string line;
  std::ifstream file(filename);
  if (! file.is_open()) { printf("Cannot open file %s\n", filename.c_str()); exit(-1); }

// reserve space for data, assume 40 bytes per row
  file.seekg (file.end);
  std::vector <uint64_t> data;
  data.reserve( file.tellg() / 40 );

// reset file
  file.clear();
  file.seekg(0);

  while (getline(file, line)) {
     if (line[0] == '#') continue;     // skip comments
     std::string::iterator start = line.begin();

     for (uint64_t j = 0; j < num_cols; j ++) {
       std::string::iterator end = std::find(start, line.end(), ',');
       std::string field = std::string(start, end);

       data.push_back(String_to_Uint(field, schemaTypes[j]));
       start = end + 1;
  } }

  uint64_t num_rows = data.size() / num_cols;
  gmt_put_value(my_args->starts, it + 1, num_rows);
  gmt_put_value(my_args->addresses, it, (uint64_t) (data.data()));
  new (& data) std::vector <uint64_t>;     // reassign data to an empty vector to deallocate at end-of-scope
}


void moveTableFile(uint64_t it, uint64_t num_iter, const void * args, gmt_handle_t handle) {
  rf_args_t * my_args = (rf_args_t *) args;
  uint64_t start = gmt_get_value(my_args->starts, it);
  uint64_t num_rows = gmt_get_value(my_args->starts, it + 1) - start;

  void * address = (void *) gmt_get_value(my_args->addresses, it);
  gmt_put(my_args->data, start, address, num_rows);
  free(address);
}


void readTableFiles(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  double time1 = my_timer();
  RF_args_t * my_args = (RF_args_t *) args;

  uint64_t lb = my_args->lb;
  uint64_t ub = my_args->ub;
  uint64_t num_files = ub - lb + 1;
  uint64_t num_cols = gmt_nelems_tot(my_args->schema);
  gmt_data_t starts = gmt_alloc(num_files + 1, sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "");
  gmt_data_t addresses = gmt_alloc(num_files, sizeof(uint64_t *), GMT_ALLOC_PARTITION_FROM_ZERO, "");

  rf_args_t rfArgs;
  rfArgs.lb = lb;
  rfArgs.starts = starts;
  rfArgs.addresses = addresses;
  rfArgs.schema = my_args->schema;
  memcpy(rfArgs.prefix, my_args->prefix, PREFIX_SIZE);

  gmt_for_loop(num_files, 1, readTableFile, & rfArgs, sizeof(rf_args_t), GMT_SPAWN_SPREAD);
  printf("     all %s files read, time = %lf\n", my_args->prefix, my_timer() - time1);
  time1 = my_timer();


// first sum of table lengths
  uint64_t start = 0;
  gmt_put_value(starts, 0, start);

  for (uint64_t i = 1; i <= num_files; i ++) {
      start += gmt_get_value(starts, i);
      gmt_put_value(starts, i, start);
  }

// allocate return table
  uint64_t num_rows = start;
  rfArgs.data = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, my_args->prefix);
  gmt_for_loop(num_files, 1, moveTableFile, & rfArgs, sizeof(rf_args_t), GMT_SPAWN_SPREAD);

  Table newTable;
  newTable.data = rfArgs.data;
  newTable.num_rows = num_rows;
  newTable.num_cols = num_cols;

  sortTable(& newTable, my_args->columns);

  * ret_size = sizeof(gmt_data_t);
  ((gmt_data_t *) ret)[0] = newTable.data;

  gmt_free(starts);
  gmt_free(addresses);
  printf("     table for %s files created, time = %lf\n", my_args->prefix, my_timer() - time1);
}


/******************** SORT TABLE ********************/
void sortTable(Table * table, uint64_t columns [SORT_COLS]) {
  table->data = gmt_sort(table->data, columns);
}


/******************** REVERSE EDGE TABLE ********************/
void reverseEdge(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  gentry_t * ga = mem_get_gentry( * ((gmt_data_t *) args));

  for (uint64_t i = 0; i < ga->nbytes_loc; i += ga->nbytes_elem) {
    uint64_t src = * ((uint64_t *) (ga->data + i + 0));
    uint64_t dst = * ((uint64_t *) (ga->data + i + 8));
    * ((uint64_t *) (ga->data + i + 0)) = dst;     // src in new table is dst in table
    * ((uint64_t *) (ga->data + i + 8)) = src;     // dst in new table is src in table
} }

Table reverseEdgeTable(Table & table) {
  uint64_t num_rows = table.num_rows;
  uint64_t num_cols = table.num_cols;

  Table newTable;
  newTable.num_cols = num_cols;
  newTable.num_rows = num_rows;
  newTable.schema = table.schema;
  newTable.data = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "");

  uint64_t src[SCHEMA_NAME_WORDS + 1];
  uint64_t dst[SCHEMA_NAME_WORDS + 1];
  gmt_get(table.schema, 0, (void *) src, 1);        // get src column name and type of input table
  gmt_get(table.schema, 1, (void *) dst, 1);        // get dst column name and type of input table
  gmt_put(newTable.schema, 0, (void *) dst, 1);     // src in reverse table is dst of input table
  gmt_put(newTable.schema, 1, (void *) src, 1);     // dst in reverse table is src of input table 
  
  gmt_memcpy(table.data, 0, newTable.data, 0, num_rows);
  gmt_execute_on_all(reverseEdge, (void *) & newTable.data, sizeof(gmt_data_t), GMT_PREEMPTABLE);
  
  sortTable(& newTable, __cols01);
  return newTable;
}


/******************** SELECT TABLE ********************/
Table selectTable(std::vector <uint64_t> & keyRow,
     uint64_t selectColumns[SORT_COLS], uint64_t sortColumns[SORT_COLS], Table & table, std::string & name) {

  std::pair <uint64_t, uint64_t> edgeRange = gmt_equal_range(keyRow, table.data, selectColumns);
  uint64_t num_rows = edgeRange.second - edgeRange.first;
  uint64_t num_cols = table.num_cols;

  Table newTable;
  newTable.num_cols = num_cols;
  newTable.num_rows = num_rows;
  newTable.schema = table.schema;
  newTable.data = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, name.data());

  gmt_memcpy(table.data, edgeRange.first, newTable.data, 0, newTable.num_rows);
  if (sortColumns[0] != ULLONG_MAX) sortTable(& newTable, sortColumns);

  return newTable;
}


/******************** UINQUE TABLE ********************/
void _MakeUnique(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  UT_args_t * my_args = (UT_args_t *) args;
  gentry_t * ga = mem_get_gentry(my_args->table.data);

  uint64_t * columns = my_args->columns;
  uint64_t num_cols = my_args->table.num_cols;
  uint64_t * first_row = (uint64_t *) ga->data;
  uint64_t * last_row  = (uint64_t *) (ga->data + ga->nbytes_loc - ga->nbytes_elem);

  uint64_t node = gmt_node_id();
  std::vector <uint64_t> prev_row(num_cols);

  if (node == 0) {
     memcpy(prev_row.data(), first_row, ga->nbytes_elem);
     first_row += num_cols;
  } else {
     uint64_t first_index = ga->goffset_bytes / ga->nbytes_elem;
     gmt_get(my_args->table.data, first_index - 1, (void *) prev_row.data(), 1);
     gmt_execute_on_node_nb(node - 1, _MakeUnique, args, sizeof(UT_args_t), ret, ret_size, GMT_PREEMPTABLE);
  }

  for ( ; first_row <= last_row; first_row += num_cols) {
    bool duplicate = true;

    for (uint64_t * col = columns; * col != ULLONG_MAX; col ++)
      if (prev_row[* col] != first_row[* col]) {duplicate = false; break;}

    if (duplicate)
       for (uint64_t * col = columns; * col != ULLONG_MAX; col ++) first_row[* col] = ULLONG_MAX;
    else
       for (uint64_t * col = columns; * col != ULLONG_MAX; col ++) prev_row[* col] = first_row[* col];
  }

  gmt_wait_for_nb();
}


void makeUnique(Table * table, uint64_t columns[SORT_COLS], std::string name) {
  uint64_t ret;
  uint32_t retSize;
  uint64_t num_nodes = gmt_num_nodes();

  UT_args_t args;
  args.table = * table;
  memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));

  // since _makeUnique has to read last element on previous node, spawn tasks right-to-left
  gmt_execute_on_node(num_nodes - 1, _MakeUnique, & args, sizeof(UT_args_t), & ret, & retSize, GMT_PREEMPTABLE);

  sortTable(table, args.columns);

  uint64_t num_cols = table->num_cols;
  std::vector <uint64_t> duplicateRow(table->num_cols, ULLONG_MAX);
  uint64_t num_rows = gmt_lower_bound(duplicateRow, table->data, columns);

  gmt_data_t newData = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, name.data());
  gmt_memcpy(table->data, 0, newData, 0, num_rows);

  gmt_free(table->data);
  table->data = newData;
  table->num_rows = num_rows;
}


void _UniqueTable(const void * args, uint32_t args_size, void * ret, uint32_t * ret_size, gmt_handle_t handle) {
  UT_args_t * my_args = (UT_args_t *) args;

  Table table = my_args->table;
  gentry_t * ga = mem_get_gentry(table.data);
  uint64_t * first_row = (uint64_t *) ga->data;
  uint64_t * last_row  = (uint64_t *) (ga->data + ga->nbytes_loc - ga->nbytes_elem);

  std::set <uint64_t> idSet;
  std::vector <uint64_t> prev_row(table.num_cols, ULLONG_MAX);

  for ( ; first_row <= last_row; first_row += table.num_cols)
    for (uint64_t * col = my_args->columns; * col != ULLONG_MAX; col ++)
        if (prev_row[* col] != first_row[* col])
             {idSet.insert(first_row[* col]); prev_row[* col] = first_row[* col];}

  uint64_t num_rows = idSet.size();
  uint64_t num_cols = gmt_nelems_tot(my_args->schema);

  std::vector <uint64_t> buffer(num_rows * num_cols, ULLONG_MAX);
  gmt_data_t data = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, "_Unique");

  uint64_t i = 0;
  for (uint64_t id : idSet) {buffer[i * num_cols] = id; i++;}

  gmt_put(data, 0, (void *) buffer.data(), num_rows);
  * ret_size = sizeof(gmt_data_t);
  ((gmt_data_t *) ret)[0] = data;
}


// Construct a table of unique values in the selected columns in column 0 and ULLONG_MAX in the other columns
Table uniqueTable(uint64_t columns[SORT_COLS], gmt_data_t & schema, Table & table, std::string & name) {
  uint32_t retSize;
  uint64_t num_nodes = gmt_num_nodes();
  std::vector <gmt_data_t> tables(num_nodes);
  std::vector <uint64_t> starts(num_nodes + 1, 0);

  UT_args_t args;
  args.table = table;
  args.schema = schema;
  memcpy(args.columns, columns, SORT_COLS * sizeof(uint64_t));

  // on each node, construct gmt array of unique values in the selected columns and ULLONG_MAX in other columns
  // note, a value may appear in more than one table
  for (uint64_t i = 0; i < num_nodes; i ++)
    gmt_execute_on_node_nb(i, _UniqueTable, & args, sizeof(UT_args_t), & tables[i], & retSize, GMT_PREEMPTABLE);

  gmt_wait_for_nb();

  // first sum of table lengths
  for (uint64_t i = 0; i < num_nodes; i ++) starts[i + 1] = starts[i] + gmt_nelems_tot(tables[i]);

  uint64_t num_rows = starts[num_nodes];
  uint64_t num_cols = gmt_nelems_tot(schema);

  Table newTable;
  newTable.schema = schema;
  newTable.num_cols = num_cols;
  newTable.num_rows = num_rows;
  newTable.data = gmt_alloc(num_rows, num_cols * sizeof(uint64_t), GMT_ALLOC_PARTITION_FROM_ZERO, name.data());

  // copy tables to data
  for (uint64_t i = 0; i < num_nodes; i ++) {
    gmt_memcpy(tables[i], 0, newTable.data, starts[i], gmt_nelems_tot(tables[i]));
    gmt_free(tables[i]);
  }

  sortTable(& newTable, __cols0);            // sort table
  makeUnique(& newTable, __cols0, name);     // since two tables may have the same value, check for duplicates
  return newTable;
}
