#ifndef MAIN_H 
#define MAIN_H

#include <map>
#include <set>
#include <mutex>
#include <regex>
#include <deque>
#include <string>
#include <vector>
#include <climits>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <algorithm>

#include "gmt/gmt.h"
#include "gmt/memory.h"
#include "sort.h"

extern "C" int gmt_main (uint64_t argc, char * argv[] );

#define SKIP 2048
#define PREFIX_SIZE 80
#define SCHEMA_NAME_WORDS 3
#define SCHEMA_ELEMS_BYTES ((SCHEMA_NAME_WORDS + 1) * sizeof(uint64_t))

#define NUM_SEEDS 1000
#define MAX_HORIZONS 7
#define NUM_BAD_GUYS 298

enum SchemaType {
  STRING,
  CHARS,
  UINT,
  INT,
  DOUBLE,
  COMPLEX,
  BOOL,
  DATE,
  USDATE,
  DATETIME,
  IPADDRESS,
  LIST_UINT,
  LIST_INT,
  LIST_DOUBLE,
  NONE
};

typedef struct RF_args_t {
  uint64_t lb;
  uint64_t ub;
  gmt_data_t schema;
  char prefix[PREFIX_SIZE];
  uint64_t columns[SORT_COLS];
} RF_args_t;

typedef struct Table {
   uint64_t num_rows;
   uint64_t num_cols;
   gmt_data_t schema;
   gmt_data_t data;
} Table;

typedef std::map <std::string, Table *> Graph_t;

static uint64_t __cols_   [SORT_COLS] = {ULLONG_MAX};
static uint64_t __cols0   [SORT_COLS] = {0, ULLONG_MAX};
static uint64_t __cols1   [SORT_COLS] = {1, ULLONG_MAX};
static uint64_t __cols01  [SORT_COLS] = {0, 1, ULLONG_MAX};
static uint64_t __cols10  [SORT_COLS] = {1, 0, ULLONG_MAX};
static uint64_t __cols02  [SORT_COLS] = {0, 2, ULLONG_MAX};
static uint64_t __cols012 [SORT_COLS] = {0, 1, 2, ULLONG_MAX};
typedef bool (* Comparator) (const uint64_t *, const uint64_t *);

std::vector <SchemaType> getSchemaTypes(gmt_data_t);
void assign_schema_pair(std::string, SchemaType, uint64_t ndx, gmt_data_t table);

uint64_t IP_to_Uint(std::string &);
std::string Uint_to_String(uint64_t, SchemaType);
uint64_t String_to_Uint(std::string &, SchemaType);

Table reverseEdgeTable(Table &);
void sortTable(Table *, uint64_t *);
uint64_t findColumn(std::string, gmt_data_t &);
void printTable(uint64_t, std::string, Table &);
void readTableFiles(const void *, uint32_t, void *, uint32_t *, uint32_t);
Table uniqueTable(uint64_t [SORT_COLS], gmt_data_t &, Table &, std::string &);
Table selectTable(std::vector <uint64_t> &, uint64_t [SORT_COLS], uint64_t [SORT_COLS], Table &, std::string &);

void BFS_Setup(Graph_t &);

#endif /* MAIN_H */
