#ifndef A_C
#define A_C
#include "pager.h"
#include "btree.h"
void serialize_db_header (char hbuff[DB_HEADER_SIZE]);
void write_db_header ();
void _load_db_header ();

typedef struct table_root_info {
  char* table_name;
  int name_len;
  unsigned int root_pointer;
  btree_config conf;

} table_root_info;

#endif