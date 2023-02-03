#include "btree.c"
#include "journal.h"

typedef struct insert_query {
  const char* BTREE_name;
  unsigned int key;
  char* item;
  unsigned short item_len;
} insert_query;

typedef struct delete_query {
  const char* BTREE_name;
  unsigned int key;
} delete_query;

typedef struct all_items_query {
  const char* BTREE_name;
} all_items_query;

typedef struct get_items_range_query {
  const char* BTREE_name;
  unsigned int low_bound;
  unsigned int high_bound;
} get_items_range_query;

bool get_BTREE_item (char* BTREE_name, unsigned int key, char* row_buffer, unsigned short* item_len) {
  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  return btree_search(key, BTREE_name, row_buffer, item_len, conf);
}

vector* get_BTREE_item_key_range (char* BTREE_name, unsigned int low_bound, unsigned int high_bound) {
  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  return btree_range_row_scan(BTREE_name, low_bound, high_bound, conf);
}

void insert_BTREE_item (char* BTREE_name, unsigned int key, char* item, unsigned short item_len){
  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  return btree_insert(key, BTREE_name, item, item_len, conf);
}

bool delete_BTREE_item (char* BTREE_name, unsigned int key){
  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  return btree_delete(BTREE_name, key, conf);
}

void foreach_item (char* BTREE_name, bool (*func)(char* item, unsigned short len)) {

  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  btree_foreach(BTREE_name, func, conf);

}

chunk_iter* chunk_iterator (char* BTREE_name, unsigned int items_per_chunk, chunk_iter* iter) {

  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  return btree_chunk_iterator(BTREE_name, items_per_chunk, iter, conf);

}

bool update_BTREE_item (unsigned int key, char* BTREE_name, char* row, unsigned int row_size) {

  btree_config* conf = get_BTREE_root_page_id(BTREE_name);
  return btree_update(key, BTREE_name, row, row_size, conf);

}

void start_transaction () {
  journal_start_transaction();
}

void commit_transaction () {
  
  journal_commit_transaction();
}

void abort_transaction () {
  journal_rollback();
}