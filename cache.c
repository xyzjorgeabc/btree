#include "pager.c"
#include "btree.h"
#include <time.h>
#define CACHE_SIZE 500

typedef enum ACCESS_MODE {
  READ = 0,
  WRITE = 1
} ACCESS_MODE;

typedef struct hist_record {
  unsigned int page_id;
  bool lock;
  bool dirty;
  btree_node* bnode;
  struct hist_record* lru_next;
  struct hist_record* lru_prev;
} hist_record;

uint32_p_hash_table* hist;
hist_record* lru_head = NULL;
hist_record* lru_tail = NULL;
queue* cementery;

int total_tries = 0;
int total_misses = 0;
int total_nodes_created = 0;

int total_pages_read = 0;
int total_pages_written = 0;

int total_parse = 0;
int total_serialize = 0;

hist_record* init_hist_record (unsigned int page_id, btree_node* bnode) {

  hist_record* record = malloc(sizeof(hist_record));
  record->dirty = false;
  record->page_id = page_id;
  record->bnode = bnode;
  record->lru_next = NULL;
  record->lru_next = NULL;
  record->lock = false;
  return record;
}

hist_record* reset_hist_record (hist_record* record, unsigned int page_id) {

  record->dirty = false;
  record->page_id = page_id;

  return record;
}

void init_cache() {

  hist = init_uint32_p_hash_table(CACHE_SIZE);
  cementery = init_queue(CACHE_SIZE);

  init_pager();
}

bool is_cache_full() {
  return hist->n == hist->max_n;
}

void lru_insert (hist_record* record) {
  record->lru_next = lru_head;
  
  if (lru_head) lru_head->lru_prev = record;
  else lru_tail = record;
  
  lru_head = record;
}

void lru_delete (hist_record* record) {
  if (record == lru_head) lru_head = record->lru_next;
  else record->lru_prev->lru_next = record->lru_next;

  if (record == lru_tail) lru_tail = record->lru_prev;
  else record->lru_next->lru_prev = record->lru_prev;
}

void lru_reset () {
  lru_head = NULL;
  lru_tail = NULL;
}

void lru_update (hist_record* record) {
  if (lru_head == record) {
    return;
  }

  if (record == lru_tail) lru_tail = record->lru_prev;
  else record->lru_next->lru_prev = record->lru_prev;
  record->lru_prev->lru_next = record->lru_next;
  
  record->lru_next = lru_head;
  lru_head->lru_prev = record;
  lru_head = record;

}

hist_record* lru_update_and_reuse () {

  hist_record* cursor = lru_tail;
  assert(cursor != NULL);
  while (cursor->lock) cursor = cursor->lru_prev;

  if (cursor == lru_tail) lru_tail = cursor->lru_prev;
  else cursor->lru_next->lru_prev = cursor->lru_prev;
  
  cursor->lru_prev->lru_next = cursor->lru_next;

  cursor->lru_next = lru_head;
  lru_head->lru_prev = cursor;
  lru_head = cursor;

  return cursor;
}

btree_node* cache_acquire_node (unsigned int page_id, ACCESS_MODE mode, btree_config* conf) {

  hist_record* record;
  bool was_dirty = false;
  char* page_tmp;
  retry:
  record = uint32_p_ht_get(page_id, hist);
  total_tries++;
  
  if (record != NULL) {
    record->lock = true;
    lru_update(record);
    return record->bnode;
  }

  page_tmp = calloc(conf->page_size, 1);
  total_misses++;
  total_pages_read++;

  if (!queue_is_empty(cementery)) {
    record = queue_pop(cementery);
    reset_hist_record(record, page_id);
    lru_insert(record);
    free_btree_node(record->bnode);
  } 
  else if (!is_cache_full()) {
    record = init_hist_record(page_id, NULL);
    record->lock = true;
    lru_insert(record);
  } 
  else {

    record = lru_update_and_reuse();
    uint32_p_ht_delete(record->page_id, hist);

    //if(record->dirty){
    if(true){
      serialize_node(page_tmp, record->bnode, record->bnode->conf);
      pager_real_write(record->page_id, page_tmp);
      total_pages_written++;
    }
    free_btree_node(record->bnode);
    reset_hist_record(record, page_id);
  }

  uint32_p_ht_insert(page_id, record, hist);

  record->bnode = init_btree_node(conf);

  load_page(page_id, page_tmp);
  parse_node(page_tmp, page_id, record->bnode, conf);

  if (!(record->bnode->flags & IS_LEAF)){
    free(record->bnode->payload);
    record->bnode->payload = NULL;
  }

  free(page_tmp);
  return record->bnode;
}

void cache_free_node (unsigned int page_id, ACCESS_MODE mode, bool modified){
  hist_record* record = uint32_p_ht_get(page_id, hist);
  record->dirty = modified ? true : record->dirty;
  record->lock = false;
}

unsigned int  cache_write_new_node (btree_node* bnode) {
  
  total_nodes_created++;
  hist_record* record;
  unsigned int new_page_id = pager_reserve_page();
  bnode->page_id = new_page_id;
  
  char* page_tmp = calloc(1, bnode->conf->page_size);
  total_misses++;

  if (!queue_is_empty(cementery)){
    record = queue_pop(cementery);
    reset_hist_record(record, new_page_id);
    free_btree_node(record->bnode);
    record->bnode = bnode;
    uint32_p_ht_insert(new_page_id, record, hist);
    lru_insert(record);
  } else if (!is_cache_full()) {
    record = init_hist_record(new_page_id, NULL);
    record->bnode = bnode;
    record->dirty = true;
    uint32_p_ht_insert(new_page_id, record, hist);
    lru_insert(record);
  } else {

    record = lru_update_and_reuse();
    uint32_p_ht_delete(record->page_id, hist);
    //if(record->dirty){
    if(true){
      total_pages_written++;
      serialize_node(page_tmp, record->bnode, record->bnode->conf);
      pager_real_write(record->page_id, page_tmp);
    }
    reset_hist_record(record, new_page_id);
    free_btree_node(record->bnode);
    record->bnode = bnode;
    record->dirty = true;
    uint32_p_ht_insert(new_page_id, record, hist);
    record->lock = false;
  }
  free(page_tmp);
  return new_page_id;
}

void cache_delete_page (unsigned int node_id) {

  hist_record* record = uint32_p_ht_get(node_id, hist);

  if (record == NULL) return;
  
  lru_delete(record);
  uint32_p_ht_delete(node_id, hist);

  free_btree_node(record->bnode);
  free(record);
}

void cache_force_flush (unsigned int page_id) {
  hist_record* record = uint32_p_ht_get(page_id, hist);
  if (!record || record->dirty == false) return; // already flushed or untouched.
  
  record->dirty =  false;
  char* page = calloc(1, record->bnode->conf->page_size);
  serialize_node(page, record->bnode, record->bnode->conf);
  pager_real_write(page_id, page);
  free(page);
}

void cache_reset () {

  hist_record* rec = lru_head;

  if (rec == NULL) return;

  do {
    assert(!rec->dirty);
    uint32_p_ht_delete(rec->page_id, hist);
    free_btree_node(rec->bnode);
    free(rec->lru_prev);
  } while ((rec = rec->lru_next));

  free(rec);
  lru_reset();
  queue_empty(cementery);

}
