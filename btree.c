#include <assert.h>
#include "cache.c"
#include "journal.h"
#include "journal.c"

typedef struct item {
  unsigned int key;
  unsigned short len;
  char* data;
} item;

typedef struct chunk_iter {
  unsigned int size;
  unsigned int n_items;
  item* items;
} chunk_iter;

chunk_iter* init_chunk_iter (unsigned int chunk_size) {

  chunk_iter* iter = malloc(sizeof(chunk_iter));
  iter->items = malloc(sizeof(item)*chunk_size);
  iter->n_items = 0;
  iter->size = chunk_size;
  return iter;
}

typedef struct btree_cursor {
  unsigned int node_id;
  btree_node* bnode_p;
  char table_name[ROOT_NAME_MAX_SIZE];
  rstack* path;
} btree_cursor;

btree_cursor* init_btree_cursor (const char* table_name) {
  btree_cursor* cursor = malloc(sizeof(btree_cursor));
  cursor->path = init_rstack(1024);
  cursor->node_id = 0;
  cursor->bnode_p = NULL;
  strcpy(cursor->table_name, table_name);
  return cursor;
}

void free_cursor (btree_cursor* cursor) {
  free_rstack(cursor->path);
  free(cursor);
}

void empty_btree_node (btree_node* node, btree_config* conf) {
  memset(node->slots, 0, conf->slots_size);
  node->n_keys = 0;
  node->n_pointers = 0;
}

btree_node* init_btree_node (btree_config* conf) {

  btree_node* bnode = calloc(sizeof(btree_node), 1);
  bnode->page_keys = calloc(conf->max_keys * conf->key_size, 1);
  bnode->page_pointers = calloc(conf->max_cells * conf->pointer_size, 1);
  bnode->payload = calloc(conf->payload_size, 1);
  bnode->slots = calloc(conf->slots_size, 1);
  bnode->conf = conf;
  return bnode;
}

void free_btree_node (btree_node* node) {

  free(node->page_keys);
  free(node->page_pointers);
  if (node->payload) free(node->payload);
  free(node->slots);
  free(node);

}

void serialize_node (char* page, btree_node* node, btree_config* conf) {

  total_serialize++;
  write_short(page, node->flags);
  write_short(page+= conf->flags_size , node->n_keys);
  write_short(page+= conf->n_keys_size, node->n_pointers);
  write_int(page+=conf->n_pointers_size, node->next);
  page+=conf->pointer_size; //next_pointer

  for(int i = 0; i < node->n_keys; i++ ) {
    write_int(page, node->page_keys[i]);
    page+=conf->key_size;
  }

  for(int i = 0; i < node->n_pointers; i++ ) {
    write_int(page, node->page_pointers[i]);
    page+=conf->pointer_size;
  }

  if (node->flags & IS_LEAF) memcpy(page, node->payload, conf->payload_size);
}

void parse_node (char* page, unsigned int page_id, btree_node* node, btree_config* conf) {
  total_parse++;
  node->flags = bytes_to_short(page);
  node->n_keys = bytes_to_short(page+=conf->flags_size);
  node->n_pointers = bytes_to_short(page+=conf->n_keys_size);
  node->next = bytes_to_int(page+=conf->n_pointers_size);
  page+=conf->pointer_size; //next pointer

  for(int i = 0; i < node->n_keys; i++) {
    node->page_keys[i] = bytes_to_int(page);
    page+=conf->key_size;
  }
  memset(node->slots, 0, conf->slots_size);
  for(int i = 0; i < node->n_pointers; i++) {
    node->page_pointers[i] = bytes_to_int(page);
    node->slots[(node->page_pointers[i]/conf->cell_item_max_size)/8] |= (1 << (node->page_pointers[i]/conf->cell_item_max_size) % 8);
    page+=conf->pointer_size;
  }

  node->page_id = page_id;
  if (node->flags & IS_LEAF) memcpy(node->payload, page, conf->payload_size);
}

unsigned short reserve_first_empty_slot (unsigned char* slots, unsigned short n_slots) {

  for (int i = 0, j = 0; i < n_slots; i++) {
    if (slots[i] == 255) continue;

    while (j >= 0) { ///??????????????? I DONT REMEMBER WHY IS THIS NOT JUST TRUE LOL
      if (!(slots[i] & 1 << j)) {
        slots[i] |= (1 << j);
        return (i*8) + j;
      }
      j++;
    }
  }
  return 0;
}

void free_slot (unsigned char* slots, unsigned short i) {

  int slot_index = i/8;
  int bit_index = i%8;

  slots[slot_index] ^= (1 << bit_index);
}

btree_node* load_node (unsigned int page_id, ACCESS_MODE mode, btree_config* conf) {
  return cache_acquire_node(page_id, mode, conf);
}

btree_node* get_parent_node (btree_cursor* cursor) {
  return rstack_peek(cursor->path);
}

void move_to_parent_node (btree_cursor* cursor) {
  cursor->bnode_p = rstack_pop(cursor->path);
  cursor->node_id = cursor->bnode_p->page_id;
}

btree_node* get_left_sibling (btree_cursor* cursor, btree_node* parent_node, int* sibling_pointer_index_in_parent, btree_config* conf) {
  //TODO: SLOW SEARCH
  *sibling_pointer_index_in_parent = unordered_search(parent_node->page_pointers, 0, parent_node->n_pointers-1, cursor->node_id)-1;
  
  if (*sibling_pointer_index_in_parent == -1) return NULL;
  return load_node(parent_node->page_pointers[*sibling_pointer_index_in_parent], WRITE, conf);
}

btree_node* get_right_sibling (btree_cursor* cursor, unsigned int* node_id, btree_config* conf) {
  
  btree_node* parent_node = rstack_peek(cursor->path);

  int node_pointer_index = bin_search(parent_node->page_pointers, 0, parent_node->n_pointers-1, cursor->node_id)+1;
  
  if (node_pointer_index >= cursor->bnode_p->n_pointers) return NULL;

  *node_id = parent_node->page_pointers[node_pointer_index];
  return load_node(*node_id, WRITE, conf);
}

void move_direct_sibling (btree_cursor* cursor, btree_node* node, unsigned int node_id) {
  cursor->bnode_p = node;
  cursor->node_id = node_id;
}

btree_cursor* search_tree_to_delete (unsigned int key, char* table_name) {

  btree_config* conf = get_BTREE_root_page_id(table_name);
  unsigned int root_id = conf->root;
  assert(root_id >= 1);
  
  btree_cursor* cursor = init_btree_cursor(table_name);
  cursor->node_id = root_id;
  cursor->bnode_p = load_node(root_id, WRITE, conf);
  
  while (!(cursor->bnode_p->flags & IS_LEAF)) {

    int ind = first_greater_search(cursor->bnode_p->page_keys, cursor->bnode_p->n_keys, key);
    rstack_push(cursor->path, cursor->bnode_p);
    cursor->node_id = cursor->bnode_p->page_pointers[ind != -1 ? ind : cursor->bnode_p->n_pointers-1];
    cursor->bnode_p = load_node(cursor->node_id, WRITE, conf);
    
    if (cursor->bnode_p->n_keys > conf->min_keys) {
      while (!rstack_empty(cursor->path))
        cache_free_node(((btree_node*)rstack_pop(cursor->path))->page_id, WRITE, false);
    }
  }

  return cursor;
}

btree_cursor* search_tree_to_insert (unsigned int key, char* table_name) {

  btree_config* conf = get_BTREE_root_page_id(table_name);
  unsigned int root_id = conf->root;
  assert(root_id >= 1);
  
  btree_cursor* cursor = init_btree_cursor(table_name);
  cursor->node_id = root_id;
  cursor->bnode_p = load_node(root_id, WRITE, conf);
  
  while (!(cursor->bnode_p->flags & IS_LEAF)) {

    int ind = first_greater_search(cursor->bnode_p->page_keys, cursor->bnode_p->n_keys, key);
    rstack_push(cursor->path, cursor->bnode_p);
    cursor->node_id = cursor->bnode_p->page_pointers[ind != -1 ? ind : cursor->bnode_p->n_pointers-1];
    cursor->bnode_p = load_node(cursor->node_id, WRITE, conf);

    if (cursor->bnode_p->n_keys < conf->max_keys) {
      while (!rstack_empty(cursor->path))
        cache_free_node(((btree_node*)rstack_pop(cursor->path))->page_id, WRITE, false);
    }
  }

  return cursor;
}

btree_cursor* search_tree_to_update (unsigned int key, const char* table_name) {

  btree_config* conf = get_BTREE_root_page_id(table_name);
  unsigned int root_id = conf->root;

  btree_cursor* cursor = init_btree_cursor(table_name);
  cursor->node_id = root_id;
  cursor->bnode_p = load_node(root_id, WRITE, conf);
  unsigned int parent_node_id;

  while (!(cursor->bnode_p->flags & IS_LEAF)) {
    parent_node_id = cursor->node_id;
    int ind = first_greater_search(cursor->bnode_p->page_keys, cursor->bnode_p->n_keys, key);
    cursor->node_id = cursor->bnode_p->page_pointers[ind != -1 ? ind : cursor->bnode_p->n_pointers-1];
    cursor->bnode_p = load_node(cursor->node_id, WRITE, conf);
    cache_free_node(parent_node_id, WRITE, false);
  }

  return cursor;

}

btree_cursor* search_tree (unsigned int key, const char* table_name) {

  btree_config* conf = get_BTREE_root_page_id(table_name);
  unsigned int root_id = conf->root;

  btree_cursor* cursor = init_btree_cursor(table_name);
  cursor->node_id = root_id;
  cursor->bnode_p = load_node(root_id, READ, conf);
  unsigned int parent_node_id;

  while (!(cursor->bnode_p->flags & IS_LEAF)) {
    parent_node_id = cursor->node_id;
    int ind = first_greater_search(cursor->bnode_p->page_keys, cursor->bnode_p->n_keys, key);
    cursor->node_id = cursor->bnode_p->page_pointers[ind != -1 ? ind : cursor->bnode_p->n_pointers-1];
    cursor->bnode_p = load_node(cursor->node_id, READ, conf);
    cache_free_node(parent_node_id, READ, false);
  }

  return cursor;

}

 unsigned short get_row (const char* payload, unsigned int pointer, char* row, btree_config* conf) {
  unsigned short row_size = bytes_to_short(payload+pointer);
  assert(row_size == 1021);
  memcpy(row, payload+pointer+conf->cell_data_size_size+conf->cell_overflow_size, row_size);
  row[row_size] = '\0';
  return row_size;
}

void get_sibling_to_merge (unsigned int id, int* sibling_id, stack* st, bool side, btree_config* conf) {

  *sibling_id = -1;
  
  if(stack_empty(st)) return;
  
  int parent_id = stack_peek(st);
  btree_node* parent_node = load_node(parent_id, WRITE, conf);
  int id_index = bin_search(parent_node->page_pointers, 0, parent_node->n_pointers, id);
  
  if (!side && id_index != 0) {
  // can get left
    *sibling_id = parent_node->page_pointers[id_index-1];
  } else if (side && id_index != parent_node->n_pointers-1) {
  // can get right
    *sibling_id = parent_node->page_pointers[id_index+1];
  };
}

void insert_in_leaf (btree_node* node, unsigned int key, char* row, unsigned short row_size, btree_config* conf) {
  // inserts a row in a leaf node.
  //TODO: DOES NOT CARE ABOUT OVERFLOW

  unsigned int cpy_start = reserve_first_empty_slot(node->slots, conf->slots_size) * conf->cell_item_max_size;
  assert(cpy_start >= 0);

  int new_index = insert_in_order(node->page_keys, node->n_keys, key);
  node->n_keys++;
  insert_in_index(node->page_pointers, node->n_pointers, new_index, cpy_start);
  node->n_pointers++;
  write_short(node->payload+cpy_start, row_size);
  write_short(node->payload+cpy_start+ conf->cell_data_size_size, 0);
  memcpy(node->payload+cpy_start+conf->cell_data_size_size+conf->cell_overflow_size, row, row_size);
  
}

//always merges into right_node
void merge_branch (btree_node* left_node, btree_node* right_node, unsigned int lowest_key_right_node, btree_config* conf) {

  unsigned int* tmp_buffer = malloc(conf->max_cells * sizeof(unsigned int));

  memcpy(tmp_buffer, left_node->page_keys, left_node->n_keys * sizeof(unsigned int));
  memcpy(tmp_buffer + left_node->n_keys, &lowest_key_right_node, 1 * sizeof(unsigned int));
  memcpy(tmp_buffer + left_node->n_keys + 1, right_node->page_keys, right_node->n_keys * sizeof(unsigned int));
  memcpy(right_node->page_keys, tmp_buffer, (right_node->n_keys + left_node->n_keys + 1) * sizeof(unsigned int));

  memcpy(tmp_buffer, left_node->page_pointers, left_node->n_pointers * sizeof(unsigned int));
  memcpy(tmp_buffer + left_node->n_pointers, right_node->page_pointers, right_node->n_pointers * sizeof(unsigned int));
  memcpy(right_node->page_pointers, tmp_buffer, (right_node->n_pointers + left_node->n_pointers) * sizeof(unsigned int));

  right_node->n_keys += left_node->n_keys + 1;
  right_node->n_pointers += left_node->n_pointers;

  free(tmp_buffer);
}

unsigned int borrow_from_left_branch (btree_node* left_node, btree_node* right_node, unsigned int lowest_key_right_node, btree_config* conf) {
  //return rights new lowest key

  int keys_to_borrow = left_node->n_keys - conf->min_keys > 2 ? (left_node->n_keys - conf->min_keys) / 2 : 1;
  unsigned int new_lowest_key_right_node = left_node->page_keys[left_node->n_keys - (keys_to_borrow)];

  for (int i = right_node->n_keys-1; i >= 0; i--) {
    right_node->page_keys[i+keys_to_borrow] = right_node->page_keys[i];
  }

  for (int i = right_node->n_pointers-1; i >= 0; i--) {
    right_node->page_pointers[i+keys_to_borrow] = right_node->page_pointers[i];
  }

  right_node->page_keys[keys_to_borrow-1] = lowest_key_right_node;

  memcpy(right_node->page_keys, left_node->page_keys+(left_node->n_keys - keys_to_borrow)+1, (keys_to_borrow-1) * sizeof(unsigned int));
  memcpy(right_node->page_pointers, left_node->page_pointers+(left_node->n_pointers - keys_to_borrow), keys_to_borrow * sizeof(unsigned int));

  left_node->n_keys -= keys_to_borrow;
  left_node->n_pointers -= keys_to_borrow;

  right_node->n_keys += keys_to_borrow;
  right_node->n_pointers += keys_to_borrow;

  return new_lowest_key_right_node;
}

unsigned int borrow_from_right_branch (btree_node* left_node, btree_node* right_node, unsigned int lowest_key_right_node, btree_config* conf) {
  //returns rights new lowest key 
  int keys_to_borrow = (right_node->n_keys - (conf->min_keys)) / 2;
  unsigned int new_lowest_key_right_node = right_node->page_keys[keys_to_borrow-1];
  
  left_node->page_keys[left_node->n_keys] = lowest_key_right_node;

  memcpy(left_node->page_keys + left_node->n_keys+1, right_node->page_keys, keys_to_borrow * sizeof(unsigned int));
  memcpy(left_node->page_pointers + left_node->n_pointers, right_node->page_pointers, (keys_to_borrow+1) * sizeof(unsigned int));

  for (int i = keys_to_borrow; i < right_node->n_keys; i++) {
    right_node->page_keys[i-keys_to_borrow] = right_node->page_keys[i];
    right_node->page_pointers[i-(keys_to_borrow)] = right_node->page_pointers[i];
  }
  right_node->page_pointers[right_node->n_pointers-keys_to_borrow-1] = right_node->page_pointers[right_node->n_pointers-1];
  
  left_node->n_keys += keys_to_borrow;
  left_node->n_pointers += keys_to_borrow;

  right_node->n_keys -= keys_to_borrow;
  right_node->n_pointers -= keys_to_borrow;

  return new_lowest_key_right_node;
}

void delete_from_leaf_only (btree_node* node, unsigned int key, btree_config* conf) {

  int index = bin_search(node->page_keys, 0, node->n_keys-1, key);
  unsigned int row_bytes = bytes_to_short(node->payload+node->page_pointers[index]);
  
  free_slot(node->slots, node->page_pointers[index]/conf->cell_item_max_size);

  delete_in_order(node->page_keys, node->n_keys, index);
  delete_in_order(node->page_pointers, node->n_pointers, index);
  node->n_keys--;
  node->n_pointers--;
}
// always merge into right node
void merge_on_right_leaf (btree_node* left_node, btree_node* right_node , btree_config* conf) {

  for (int i = 0; i < left_node->n_pointers; i++) {
    insert_in_leaf(
      right_node, left_node->page_keys[i],
      left_node->payload + left_node->page_pointers[i] + conf->cell_data_size_size + conf->cell_overflow_size,
      bytes_to_short(left_node->payload + left_node->page_pointers[i]),
      conf
    );
  }

}
// always merge into left node
void merge_on_left_leaf (btree_node* left_node, btree_node* right_node, btree_config* conf) {

  for (int i = 0; i < right_node->n_pointers; i++) {
    insert_in_leaf(
      left_node, right_node->page_keys[i],
      right_node->payload + right_node->page_pointers[i] + conf->cell_data_size_size + conf->cell_overflow_size,
      bytes_to_short(right_node->payload + right_node->page_pointers[i]),
      conf
    );
  }
}

void borrow_from_left_sibling_leaf (btree_node* borrower_node, btree_node* victim_node, btree_config* conf) {

  int n_keys_to_borrow = (victim_node->n_keys - (conf->min_keys))/2;

  for (int i = 0; i < n_keys_to_borrow; i++) {
    insert_in_leaf(
      borrower_node,
      victim_node->page_keys[victim_node->n_keys-1],
      victim_node->payload + victim_node->page_pointers[victim_node->n_keys-1] + conf->cell_data_size_size + conf->cell_overflow_size,
      bytes_to_short(victim_node->payload + victim_node->page_pointers[victim_node->n_keys-1]),
      conf
    );
    delete_from_leaf_only(victim_node, victim_node->page_keys[victim_node->n_keys-1], conf);
  }
}

void borrow_from_right_sibling_leaf (btree_node* borrower_node, btree_node* victim_node, btree_config* conf) {

  int n_keys_to_borrow = (victim_node->n_keys - (conf->min_keys))/2;

  for (int i = 0; i < n_keys_to_borrow; i++) {
    insert_in_leaf(
      borrower_node,
      victim_node->page_keys[0],
      victim_node->payload + victim_node->page_pointers[0] + conf->cell_data_size_size + conf->cell_overflow_size,
      bytes_to_short(victim_node->payload + victim_node->page_pointers[0]),
      conf
    );
    delete_from_leaf_only(victim_node, victim_node->page_keys[0], conf);
  }

}

void delete_pointer_from_branch (btree_node* node, int pointer_index) {
  int key_index = pointer_index == 0 ? 0 : pointer_index-1;
  delete_in_order(node->page_pointers, node->n_pointers, pointer_index);
  delete_in_order(node->page_keys, node->n_keys, key_index);

  node->n_pointers--;
  node->n_keys--;
}

void balance_node (btree_cursor* cursor, btree_config* conf) {

  if (cursor->bnode_p->n_keys >= conf->min_keys) {
    cache_free_node(cursor->node_id, WRITE, true);
    return;
  }
  else if (rstack_empty(cursor->path) && cursor->bnode_p->n_pointers == 1) {
    journal_write_header();
    update_BTREE_root_page_id(cursor->table_name, cursor->bnode_p->page_pointers[0]);
    cache_delete_page(cursor->node_id);
    return;
  } 
  else if (rstack_empty(cursor->path)) {
    cache_free_node(cursor->node_id, WRITE, true);
    return;
  }

  unsigned int node_id = cursor->node_id;
  btree_node* parent_node = get_parent_node(cursor);
  unsigned int parent_node_id = parent_node->page_id;
  
  int sibling_pointer_index_in_parent;
  btree_node* sibling_node = get_left_sibling(cursor, parent_node, &sibling_pointer_index_in_parent, conf);
  int sibling_key_index_in_parent = sibling_pointer_index_in_parent-1;

  if (sibling_node != NULL) {
    
    journal_write_page(parent_node);
    journal_write_page(sibling_node);
    journal_write_page(cursor->bnode_p);

    if (sibling_node->n_keys > conf->min_keys) {
      //borrow from left
      parent_node->page_keys[sibling_key_index_in_parent+1] = borrow_from_left_branch(sibling_node, cursor->bnode_p, parent_node->page_keys[sibling_key_index_in_parent+1], conf);
      //cache_free_node(parent_node_id, WRITE, true);
      cache_free_node(sibling_node->page_id, WRITE, true);
      cache_free_node(cursor->node_id, WRITE, true);
    }
    else {
      // merge with left
      merge_branch(sibling_node, cursor->bnode_p, parent_node->page_keys[sibling_key_index_in_parent+1], conf);
      parent_node->page_keys[sibling_key_index_in_parent+1] = parent_node->page_keys[sibling_key_index_in_parent];
      delete_pointer_from_branch(parent_node, sibling_pointer_index_in_parent);
      move_to_parent_node(cursor);
      balance_node(cursor, conf);
      cache_delete_page(sibling_node->page_id);
      cache_free_node(node_id, WRITE, true);
    }
  }
  else {
    sibling_pointer_index_in_parent += 2;
    sibling_key_index_in_parent = sibling_pointer_index_in_parent-1;
    sibling_node = load_node(parent_node->page_pointers[sibling_pointer_index_in_parent], WRITE, conf);

    journal_write_page(parent_node);
    journal_write_page(sibling_node);
    journal_write_page(cursor->bnode_p);

    if (sibling_node->n_keys > conf->min_keys) {
      //borrow from right
      parent_node->page_keys[sibling_key_index_in_parent] = borrow_from_right_branch(cursor->bnode_p, sibling_node, parent_node->page_keys[sibling_key_index_in_parent], conf);
      //cache_free_node(parent_node_id, WRITE, true);
      cache_free_node(sibling_node->page_id, WRITE, true);
      cache_free_node(cursor->node_id, WRITE, true);
    }
    else {
      // merge with right
      merge_branch(cursor->bnode_p, sibling_node, parent_node->page_keys[sibling_key_index_in_parent], conf);
      parent_node->page_keys[sibling_key_index_in_parent] = parent_node->page_keys[sibling_key_index_in_parent];
      delete_pointer_from_branch(parent_node, sibling_pointer_index_in_parent-1);
      move_to_parent_node(cursor);
      balance_node(cursor, conf);
      cache_delete_page(node_id);
      cache_free_node(sibling_node->page_id, WRITE, true);
    }
  }
}

void delete_from_leaf (btree_cursor* cursor, unsigned int key, btree_config* conf){

  delete_from_leaf_only(cursor->bnode_p, key, conf);

  if (cursor->bnode_p->n_keys >=  conf->min_keys) {
    journal_write_page(cursor->bnode_p);
    cache_free_node(cursor->node_id, WRITE, true);
    free_cursor(cursor);
    return;
  }
  
  unsigned int node_id = cursor->node_id;
  btree_node* parent_node = get_parent_node(cursor);
  unsigned int parent_node_id = parent_node->page_id;

  int sibling_pointer_index_in_parent;
  btree_node* sibling_node = get_left_sibling(cursor, parent_node, &sibling_pointer_index_in_parent, conf);
  int sibling_key_index_in_parent = sibling_pointer_index_in_parent-1;

  if (sibling_node != NULL) {
    // has left sibling
    journal_write_page(parent_node);
    journal_write_page(sibling_node);
    journal_write_page(cursor->bnode_p);

    if (sibling_node->n_keys > conf->min_keys) {
      // can borrow
      borrow_from_left_sibling_leaf(cursor->bnode_p, sibling_node, conf);
      parent_node->page_keys[sibling_key_index_in_parent+1] = cursor->bnode_p->page_keys[0];
      //cache_free_node(parent_node_id, WRITE, true);
      cache_free_node(sibling_node->page_id, WRITE, true);
      cache_free_node(cursor->node_id, WRITE, true);
    }
    else {
      // cant borrow, merge
      sibling_node->next = cursor->bnode_p->next;
      merge_on_left_leaf(sibling_node, cursor->bnode_p, conf);
      delete_pointer_from_branch(parent_node, sibling_pointer_index_in_parent+1);
      move_to_parent_node(cursor);
      balance_node(cursor, conf);
      cache_delete_page(node_id);
      cache_free_node(sibling_node->page_id, WRITE, true);
    }
  }
  else {
    // no left sibling, use right one
    sibling_pointer_index_in_parent += 2;
    sibling_key_index_in_parent = sibling_pointer_index_in_parent-1;
    sibling_node = load_node(parent_node->page_pointers[sibling_pointer_index_in_parent], WRITE, conf);

    journal_write_page(parent_node);
    journal_write_page(sibling_node);
    journal_write_page(cursor->bnode_p);

    if (sibling_node->n_pointers > conf->min_keys) {
      // can borrow
      borrow_from_right_sibling_leaf(cursor->bnode_p, sibling_node, conf);
      parent_node->page_keys[sibling_key_index_in_parent] = sibling_node->page_keys[0];
      //cache_free_node(parent_node_id, WRITE, true);
      cache_free_node(sibling_node ->page_id, WRITE, true);
      cache_free_node(cursor->node_id, WRITE, true);
    }
    else {
      // cant borrow, merge
      cursor->bnode_p->next = sibling_node->next;
      merge_on_left_leaf(cursor->bnode_p, sibling_node, conf);
      delete_pointer_from_branch(parent_node, sibling_pointer_index_in_parent);
      move_to_parent_node(cursor);
      balance_node(cursor, conf);
      cache_delete_page(sibling_node->page_id);
      cache_free_node(node_id, WRITE, true);
    }
  }
  //TODO: TRUE(MODIFIED) IS NO ALWAYS NEEDED IF WE DIDNT TOUCH IT.
  while (!rstack_empty(cursor->path))
    cache_free_node(((btree_node*)rstack_pop(cursor->path))->page_id, WRITE, true);

  free_cursor(cursor);
}

unsigned int split_branch_node_and_insert (btree_node* pnode, unsigned int nnp, btree_node* new_node, unsigned int lower_key, btree_config* conf) {

  unsigned  int new_node_lowest_key; 
  unsigned int* tmp_keys = calloc((conf->max_cells+1) * conf->key_size, 1);
  unsigned int* tmp_pointers = calloc((conf->max_cells+1) * conf->key_size, 1);

  memcpy(tmp_keys, pnode->page_keys, conf->max_keys * conf->key_size);
  memcpy(tmp_pointers, pnode->page_pointers, conf->max_cells * conf->pointer_size);

  int key_index = insert_in_order(tmp_keys, conf->max_keys, lower_key);
  insert_in_index(tmp_pointers, conf->max_cells, key_index+1, nnp);

  empty_btree_node(pnode, conf);

  int old_node_n_keys = conf->min_keys;
  int new_node_n_keys = conf->max_keys - conf->min_keys;

  int old_node_n_pointers = (conf->max_cells + 1)/2;
  int new_node_n_pointers = (conf->max_cells + 1) - (conf->max_cells + 1)/2;

  pnode->n_keys = old_node_n_keys;
  pnode->n_pointers = old_node_n_pointers;
  pnode->next = 0;
  
  new_node->n_keys = new_node_n_keys;
  new_node->n_pointers = new_node_n_pointers;
  new_node->next = 0;

  memcpy(pnode->page_keys, tmp_keys, old_node_n_keys * conf->key_size);
  memcpy(pnode->page_pointers, tmp_pointers, old_node_n_pointers * conf->pointer_size);

  memcpy(new_node->page_keys, tmp_keys + (old_node_n_keys+1), new_node_n_keys  * conf->key_size);
  memcpy(new_node->page_pointers, tmp_pointers + (old_node_n_pointers), new_node_n_pointers * conf->pointer_size);
  
  new_node_lowest_key = tmp_keys[conf->min_keys];

  free(tmp_pointers);
  free(tmp_keys);

  return new_node_lowest_key;
}

// inserts a new node created to the parent of the splited node.
void insert_in_parent (btree_cursor* cursor, char* table_name, unsigned int node_id_to_insert, unsigned int lower_key, btree_config* conf) {
  
  // splitted node is root  
  if (rstack_empty(cursor->path)) {
    unsigned int new_root_id;
    btree_node* new_root = init_btree_node(conf);
    new_root->page_keys[0] = lower_key;
    new_root->page_pointers[0] = cursor->node_id;
    new_root->page_pointers[1] = node_id_to_insert;
    new_root->n_keys = 1;
    new_root->n_pointers = 2;
    new_root->next = 0;
    new_root_id = cache_write_new_node(new_root);
    journal_register_created_page(new_root_id);
    journal_write_header();
    update_BTREE_root_page_id(table_name, new_root_id);
    return;
  }
  
  move_to_parent_node(cursor);
  if (cursor->bnode_p->n_keys < conf->max_keys) {
    //direct insert
    journal_write_page(cursor->bnode_p);
    int index = insert_in_order(cursor->bnode_p->page_keys, cursor->bnode_p->n_keys, lower_key);
    insert_in_index(cursor->bnode_p->page_pointers, cursor->bnode_p->n_pointers, index+1, node_id_to_insert);
    cursor->bnode_p->n_keys++;
    cursor->bnode_p->n_pointers++;
    cache_free_node(cursor->node_id, WRITE, true);
  }
  else {
    //split
    journal_write_page(cursor->bnode_p);
    btree_node* new_node = init_btree_node(conf);
    unsigned int node_id = cursor->node_id;
    unsigned int new_node_lowest_key =  split_branch_node_and_insert(cursor->bnode_p, node_id_to_insert, new_node, lower_key, conf);
    unsigned int new_node_id = cache_write_new_node(new_node);
    journal_register_created_page(new_node_id);
    insert_in_parent(cursor, table_name, new_node_id, new_node_lowest_key, conf);
    cache_free_node(node_id, WRITE, true);
  }
}

void btree_insert (unsigned int id, char* table_name, char* row, unsigned int row_size, btree_config* conf) {

  btree_cursor* cursor = search_tree_to_insert(id, table_name);

  if (cursor->bnode_p->n_pointers < conf->max_keys) {
    journal_write_page(cursor->bnode_p);
    insert_in_leaf(cursor->bnode_p, id, row, row_size, conf);
    cache_free_node(cursor->node_id, WRITE, true);
  }
  else {
    // leaf needs to be splitted
    journal_write_page(cursor->bnode_p);
    btree_node* new_node = init_btree_node(conf);
    new_node->flags = IS_LEAF;

    unsigned int* tmp_rows_ids = malloc((conf->max_keys+1)*conf->key_size);
    char* tmp_rows = calloc((conf->max_keys+1)*(conf->cell_item_data_max_size+1)*5, 1);
    unsigned short* tmp_rows_len = malloc((conf->max_keys+1)* sizeof(short));
    memcpy(tmp_rows_ids, cursor->bnode_p->page_keys, conf->max_keys * conf->key_size);

    int new_row_tmp_index = insert_in_order(tmp_rows_ids, conf->max_keys, id);

    for (int i = 0; i < new_row_tmp_index; i++)
      tmp_rows_len[i] = get_row(cursor->bnode_p->payload, cursor->bnode_p->page_pointers[i], tmp_rows + i*conf->cell_item_data_max_size, conf);

    for (int i = new_row_tmp_index+1; i < conf->max_keys+1; i++)
      tmp_rows_len[i] = get_row(cursor->bnode_p->payload, cursor->bnode_p->page_pointers[i-1], tmp_rows + i*conf->cell_item_data_max_size, conf);

    empty_btree_node(cursor->bnode_p, conf);

    for (int i = 0; i < (conf->max_keys+1)/2; i++)
      if (i == new_row_tmp_index) insert_in_leaf(cursor->bnode_p, tmp_rows_ids[i], row, row_size, conf);
      else insert_in_leaf(cursor->bnode_p, tmp_rows_ids[i], &tmp_rows[i*conf->cell_item_data_max_size], tmp_rows_len[i], conf);

    for (int i = (conf->max_keys+1)/2; i <= conf->max_keys; i++)
      if (i == new_row_tmp_index) insert_in_leaf(new_node, tmp_rows_ids[i], row, row_size, conf);
      else insert_in_leaf(new_node, tmp_rows_ids[i], &tmp_rows[i*conf->cell_item_data_max_size], tmp_rows_len[i], conf);
    
    free(tmp_rows_ids);
    free(tmp_rows);
    free(tmp_rows_len);

    unsigned int new_node_id;
    unsigned int node_id = cursor->node_id;
    new_node->next = cursor->bnode_p->next;
    new_node_id = cache_write_new_node(new_node);
    journal_register_created_page(new_node_id);
    cursor->bnode_p->next = new_node_id;
    insert_in_parent(cursor, table_name, new_node_id, new_node->page_keys[0], conf);
    cache_free_node(node_id, WRITE, true);
  }
  free_cursor(cursor);
}

bool btree_update (unsigned int key, char* table_name, char* row, unsigned int row_size, btree_config* conf) {

  btree_cursor* cursor = search_tree_to_insert(key, table_name);

  int key_index = bin_search(cursor->bnode_p->page_keys, 0 ,cursor->bnode_p->n_keys-1,key);
  if (key_index == -1){
    cache_free_node(cursor->node_id, WRITE, false);
    free_cursor(cursor);
    return false;
  }
  unsigned short cpy_start = cursor->bnode_p->page_pointers[key_index];

  journal_write_page(cursor->bnode_p);
  write_short(cursor->bnode_p->payload+cpy_start, row_size);
  write_short(cursor->bnode_p->payload+cpy_start+conf->cell_data_size_size, 0);
  memcpy(cursor->bnode_p->payload+cpy_start+conf->cell_data_size_size+conf->cell_overflow_size, row, row_size);

  cache_free_node(cursor->node_id, WRITE, true);
  free_cursor(cursor);

  return true;
}

bool btree_delete (char* table_name, unsigned int key, btree_config* conf) {
  /* return found and deleted */
  btree_cursor* cursor = search_tree_to_delete(key, table_name);

  int key_index = bin_search(cursor->bnode_p->page_keys, 0 ,cursor->bnode_p->n_keys-1, key);
  if (key_index == -1) {
    cache_free_node(cursor->node_id, READ, false);
    while(!rstack_empty(cursor->path))cache_free_node(((btree_node*)rstack_pop(cursor->path))->page_id, WRITE, false);
    return false;
  }
  delete_from_leaf(cursor, key, conf);
  return true;
}

chunk_iter* btree_chunk_iterator (char* table_name, unsigned int items_per_chunk, chunk_iter* iter, btree_config* conf) {

  btree_cursor* cursor = search_tree_to_update( iter == NULL ? 0 : iter->items[iter->n_items-1].key, table_name);
  btree_node* node = cursor->bnode_p;
  unsigned int last_node_id = cursor->node_id;
  free_cursor(cursor);
  unsigned short item_start;
  unsigned int  i = 0;

  if (iter == NULL) {
    iter = init_chunk_iter(items_per_chunk);
  } else {
    i = bin_search(node->page_keys, 0, node->n_keys-1, iter->items[iter->n_items-1].key)+1;
    iter->n_items = 0;
  }

  while(true){
    for (; i < node->n_keys; i++) {
      item_start = node->page_pointers[i];
      iter->items[iter->n_items].key = node->page_keys[i];
      iter->items[iter->n_items].len = bytes_to_short(node->payload+item_start);

      memcpy(
        iter->items[iter->n_items].data,
        node->payload+item_start+conf->cell_data_size_size+conf->cell_overflow_size,
        iter->items[iter->n_items].len
      );
      if (++iter->n_items == iter->size) goto ret;
    }
    i = 0;
    if (node->next != 0) {
      node = load_node(node->next, WRITE, conf);
      cache_free_node(last_node_id, WRITE, false);
      last_node_id = node->page_id;
    }
    else{ 
      cache_free_node(node->page_id, WRITE, false);
      break;
    }
  }
  ret:
  cache_free_node(node->page_id, WRITE, false);
  return iter;
}

bool btree_search (unsigned int key, const char* table_name, char* row, unsigned short* row_size, btree_config* conf) {
  
  btree_cursor* cursor = search_tree(key, table_name);

  int key_index = bin_search(cursor->bnode_p->page_keys, 0 ,cursor->bnode_p->n_keys-1, key);
  unsigned int  row_offset = cursor->bnode_p->page_pointers[key_index];
  if (key_index == -1) {
    cache_free_node(cursor->node_id, READ, false);
    free_cursor(cursor);
    return false;
  }
  *row_size = bytes_to_short(cursor->bnode_p->payload + row_offset);

  get_row(cursor->bnode_p->payload, row_offset, row, conf);
  cache_free_node(cursor->node_id, READ, false);
  free_cursor(cursor);
  return true;
}

vector* btree_range_row_scan (char* table_name, unsigned int low_bound, unsigned int high_bound, btree_config* conf) {

  vector* q = init_vector(1024);
  btree_cursor* cursor = search_tree(low_bound, table_name);
  btree_node* node = cursor->bnode_p;
  unsigned int last_node_id = cursor->node_id;
  free_cursor(cursor);
  char* tmp_row;
  do{
    for (int i = 0; i < node->n_keys; i++) {
      if (node->page_keys[i] < low_bound) continue;
      if (node->page_keys[i] > high_bound) return q;
      tmp_row = malloc(conf->cell_item_data_max_size+1);
      get_row(node->payload, node->page_pointers[i], tmp_row, conf);
      vector_push(q, tmp_row);
    }
    if (node->next != 0) {
      node = load_node(node->next, READ, conf);
      cache_free_node(last_node_id, READ, false);
      last_node_id = node->page_id;
    }
    else{ 
      cache_free_node(node->page_id, READ, false);
      break;
    }
  } while(true);

  return q;
}

void btree_foreach (char* table_name, bool (*func)(char* item, unsigned short len), btree_config* conf) {

  btree_cursor* cursor = search_tree_to_update(0, table_name);
  btree_node* node = cursor->bnode_p;
  unsigned int last_node_id = cursor->node_id;
  free_cursor(cursor);
  unsigned short item_start;
  bool modified = false;
  do{
    for (int i = 0; i < node->n_keys; i++) {
      item_start = node->page_pointers[i];
      modified = func(node->payload+item_start+ conf->cell_data_size_size+conf->cell_overflow_size, bytes_to_short(node->payload+item_start));
    }
    if (node->next != 0) {
      node = load_node(node->next, WRITE, conf);
      cache_free_node(last_node_id, WRITE, modified);
      last_node_id = node->page_id;
    }
    else{ 
      cache_free_node(node->page_id, WRITE, modified);
      break;
    }
  } while(true);
}

unsigned int btree_create (btree_config* conf) {

  /* create btree structure*/
  
  /* create 1th leaf node*/
  unsigned int leaf1_id;
  btree_node* leaf1 = init_btree_node(conf);
  leaf1->flags = 1;
  leaf1->n_keys = 0;
  leaf1->n_pointers = 0;
  leaf1->next = 0;
  memset(leaf1->payload, 0, conf->payload_size);
  memset(leaf1->slots, 0, conf->slots_size);
  leaf1_id = cache_write_new_node(leaf1);

  /* create 0th leaf node*/
  unsigned int leaf0_id;
  btree_node* leaf0 = init_btree_node(conf);
  leaf0->flags = 1;
  leaf0->n_keys = 0;
  leaf0->n_pointers = 0;
  leaf0->next = leaf1_id;
  memset(leaf0->payload, 0, conf->payload_size);
  memset(leaf0->slots, 0, conf->slots_size);
  leaf0_id = cache_write_new_node(leaf0);

  /* create root node*/
  btree_node* root = init_btree_node(conf);
  root->flags = 0;
  root->n_keys = 1;
  root->n_pointers = 2;
  root->next = 0;
  root->page_keys[0] = conf->max_keys;
  root->page_pointers[0] = leaf0_id;
  root->page_pointers[1] = leaf1_id;
  memset(root->payload, 0, conf->payload_size);
  memset(root->slots, 0, conf->slots_size);
  
  return cache_write_new_node(root);
}

void init_btree () {
  init_cache();
}
