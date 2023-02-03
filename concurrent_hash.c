#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

typedef struct cht_value {
  bool lock;
  unsigned int collisions;
  unsigned int key;
  unsigned int index;
  void* data;
} cht_value;

typedef struct chash_table {
  cht_value* values;
  unsigned int buffer_size;
  unsigned int mask;
  unsigned int max_n;
  unsigned int n;
  unsigned int p;
} chash_table;

unsigned int uk_hash(unsigned int key, unsigned short p) {
  unsigned int ALPHA = 2654435769; // 2^32 * phi
  return (key * ALPHA) >> (32 - p);
}

extern inline void atomic_set (volatile bool* atomic_lock) {
  bool CELL_FREE = 0;
  bool CELL_LOCKED = 1;

  while(true){
    CELL_FREE = 0;
    if(__atomic_compare_exchange_n(atomic_lock, &CELL_FREE, CELL_LOCKED, false,__ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) return;
  }  
}

extern inline void atomic_unset (volatile bool* atomic_lock) {
  bool CELL_FREE = 0;
  bool CELL_LOCKED = 1;

  while(true){
    CELL_LOCKED = 1;
    if(__atomic_compare_exchange_n(atomic_lock, &CELL_LOCKED, CELL_FREE, false,__ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) return;
  }  
}

chash_table* init_chash_table (size_t size) {

  unsigned int buffer_size = 2;
  unsigned int mask = 0x00000001;
  unsigned int p = 1;
  while(buffer_size <= size*2) {
    mask |= buffer_size;
    buffer_size*=2;
    p++;
  }

  chash_table* map = malloc(sizeof(chash_table));
  map->max_n = size;
  map->buffer_size = buffer_size;
  map->mask = mask;
  map->values = malloc(buffer_size * sizeof(cht_value));
  map->n = 0;
  map->p = p;

  memset(map->values, 0, buffer_size * sizeof(cht_value));
  return map;
}

void* hm_get (unsigned int key, chash_table* map) {

  unsigned int index = uk_hash(key, map->p);
  unsigned int cursor = index;
  unsigned int collisions;
  void* data = NULL;
  int i = 0;

  atomic_set(&map->values[cursor].lock);
  collisions = map->values[cursor].collisions;
  if (!collisions) {
    atomic_unset(&map->values[cursor].lock);
    return NULL;
  }
  if (map->values[cursor].key == key) {
    data =  map->values[cursor].data;
    atomic_unset(&map->values[cursor].lock);
    return data;  
  }
  (map->values[cursor].index) == index && --collisions;
  atomic_unset(&map->values[cursor].lock);
  ++cursor;
  
  while (collisions) {
    i++;
    (cursor == map->buffer_size) && (cursor = 0);
    atomic_set(&map->values[cursor].lock);
    if (map->values[cursor].key == key) {
      data = map->values[cursor].data;
      atomic_unset(&map->values[cursor].lock);
      break;
    }
    (map->values[cursor].index) == index && --collisions;
    atomic_unset(&map->values[cursor].lock);
    ++cursor;
  }

  return data;
}

void hm_insert (unsigned int key, void* data, chash_table* map) {
  
  unsigned int index = uk_hash(key, map->p);
  unsigned int cursor = index;

  __atomic_fetch_add(&map->n, 1, __ATOMIC_RELAXED);
  atomic_set(&map->values[cursor].lock);
  ++map->values[cursor].collisions;
  if (map->values[cursor].data == NULL) {
    map->values[cursor].data = data;
    map->values[cursor].key = key;
    map->values[cursor].index = index;
    atomic_unset(&map->values[cursor].lock);
    return;
  }
  atomic_unset(&map->values[cursor].lock);
  int i = 0;
  while (true) {
    ++cursor;
    i++;
    (cursor == map->buffer_size) && (cursor = 0);
    atomic_set(&map->values[cursor].lock);
    if(map->values[cursor].data == NULL) break;
    atomic_unset(&map->values[cursor].lock);
  }

  map->values[cursor].data = data;
  map->values[cursor].key = key;
  map->values[cursor].index = index;
  atomic_unset(&map->values[cursor].lock);

  return;
}

bool hm_delete (unsigned int key, chash_table* map) {

  unsigned int index = uk_hash(key, map->p);
  unsigned int cursor = index;
  unsigned int delete_index;
  unsigned int collisions;
  __atomic_fetch_sub(&map->n, 1, __ATOMIC_RELAXED);
  atomic_set(&map->values[cursor].lock);
  collisions = map->values[cursor].collisions;
  --map->values[cursor].collisions;
  (map->values[cursor].index == index) && --collisions;

  // is index the item to delete.
  if (map->values[cursor].key == key) {
    if (!collisions) {
      map->values[cursor].data = NULL;
      atomic_unset(&map->values[cursor].lock);
      return true;
    }
    delete_index = cursor;
  }
  else {
    atomic_unset(&map->values[cursor].lock);
    
    while (collisions) {
      // find the index of item to delete.
      ++cursor;
      (cursor == map->buffer_size) && (cursor = 0);
      atomic_set(&map->values[cursor].lock);
      (map->values[cursor].index == index) && --collisions;
      if (map->values[cursor].key == key) {
        if (!collisions) {
          map->values[cursor].data = NULL;
          atomic_unset(&map->values[cursor].lock);
          return true;
        }
        delete_index = cursor;
        break;
      }
      atomic_unset(&map->values[cursor].lock);
    }
  }
  int i = 0;
  // there are still collisions, find the last one
  while (collisions) {
    ++cursor;
    i++;
    (cursor == map->buffer_size) && (cursor = 0);
    atomic_set(&map->values[cursor].lock);
    if (map->values[cursor].index == index && !--collisions) break;
    atomic_unset(&map->values[cursor].lock);
  }

  map->values[delete_index].key = map->values[cursor].key;
  map->values[delete_index].data = map->values[cursor].data;
  map->values[cursor].data = NULL;
  atomic_unset(&map->values[cursor].lock);
  atomic_unset(&map->values[delete_index].lock);
  return true;
}

bool cht_is_full (chash_table* table) {
  return table->n == table->max_n;
}
