#include "stdbool.h"
#include "string.h"
#include "ctype.h"
#include <stdlib.h>

typedef struct str_p_ht_value {
  unsigned int collisions;
  const char* key;
  unsigned int index;
  void* data;
} str_p_ht_value;

typedef struct str_p_hash_table {
  str_p_ht_value* values;
  unsigned int buffer_size;
  unsigned int mask;
  unsigned int max_n;
  unsigned int n;
  unsigned short p;
} str_p_hash_table;

unsigned int uk_hash(unsigned int, unsigned short);

unsigned int djb2_hash(const char* str, unsigned short p) {

  unsigned int hash = 5381; // seed
  int c;

  while ((c=*(str++))) hash = ((hash << 5) + hash) + c;

  hash >>= (32-p); 
  return hash;
}

str_p_hash_table* init_hash_table (size_t size) {

  unsigned int buffer_size = 2;
  unsigned int mask = 0x00000001;
  unsigned short p = 1;
  while(buffer_size <= size*2) {
    mask |= buffer_size;
    buffer_size*=2;
    p++;
  }

  str_p_hash_table* map = malloc(sizeof(str_p_hash_table));
  map->max_n = size;
  map->buffer_size = buffer_size;
  map->mask = mask;
  map->values = malloc(buffer_size * sizeof(str_p_ht_value));
  map->n = 0;
  map->p = p;

  memset(map->values, 0, buffer_size * sizeof(str_p_ht_value));
  return map;
}

void free_hash_table (str_p_hash_table* ht) {
  free(ht->values);
  free(ht);
}

bool str_p_ht_is_full (str_p_hash_table* table) {
  return table->n == table->max_n;
}

void* str_p_ht_get (const char* key, str_p_hash_table* table) {

  unsigned int index = djb2_hash(key, table->p);
  unsigned int cursor = index;
  unsigned int collisions;
  void* data = NULL;

  collisions = table->values[cursor].collisions;
  if (!collisions) {
    return NULL;
  }
  if (strcmp(table->values[cursor].key, key) == 0) {
    data =  table->values[cursor].data;
    return data;  
  }
  (table->values[cursor].index) == index && --collisions;
  ++cursor;
  
  while (collisions) {
    (cursor == table->buffer_size) && (cursor = 0);
    if (strcmp(table->values[cursor].key, key) == 0) {
      data = table->values[cursor].data;
      break;
    }
    (table->values[cursor].index) == index && --collisions;
    ++cursor;
  }

  return data;
}

void str_p_ht_insert (const char* key, void* data, str_p_hash_table* table) {

  unsigned int index = djb2_hash(key, table->p);
  unsigned int cursor = index;

  ++table->n;
  ++table->values[cursor].collisions;
  if (table->values[cursor].data == NULL) {
    table->values[cursor].data = data;
    table->values[cursor].key = key;
    table->values[cursor].index = index;
    return;
  }

  while (true) {
    ++cursor;
    (cursor == table->buffer_size) && (cursor = 0);
    if(table->values[cursor].data == NULL) break;
  }

  table->values[cursor].data = data;
  table->values[cursor].key = key;
  table->values[cursor].index = index;

  return;
}

bool str_p_ht_delete (const char* key, str_p_hash_table* table) {

  unsigned int index = djb2_hash(key, table->p);
  unsigned int cursor = index;
  unsigned int delete_index;
  unsigned int collisions;
  --table->n;
  collisions = table->values[cursor].collisions;
  --table->values[cursor].collisions;
  (table->values[cursor].index == index) && --collisions;

  // is index the item to delete.
  if (strcmp(table->values[cursor].key, key) == 0) {
    if (!collisions) {
      table->values[cursor].data = NULL;
      return true;
    }
    delete_index = cursor;
  }
  else {

    while (collisions) {
      // find the index of item to delete.
      ++cursor;
      (cursor == table->buffer_size) && (cursor = 0);
      (table->values[cursor].index == index) && --collisions;
      if (strcmp(table->values[cursor].key, key) == 0) {
        if (!collisions) {
          table->values[cursor].data = NULL;
          return true;
        }
        delete_index = cursor;
        break;
      }
    }
  }
  // there are still collisions, find the last one
  while (collisions) {
    ++cursor;
    (cursor == table->buffer_size) && (cursor = 0);
    if (table->values[cursor].index == index && !--collisions) break;
  }

  table->values[delete_index].key = table->values[cursor].key;
  table->values[delete_index].data = table->values[cursor].data;
  table->values[cursor].data = NULL;
  return true;
}

unsigned int  str_p_ht_all (void** buffer, unsigned int buffer_size, str_p_hash_table* table) {

  int j = 0;
  for (int i = 0; i < table->buffer_size; i++) {
    if (table->values[i].data != NULL) buffer[j++] = table->values[i].data;
  }

  return j;
}

typedef struct uint32_p_ht_value {
  unsigned int collisions;
  unsigned int key;
  unsigned int index;
  void* data;
} uint32_p_ht_value;

typedef struct uint32_p_hash_table {
  uint32_p_ht_value* values;
  unsigned int buffer_size;
  unsigned int mask;
  unsigned int max_n;
  unsigned int n;
  unsigned short p;
} uint32_p_hash_table;


uint32_p_hash_table* init_uint32_p_hash_table (size_t size) {

  unsigned int buffer_size = 2;
  unsigned int mask = 0x00000001;
  unsigned short p = 1;
  while(buffer_size <= size*2) {
    mask |= buffer_size;
    buffer_size*=2;
    p++;
  }

  uint32_p_hash_table* map = malloc(sizeof(uint32_p_hash_table));
  map->max_n = size;
  map->buffer_size = buffer_size;
  map->mask = mask;
  map->values = malloc(buffer_size * sizeof(uint32_p_ht_value));
  map->n = 0;
  map->p = p;

  memset(map->values, 0, buffer_size * sizeof(uint32_p_ht_value));
  return map;
}

bool uint32_p_ht_is_full (uint32_p_hash_table* table) {
  return table->n == table->max_n;
}

void* uint32_p_ht_get (unsigned int key, uint32_p_hash_table* table) {

  unsigned int index = uk_hash(key, table->p);
  unsigned int cursor = index;
  unsigned int collisions;
  void* data = NULL;

  collisions = table->values[cursor].collisions;
  if (!collisions) {
    return NULL;
  }
  if (table->values[cursor].key == key) {
    data =  table->values[cursor].data;
    return data;  
  }
  (table->values[cursor].index) == index && --collisions;
  ++cursor;
  
  while (collisions) {
    (cursor == table->buffer_size) && (cursor = 0);
    if (table->values[cursor].key == key) {
      data = table->values[cursor].data;
      break;
    }
    (table->values[cursor].index) == index && --collisions;
    ++cursor;
  }

  return data;
}

void uint32_p_ht_insert (unsigned int key, void* data, uint32_p_hash_table* table) {

  unsigned int index = uk_hash(key, table->p);
  unsigned int cursor = index;

  ++table->n;
  ++table->values[cursor].collisions;
  if (table->values[cursor].data == NULL) {
    table->values[cursor].data = data;
    table->values[cursor].key = key;
    table->values[cursor].index = index;
    return;
  }

  while (true) {
    ++cursor;
    (cursor == table->buffer_size) && (cursor = 0);
    if(table->values[cursor].data == NULL) break;
  }

  table->values[cursor].data = data;
  table->values[cursor].key = key;
  table->values[cursor].index = index;

  return;
}

bool uint32_p_ht_delete (unsigned int key, uint32_p_hash_table* table) {

  unsigned int index = uk_hash(key, table->p);
  unsigned int cursor = index;
  unsigned int delete_index;
  unsigned int collisions;
  --table->n;
  collisions = table->values[cursor].collisions;
  --table->values[cursor].collisions;
  (table->values[cursor].index == index) && --collisions;

  // is index the item to delete.
  if (table->values[cursor].key == key) {
    if (!collisions) {
      table->values[cursor].data = NULL;
      return true;
    }
    delete_index = cursor;
  }
  else {

    while (collisions) {
      // find the index of item to delete.
      ++cursor;
      (cursor == table->buffer_size) && (cursor = 0);
      (table->values[cursor].index == index) && --collisions;
      if (table->values[cursor].key == key) {
        if (!collisions) {
          table->values[cursor].data = NULL;
          return true;
        }
        delete_index = cursor;
        break;
      }
    }
  }
  // there are still collisions, find the last one
  while (collisions) {
    ++cursor;
    (cursor == table->buffer_size) && (cursor = 0);
    if (table->values[cursor].index == index && !--collisions) break;
  }

  table->values[delete_index].key = table->values[cursor].key;
  table->values[delete_index].data = table->values[cursor].data;
  table->values[cursor].data = NULL;
  return true;
}

//crear buffer dentro de la funcion, se asume el tamano es igual a table->n
void**  uint32_p_ht_all (uint32_p_hash_table* table) {

  void** ptrs = malloc(table->n);

  int j = 0;
  for (int i = 0; i < table->buffer_size; i++) {
    if (table->values[i].data != NULL) ptrs[j++] = table->values[i].data;
  }

  return ptrs;
} 
