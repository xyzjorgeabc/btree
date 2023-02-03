#ifndef A_H
#define A_H
#include <stdbool.h> 
#define SECTOR_SIZE 4096
#define IS_LEAF 1

typedef enum sibling_side {
  RIGHT_SIBLING = 0,
  LEFT_SIBLING = 1
} sibling_side;

typedef struct btree_item {
  bool overflow;
  unsigned short len;
  char* data;
} btree_item;

typedef struct btree_config {
  unsigned int root;
  unsigned short flags_size;
  unsigned short n_keys_size;
  unsigned short n_pointers_size;
  unsigned int key_size;
  unsigned int pointer_size;
  unsigned short max_keys;
  unsigned short min_keys;
  unsigned short max_cells;
  unsigned short cell_data_size_size;
  unsigned short cell_overflow_size;
  unsigned short cell_item_max_size;
  unsigned short cell_item_data_max_size;
  unsigned short slots_size;
  unsigned int payload_size;
  unsigned int page_size;
} btree_config;

typedef struct btree_node {
  btree_config* conf;
  unsigned int page_id;
  unsigned short flags;
  unsigned short n_keys;
  unsigned short n_pointers;
  unsigned int next;
  unsigned int* page_keys;
  unsigned int* page_pointers;
  char* payload;
  unsigned short n_slots;
  unsigned char* slots;
} btree_node;

void serialize_node (char* page, btree_node* node, btree_config* conf);
void parse_node (char* page, unsigned int page_id, btree_node* node, btree_config* conf);
unsigned int btree_create(btree_config* conf);
btree_node* init_btree_node (btree_config* conf);
void free_btree_node (btree_node* node);

#endif