#include <math.h>
#include "utils.c"
#include "pager.h"
#include "btree.h"
#include "catalog.h"
#include "cache.h"

typedef struct db_header {
  unsigned char flags;
  str_p_hash_table* BTREES;
  unsigned int n_roots;
  off_t first_free_byte;
  unsigned int page_size;
} db_header;

typedef enum KEY_TYPE {
  STRING_KEY,
  INT_KEY
} KEY_TYPE;

table_root_info* init_table_root_info (
  const char* BTREE_name,
  unsigned short key_size,
  unsigned short max_data_size,
  unsigned int page_size ){

  table_root_info* bi = malloc(sizeof(table_root_info));

  bi->name_len = strlen(BTREE_name);
  bi->table_name = malloc(bi->name_len+1);
  strcpy(bi->table_name, BTREE_name);

  bi->conf.flags_size = sizeof(unsigned short);
  bi->conf.n_keys_size = sizeof(unsigned short);
  bi->conf.n_pointers_size = sizeof(unsigned short);
  bi->conf.key_size = key_size;
  bi->conf.pointer_size = sizeof(unsigned int);

  bi->conf.cell_data_size_size = sizeof(unsigned short);
  bi->conf.cell_overflow_size = 1;
  bi->conf.cell_item_max_size = max_data_size+bi->conf.cell_data_size_size+bi->conf.cell_overflow_size;
  bi->conf.cell_item_data_max_size = max_data_size;

  unsigned int bytes_aval = page_size - bi->conf.flags_size - bi->conf.n_keys_size - bi->conf.n_pointers_size - bi->conf.pointer_size;
  unsigned int n = bytes_aval / (2*bi->conf.key_size + bi->conf.cell_item_max_size);
  
  bi->conf.max_keys = n - 1;
  bi->conf.min_keys = bi->conf.max_keys / 2;
  bi->conf.max_cells = n;
  bi->conf.payload_size = bi->conf.max_keys * bi->conf.cell_item_max_size;

  bi->conf.slots_size = (unsigned short)ceil((float)bi->conf.max_cells/(float)8);
  bi->conf.page_size = page_size;

  return bi;
}

void free_table_root_info(table_root_info* btree_info){
  free(btree_info->table_name);
  free(btree_info);
}

//int db_fd = -1;
FILE* db_fd;
db_header dbh;

//TODO: WHY POINTERS?

void _load_db_header (char hbuff[DB_HEADER_SIZE]) {
  
  char* hptr = hbuff;
  
  memcpy(&dbh.flags, hptr, DB_HEADER_FLAGS_SIZE);
  memcpy(&dbh.n_roots, hptr+=DB_HEADER_FLAGS_SIZE, DB_HEADER_TABLE_COUNTER_SIZE);
  memcpy(&dbh.first_free_byte, hptr+=DB_HEADER_TABLE_COUNTER_SIZE, DB_HEADER_FIRST_FREE_BYTE_SIZE);
  hptr += DB_HEADER_FIRST_FREE_BYTE_SIZE;

  table_root_info* root_tmp;

  for (int i = 0; i < dbh.n_roots; i++) {

    char name[ROOT_NAME_MAX_SIZE] = {0};
    unsigned char name_len;
    unsigned int root;

    memcpy(&name_len, hptr, ROOT_NAME_SIZE);
    memcpy(name, hptr+=ROOT_NAME_SIZE, name_len);
    name[name_len] = '\0';
    root = bytes_to_int(hptr+=ROOT_NAME_MAX_SIZE);
    hptr+=ROOT_POINTER_SIZE;
    //root_tmp = init_table_root_info(name, root);
    str_p_ht_insert(root_tmp->table_name, root_tmp, dbh.BTREES);
  }
}

void load_db_header () {
  
  char hbuff[DB_HEADER_SIZE] = {0};
  char* hptr = hbuff;
  
  fseek(db_fd, 0, 0);
  fread(hbuff, 1, DB_HEADER_SIZE, db_fd);

  memcpy(&dbh.flags, hptr, DB_HEADER_FLAGS_SIZE);
  memcpy(&dbh.n_roots, hptr+=DB_HEADER_FLAGS_SIZE, DB_HEADER_TABLE_COUNTER_SIZE);
  memcpy(&dbh.first_free_byte, hptr+=DB_HEADER_TABLE_COUNTER_SIZE, DB_HEADER_FIRST_FREE_BYTE_SIZE);
  hptr += DB_HEADER_FIRST_FREE_BYTE_SIZE;

  table_root_info* root_tmp;

  for (int i = 0; i < dbh.n_roots; i++) {

    char name[ROOT_NAME_MAX_SIZE] = {0};
    unsigned char name_len;
    unsigned int root;

    memcpy(&name_len, hptr, ROOT_NAME_SIZE);
    memcpy(name, hptr+=ROOT_NAME_SIZE, name_len);
    name[name_len] = '\0';
    root = bytes_to_int(hptr+=ROOT_NAME_MAX_SIZE);
    hptr+=ROOT_POINTER_SIZE;
    //root_tmp = init_table_root_info(name, root);
    str_p_ht_insert(root_tmp->table_name, root_tmp, dbh.BTREES);
  }
}

void serialize_db_header (char hbuff[DB_HEADER_SIZE]) {

  char* hptr = hbuff;
  table_root_info** btrees = malloc(dbh.BTREES->n * sizeof(void*));
  str_p_ht_all((void**)btrees, dbh.BTREES->n, dbh.BTREES);

  memcpy(hptr, &dbh.flags, DB_HEADER_FLAGS_SIZE);
  memcpy(hptr+=DB_HEADER_FLAGS_SIZE, &dbh.n_roots, DB_HEADER_TABLE_COUNTER_SIZE);
  memcpy(hptr+=DB_HEADER_TABLE_COUNTER_SIZE, &dbh.first_free_byte, DB_HEADER_FIRST_FREE_BYTE_SIZE);
  hptr += DB_HEADER_FIRST_FREE_BYTE_SIZE;


  for (int i = 0; i < dbh.n_roots; i++) {
    memcpy(hptr, &btrees[i]->name_len, ROOT_NAME_SIZE);
    memcpy(hptr+=ROOT_NAME_SIZE, btrees[i]->table_name, btrees[i]->name_len);
    memcpy(hptr+=ROOT_NAME_MAX_SIZE, &btrees[i]->root_pointer, ROOT_POINTER_SIZE);
    hptr+=ROOT_POINTER_SIZE;
  }
  free(btrees);
}

void write_db_header () {

  char hbuff[DB_HEADER_SIZE] = {0};
  char* hptr = hbuff;
  table_root_info** btrees = malloc(dbh.BTREES->n * sizeof(void*));
  str_p_ht_all((void**)btrees, dbh.BTREES->n, dbh.BTREES);

  memcpy(hptr, &dbh.flags, DB_HEADER_FLAGS_SIZE);
  memcpy(hptr+=DB_HEADER_FLAGS_SIZE, &dbh.n_roots, DB_HEADER_TABLE_COUNTER_SIZE);
  memcpy(hptr+=DB_HEADER_TABLE_COUNTER_SIZE, &dbh.first_free_byte, DB_HEADER_FIRST_FREE_BYTE_SIZE);
  hptr += DB_HEADER_FIRST_FREE_BYTE_SIZE;


  for (int i = 0; i < dbh.n_roots; i++) {
    memcpy(hptr, &btrees[i]->name_len, ROOT_NAME_SIZE);
    memcpy(hptr+=ROOT_NAME_SIZE, btrees[i]->table_name, btrees[i]->name_len);
    memcpy(hptr+=ROOT_NAME_MAX_SIZE, &btrees[i]->root_pointer, ROOT_POINTER_SIZE);
    hptr+=ROOT_POINTER_SIZE;
  }
  fseek(db_fd, 0, 0);
  fwrite(hbuff, 1, DB_HEADER_SIZE, db_fd);
  free(btrees);
}

void init_catalog () {
  db_fd = fopen("test.bin", "rb+");

  dbh.BTREES = init_hash_table(MAX_TABLES);
  dbh.first_free_byte = DB_HEADER_SIZE;
  dbh.flags = 0;
  dbh.n_roots = 0;
  dbh.page_size = SECTOR_SIZE * 64;
  //load_db_header();
}


bool create_btree (const char* btree_name, unsigned short key_size, unsigned short max_data_size){

  table_root_info* btree_info = str_p_ht_get(btree_name, dbh.BTREES);
  if (btree_info != NULL || str_p_ht_is_full(dbh.BTREES)) return false;

  btree_info = init_table_root_info(btree_name, key_size, max_data_size, PAGE_SIZE);
  btree_info->root_pointer = btree_create(&btree_info->conf);
  btree_info->conf.root = btree_info->root_pointer;
  dbh.n_roots++;

  str_p_ht_insert(btree_info->table_name, btree_info, dbh.BTREES);
  write_db_header();
  return true;
}

bool delete_btree (const char* btree_name) {

  //TODO: ASK BTREE TO POSTORDER THE TREE AND DELETE EACH NODE.

  table_root_info* btree = str_p_ht_get(btree_name, dbh.BTREES);
  if (btree == NULL) return false;

  free_table_root_info(btree);
  str_p_ht_delete(btree_name, dbh.BTREES);
  dbh.n_roots--;
  free_table_root_info(btree);
  write_db_header();
  return true;
}

unsigned int _get_BTREE_root_page_id (const char* btree_name) {

  table_root_info* btree = str_p_ht_get(btree_name, dbh.BTREES);
  assert(btree);

  return btree->root_pointer;
}

btree_config* get_BTREE_root_page_id (const char* btree_name) {

  table_root_info* btree = str_p_ht_get(btree_name, dbh.BTREES);
  assert(btree);

  return &btree->conf;
}


void update_BTREE_root_page_id (const char* btree_name, unsigned int root_page_id) {

  table_root_info* btree = str_p_ht_get(btree_name, dbh.BTREES);
  assert(btree);

  btree->root_pointer = root_page_id;
  btree->conf.root = root_page_id;
}

bool create_db (const char* db_name, int page_size) {

  FILE* fp;
  int db_name_len = strlen(db_name);
  char* file_name = malloc(db_name_len + strlen(".bbdb") + 1);
  strcpy(file_name, db_name);
  strcat(file_name, ".bbdb");

  struct stat st;
  if (stat(file_name, &st) == 0) return false;

  fp = fopen(file_name, "r+b");
  
  db_header dh;
  dh.page_size = page_size;
  dh.n_roots = 0;
  dh.first_free_byte = DB_HEADER_SIZE;
  dh.flags = 0;
  
  char* hbuff = calloc(DB_HEADER_SIZE, 1);
  char* hptr = hbuff;

  memcpy(hptr, &dbh.flags, DB_HEADER_FLAGS_SIZE);
  memcpy(hptr+=DB_HEADER_FLAGS_SIZE, &dbh.n_roots, DB_HEADER_TABLE_COUNTER_SIZE);
  memcpy(hptr+=DB_HEADER_TABLE_COUNTER_SIZE, &dbh.first_free_byte, DB_HEADER_FIRST_FREE_BYTE_SIZE);
  hptr += DB_HEADER_FIRST_FREE_BYTE_SIZE;

  fseek(fp, 0, 0);
  fwrite(hbuff, 1, DB_HEADER_SIZE, fp);
  fclose(fp);
  free(hbuff);
  free(file_name);
  return true;
}