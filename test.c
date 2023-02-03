#include "operations.c"
#include <time.h>
#include <sys/syscall.h>

void create_db_test_file () {
  struct stat f;
  if (stat ("test.bin", &f) == 0)system("rm test.bin");

  system("fallocate -l 4G test.bin");
}

void setup_db_test_file () {
  /* CREATE DB_FILE */
  create_db_test_file();
}

void test_avl_tree () {
  unsigned int n =  1000000;
  unsigned int* arr = uint_random_nr_in_range(&n, 0, 22949672);
  unsigned int* arr2 = malloc(n*sizeof(unsigned int));
  memcpy(arr2, arr, n*sizeof(unsigned int));
  uint_qsort(arr2, n);

  avl_bin_tree* tree = init_avl_bin_tree();
  
  for (unsigned int i = 0; i<n; i++) {
    if (arr[i]==0)continue;
    avl_insert(arr[i], &arr[i], tree);
  }

  queue* q = avl_inorder(tree);
  
  for (unsigned int i = 0; i<n/2-1; i++) {
    if(arr2[i]==0)continue;
    unsigned int num = *(unsigned int *)queue_pop(q);
    assert(*(unsigned int *)avl_search(arr2[i], tree) == num);
    assert(avl_delete(num, tree));
  }
  for (unsigned int i = n/2-1; i<n; i++) {
    if(arr2[i]==0)continue;
    unsigned int num = *(unsigned int *)queue_pop(q);
    assert(*(unsigned int *)avl_search(arr2[i], tree) == num);
  }

}

void test_queue () {

  queue* q = init_queue(1024);
  unsigned int * p = NULL;

  for (int i =0; i < 200000; i++) {
    p = malloc(sizeof(int));
    *p = i;
    queue_push(q, p);
  }

  unsigned int* last = queue_pop(q);
  unsigned int* act;

  while (!queue_is_empty(q)) {
    act = queue_pop(q);
    
    if(!(*last < *act)) {
      printf("%u vs %u n_element in q: %lu \n", *last, *act, q->n_elements);
    }

    *last = *act;
  }
}

void test_ll () {

  unsigned int arr[10] = {0,1,2,3,4,5,6,7,8,9};
  linked_list* ll = init_linked_list();

  
  llist_insert(arr[1], &arr[1], ll);
  llist_delete(arr[1], ll);
  llist_insert(arr[2], &arr[2], ll);
  llist_insert(arr[3], &arr[3], ll);
  llist_delete(arr[2], ll);
  llist_insert(arr[4], &arr[4], ll);
  llist_insert(arr[5], &arr[5], ll);
  llist_insert(arr[6], &arr[6], ll);
  llist_insert(arr[7], &arr[7], ll);
  llist_insert(arr[8], &arr[8], ll);
  llist_insert(arr[9], &arr[9], ll);

  llist_delete(arr[9], ll);
  llist_delete(arr[0], ll);
  llist_delete(arr[1], ll);
  llist_delete(arr[5], ll);
  llist_insert(arr[9], &arr[9], ll);
  
}

void test_range_query (char* table_name, unsigned int* keys, int n_rows, btree_config* conf) {

  uint_qsort(keys, n_rows);

  unsigned int lower_bound = keys[0];
  unsigned int upper_bound = keys[n_rows-1];
  printf("\nstarting scan");
  
  vector* vec =  btree_range_row_scan(table_name, lower_bound, upper_bound, conf);

  printf("\nfinished scan");
  char* row =  calloc(conf->cell_item_data_max_size, 1);

  char* row_test;
  bool errors = false;
  printf("\nstarting comparison");

  for (int i = 0; i < n_rows; i++) {
    sprintf(row, "TEST ROW N: %u", keys[i]);

    row_test = ((char*)vector_pop(vec));

    if (strcmp(row_test, row) != 0){
      errors = true;
      printf("\n TEST_RANGE_QUERY FAILED,  EXPECTED: \n %s \n BUT GOT: \n %s \n", row, row_test);
    }
    free(row_test);
  }
  if (!errors) printf("\nfinished comparison, no errors.\n");
  else printf("\nfinished comparison, found errors!\n");

  free(row);
  free_vector(vec);
}

unsigned int* test_insert (char* table_name, unsigned int* keys, unsigned int n_keys, btree_config* conf) {
  int seed = 1;
  srand(seed);

  char* row =  calloc(conf->cell_item_data_max_size, 1);

  printf("\nTesting random key insert");

  printf("\nGenerated %i random keys with no repetitions. Seed: %i", n_keys, seed);

  struct timespec start = {0};
  struct timespec end = {0};
  struct timespec time_res = {0};
  
  clock_gettime(CLOCK_REALTIME, &start);
  printf("\nStarting insertion of %i rows", n_keys);

  for (unsigned int i = 0; i < n_keys; i++) {
    sprintf(row, "TEST ROW N: %u", keys[i]);
    btree_insert(keys[i], table_name, row, conf->cell_item_data_max_size, conf);
    //assert(btree_search(keys[i], table_name, row, &conf->cell_item_data_max_size, conf));
  }

  clock_gettime(CLOCK_REALTIME, &end);

  get_time_from_to(&start, &end, &time_res);

  free(row);
  printf("\nFinished inserting, it took: %li seconds and %li milliseconds\n", time_res.tv_sec, time_res.tv_nsec);

  return keys;
}

void test_unsorted_query (char* table_name, unsigned int* keys, int n_rows, btree_config* conf) {

  char* row =  calloc(conf->cell_item_data_max_size, 1);

  char* row_test = malloc(conf->cell_item_data_max_size);
  unsigned short row_test_size = 0;
  
  printf("\n starting comparison");
  int not_founds = 0;
  for (int i = 0; i < n_rows; i++) {
    
    sprintf(row, "TEST ROW N: %u", keys[i]);

    if (!btree_search(keys[i], table_name, row_test, &row_test_size, conf)) 
      printf("\nNO FOUUUNNDDD"), not_founds++;
    else if (strcmp(row_test, row) != 0){
      printf("\n TEST_RANGE_QUERY FAILED,  EXPECTED: \n %s \n BUT GOT: \n %s \n I: %i", row, row_test, i);
    }
  }
  printf("\n finish comparison TOTAL NOT FOUNDS:  %i \n", not_founds);
  free(row);
  free(row_test);
}

unsigned int* test_random_delete (char* table_name, unsigned int* keys, unsigned int* n_keys, btree_config* conf){

  unsigned int ammount_to_delete = *n_keys/2;
  printf("\ngenerating %i keys to delete", ammount_to_delete);
  unsigned int* keys_to_delete = uint_random_nr_in_range(&ammount_to_delete, 0, *n_keys-1);
  uint_qsort(keys_to_delete, ammount_to_delete);

  printf("\nstarting %i deletes", ammount_to_delete);

  struct timespec start = {0};
  struct timespec end = {0};
  struct timespec time_res = {0};
  
  clock_gettime(CLOCK_REALTIME, &start);

  for (unsigned int i = 0; i < ammount_to_delete; i++) {
    btree_delete(table_name, keys[keys_to_delete[i]], conf);
  }
  
  clock_gettime(CLOCK_REALTIME, &end);
  get_time_from_to(&start, &end, &time_res);

  printf("\nFinished deletes, it took %li seconds and %li milliseconds", time_res.tv_sec, time_res.tv_nsec);
  
  unsigned int new_keys_len = *n_keys;
  unsigned int* new_keys = malloc(*n_keys * sizeof(unsigned int));
  memcpy(new_keys, keys, *n_keys * sizeof(unsigned int));

  for (unsigned int i = 0; i < ammount_to_delete; i++) {
    new_keys_len = delete_value(new_keys, new_keys_len, keys[keys_to_delete[i]]);
  }
  
  free(keys_to_delete);
  free(keys);

  *n_keys = new_keys_len;
  return new_keys;
}

#define SLOTS_SIZE 200
void test_slots () {

  unsigned char slots[SLOTS_SIZE] = {254,255,255,255,255,0,255,0};
  
  for (int i = 0; i < SLOTS_SIZE; i++)
    printf("%u ", slots[i]);
  printf("\n");
  unsigned int pointr = reserve_first_empty_slot(slots, SLOTS_SIZE);

  for (int i = 0; i < SLOTS_SIZE; i++)
    printf("%u ", slots[i]);
  printf("\n");
  free_slot(slots, pointr);

  for (int i = 0; i < SLOTS_SIZE; i++)
    printf("%u ", slots[i]);
  printf("\n");
}

void bm_unsorted_query (char* table_name, unsigned int* keys, unsigned  int n_keys, btree_config* conf) {

  char* row_test =  calloc(conf->cell_item_data_max_size, 1);
  unsigned short row_test_size = 0;

  struct timespec start = {0};
  struct timespec end = {0};
  struct timespec time_res = {0};
  clock_gettime(CLOCK_REALTIME, &start);

  printf("\nStarting get with %u keys", n_keys);
  for (int i = 0; i < n_keys; i++) {
    assert(btree_search(keys[i], table_name, row_test, &row_test_size, conf));
  }

  clock_gettime(CLOCK_REALTIME, &end);
  get_time_from_to(&start, &end, &time_res);

  printf("\nFinished get sequence, it took: %li seconds and %li milliseconds", time_res.tv_sec, time_res.tv_nsec);
  free(row_test);
}

void test_chunk_iterator (char* bname, btree_config* conf) {

  unsigned int lastq = 0;
  chunk_iter* iter = NULL;

  do {
    iter = btree_chunk_iterator(bname, 1000, iter, conf);
    for (int i = 0; i < iter->n_items; i++) {
      assert(iter->items[i].key > lastq);
      lastq = iter->items[i].key;
    }
  } while (iter->n_items);

}

bool printfunc (char* item, unsigned short len) {

  printf("%s\n", item);

  return false;
}

int main () {

  char* bname = "test_table";
  char* bname2 = "test_table2";

  setup_db_test_file();
  init_btree();

  create_btree(bname, sizeof(unsigned int), 1021);
  //create_btree(bname2, sizeof(unsigned int), 1021);
  btree_config* conf = get_BTREE_root_page_id(bname);
  //btree_config* conf2 = get_BTREE_root_page_id(bname2);
  //test_queue();
  //test_avl_tree();
  //test_ll();
  //test_slots();
  
  unsigned int n_keys = 100000;
  unsigned int* keys = uint_random_nr_in_range(&n_keys, 0, RAND_MAX/2);
  uint_qsort(keys, n_keys);
  start_transaction();
  
  test_insert(bname, keys, n_keys, conf);
  bm_unsorted_query(bname, keys, n_keys, conf);
  //test_insert(bname2, keys, n_keys, conf2);

  //test_chunk_iterator(bname);
  //test_insert(bname2, keys, n_keys);

  keys = test_random_delete(bname, keys, &n_keys, conf);

  commit_transaction();

  //bm_unsorted_query(bname, keys, n_keys);
  //bm_unsorted_query(bname2, keys, n_keys);
  //test_unsorted_query(bname, keys, n_keys);
  //test_range_query(bname, keys, n_keys);

  free(keys);

  printf("\n\n----- TOTAL TRIES: %i  -----", total_tries);
  printf("\n----- TOTAL MISSES: %i -----", total_misses);
  printf("\n----- TOTAL NODES CREATED: %i -----", total_nodes_created);
  printf("\n----- PROPORTION MISSES: %f -----\n", (float)total_misses/(float)total_tries);
  printf("\n----------------------\n");
  printf("\n----- TOTAL PAGES READ:  %i  -----", total_pages_read);
  printf("\n----- TOTAL PAGES WRITTEN: %i  -----\n", total_pages_written);

  return 0;
}
