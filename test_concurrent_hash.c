#include "concurrent_hash.c"
#include <pthread.h>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

void get_time_from_to (struct timespec* then, struct timespec* now, struct timespec* out) {
  
  //TODO: GOTTA MAKE IT FASTERRRRR. ALGEBRA

  long int then_s_in_ms = then->tv_sec*1000;
  long int then_ns_in_ms = then->tv_nsec/1000000;

  long int now_s_in_ms = now->tv_sec*1000;
  long int now_ns_in_ms = now->tv_nsec/1000000;

  long int then_total = then_s_in_ms + then_ns_in_ms;
  long int now_total = now_s_in_ms + now_ns_in_ms;

  out->tv_sec = (now_total - then_total) / 1000;
  out->tv_nsec = (now_total - then_total) % 1000;
}

extern inline unsigned int random_in_range (unsigned int min, unsigned int max) {
  return min + rand() / (RAND_MAX / (max - min + 1) + 1 );
}

void knuth_shuffle (unsigned int* arr, unsigned int len) {

  unsigned int last_shuffled_index = len-1;
  unsigned int random_index;
  unsigned int tmp;

  while (last_shuffled_index != 0){
    random_index = random_in_range(0, last_shuffled_index-1);
    
    tmp = arr[last_shuffled_index];
    arr[last_shuffled_index] = arr[random_index];
    arr[random_index] = tmp;

    last_shuffled_index--;
  }
}

void uint_qsort (unsigned int* arr, int n_elements) {

  #define MAX_PARTITIONS 100 //MAX LEVEL OF PARTITIONS

  int pivot;
  int start[MAX_PARTITIONS];
  int end[MAX_PARTITIONS];
  int current_left;
  int current_right;
  unsigned int tmp;

  start[0] = 0;
  end[0] = n_elements;
  int i = 0;

  while (i>=0) {

    current_left=start[i]; 
    current_right=end[i]-1;

    if (current_left >= current_right){
      i--;
      continue;
    }
    
    pivot=arr[current_left];
    while (current_left<current_right) {
      
      while (arr[current_right] >= pivot && current_left < current_right) 
        current_right--;
        
      if (current_left < current_right) 
        arr[current_left++] = arr[current_right];
      
      while (arr[current_left] <= pivot && current_left < current_right) 
        current_left++; 
      
      if (current_left < current_right) 
        arr[current_right--] = arr[current_left];
    }

    arr[current_left] = pivot;
    start[i+1] = current_left+1;
    end[i+1] = end[i]; 
    end[i++] = current_left;
    
    if (end[i] - start[i] > end[i-1] - start[i-1]) {
      tmp = start[i];
      start[i] = start[i-1];
      start[i-1] = tmp;
      tmp = end[i];
      end[i] = end[i-1];
      end[i-1] = tmp; 
    }
  }
}

int delete_repeated_from_ordered_arr (unsigned int* arr, size_t len) {
  // TODO: GOTTA GO FASTERRRRRR
  int i,j,k, count = 0;

  for(i=0;i<len;i++){
    for(j = i+1; j < len; j++){
      if (arr[j] == arr[i]) arr[j] = 0;
      else break;
    }
  }
  return  len;
}

typedef struct data_data {
  unsigned int* arr;
  int n;
  chash_table* map;
  volatile bool ended;
} data_data;

void *insert_thread (void* qq) {
  data_data* data = qq;

  for (int i = 0; i < data->n; i++) {
    if (!data->arr[i]) continue;
    hm_insert(data->arr[i], &data->arr[i], data->map);
  }
  atomic_unset(&data->ended);
  return NULL;
}

void *search_thread (void* qq) {
  data_data* data = qq;
  void *q = NULL;
  for (int i = 0; i < data->n; i++) {
    if (!data->arr[i]) continue;
    q = hm_get(data->arr[i], data->map);
    assert(&data->arr[i] == hm_get(data->arr[i], data->map));
  }
  atomic_unset(&data->ended);
  return NULL;
}

void *delete_thread (void* qq) {
  data_data* data = qq;
  
  for (int i = 0; i < data->n; i++) {
    if (!data->arr[i]) continue;
    assert(hm_delete(data->arr[i], data->map));
  }
  atomic_unset(&data->ended);
  return NULL;
}

int main () {

  unsigned int random_keys_n = 10000000;
  unsigned int* random_keys = malloc((random_keys_n) * sizeof(unsigned int));
  chash_table* map = init_chash_table(random_keys_n);
  printf("\ngenerating %i random numbers", random_keys_n);
  srand(1);

  for (unsigned int i = 0; i < random_keys_n; i++) {
    random_keys[i] = rand();
  }

  uint_qsort(random_keys,random_keys_n);
  delete_repeated_from_ordered_arr(random_keys, random_keys_n);
  knuth_shuffle(random_keys, random_keys_n);
  random_keys_n/=10;

  printf("\nusing %i non-repeating random keys per thread", random_keys_n);

  data_data data1 = {random_keys, random_keys_n, map, false};
  data_data data2 = {&random_keys[random_keys_n], random_keys_n, map, false};
  data_data data3 = {&random_keys[random_keys_n*2], random_keys_n, map, false};

  data_data data4 = {&random_keys[random_keys_n*3], random_keys_n, map, false};
  data_data data5 = {&random_keys[random_keys_n*4], random_keys_n, map, false};
  data_data data6 = {&random_keys[random_keys_n*5], random_keys_n, map, false};

  data_data data7 = {&random_keys[random_keys_n*6], random_keys_n, map, false};
  data_data data8 = {&random_keys[random_keys_n*7], random_keys_n, map, false};
  data_data data9 = {&random_keys[random_keys_n*8], random_keys_n, map, false};
  
  data_data data10 = {&random_keys[random_keys_n*9], random_keys_n, map, false};
 
  printf("\ncreating threads\n");
  
  struct timespec start;
  struct timespec end;
  struct timespec diff;

  clock_gettime(CLOCK_REALTIME, &start);

  atomic_set(&data1.ended);
  atomic_set(&data2.ended);
  atomic_set(&data3.ended);
  atomic_set(&data4.ended);
  atomic_set(&data5.ended);
  atomic_set(&data6.ended);
  atomic_set(&data7.ended);
  atomic_set(&data8.ended);
  atomic_set(&data9.ended);
  atomic_set(&data10.ended);

  pthread_t id[10] = {0};
  
  assert(!pthread_create(&id[0], NULL, insert_thread, &data1));
  assert(!pthread_create(&id[1], NULL, insert_thread, &data2));
  assert(!pthread_create(&id[2], NULL, insert_thread, &data3));
  assert(!pthread_create(&id[3], NULL, insert_thread, &data4));
  assert(!pthread_create(&id[4], NULL, insert_thread, &data5));
  assert(!pthread_create(&id[5], NULL, insert_thread, &data6));
  assert(!pthread_create(&id[6], NULL, insert_thread, &data7));
  assert(!pthread_create(&id[7], NULL, insert_thread, &data8));
  assert(!pthread_create(&id[8], NULL, insert_thread, &data9));
  assert(!pthread_create(&id[9], NULL, insert_thread, &data10));

  atomic_set(&data1.ended);
  atomic_set(&data2.ended);
  atomic_set(&data3.ended);
  atomic_set(&data4.ended);
  atomic_set(&data5.ended);
  atomic_set(&data6.ended);
  atomic_set(&data7.ended);
  atomic_set(&data8.ended);
  atomic_set(&data9.ended);
  atomic_set(&data10.ended);

  assert(!pthread_create(&id[0], NULL, search_thread, &data1));
  assert(!pthread_create(&id[1], NULL, search_thread, &data2));
  assert(!pthread_create(&id[2], NULL, search_thread, &data3));
  assert(!pthread_create(&id[3], NULL, search_thread, &data4));
  assert(!pthread_create(&id[4], NULL, search_thread, &data5));
  assert(!pthread_create(&id[5], NULL, search_thread, &data6));
  assert(!pthread_create(&id[6], NULL, search_thread, &data7));
  assert(!pthread_create(&id[7], NULL, search_thread, &data8));
  assert(!pthread_create(&id[8], NULL, search_thread, &data9));
  assert(!pthread_create(&id[9], NULL, search_thread, &data10));

  atomic_set(&data1.ended);
  atomic_set(&data2.ended);
  atomic_set(&data3.ended);
  atomic_set(&data4.ended);
  atomic_set(&data5.ended);
  atomic_set(&data6.ended);
  atomic_set(&data7.ended);
  atomic_set(&data8.ended);
  atomic_set(&data9.ended);
  atomic_set(&data10.ended);

  assert(!pthread_create(&id[0], NULL, delete_thread, &data1));
  assert(!pthread_create(&id[1], NULL, delete_thread, &data2));
  assert(!pthread_create(&id[2], NULL, delete_thread, &data3));
  assert(!pthread_create(&id[3], NULL, delete_thread, &data4));
  assert(!pthread_create(&id[4], NULL, delete_thread, &data5));
  assert(!pthread_create(&id[5], NULL, delete_thread, &data6));
  assert(!pthread_create(&id[6], NULL, delete_thread, &data7));
  assert(!pthread_create(&id[7], NULL, delete_thread, &data8));
  assert(!pthread_create(&id[8], NULL, delete_thread, &data9));
  assert(!pthread_create(&id[9], NULL, delete_thread, &data10));

  atomic_set(&data1.ended);
  atomic_set(&data2.ended);
  atomic_set(&data3.ended);
  atomic_set(&data4.ended);
  atomic_set(&data5.ended);
  atomic_set(&data6.ended);
  atomic_set(&data7.ended);
  atomic_set(&data8.ended);
  atomic_set(&data9.ended);
  atomic_set(&data10.ended);

  clock_gettime(CLOCK_REALTIME, &end);
  get_time_from_to(&start, &end, &diff);

  printf("\nended threads, took %li secs y %li milliseconds\n", diff.tv_sec, diff.tv_nsec);
  return 0;
}
