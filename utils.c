#include <limits.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <string.h>
#include "concurrent_hash.c"
#include "hash.c"
#include <sys/stat.h>
#include "utils.h"

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))

extern inline short bytes_to_short (const char* src) {
  return *((short* )src);
}

extern inline int bytes_to_int (const char* src) {
  return *((int* )src);
}

extern inline void write_int (char* dest, int n) {
  *((int* )dest) = n;
}

extern inline void write_short (char* dest, short n) {
  *((short* )dest) = n;
} 

int bin_search (unsigned int* arr, int left, int right, unsigned int elem) {

  if (left > right) return -1;

  int mid = (left+right)/2;
  if (arr[mid] == elem) return mid;
  else if (arr[mid] < elem) return bin_search(arr, mid+1, right, elem);
  else return bin_search(arr, left, mid-1, elem);

}

int unordered_search (unsigned int* arr, int left, int right, unsigned int elem) {

  for (int i = left; i <= right; i++) {
    if (arr[i] == elem) return i;
  }
  return -1;
}

int first_greater_search (unsigned int* arr, int len, unsigned int needle) {

  int start = 0;
  int end = len-1;
  int ind = -1;
  int mid;

  while(start<=end) {

    mid = (start+end)/2;
    if (arr[mid] <= needle) start = mid+1;
    else {
      ind = mid;
      end = mid-1;
    }
  }
  return ind;
}

int insert_in_order (unsigned int* arr, int len, unsigned int elem) {

  int first_greater_index = first_greater_search(arr, len, elem);
  if (first_greater_index == -1) {
    arr[len] = elem;
    return len;
  }
  else {

    for (int i = len-1; i >= first_greater_index; i--) {
      arr[i+1] = arr[i];
    }
    arr[first_greater_index] = elem;
    return first_greater_index;
  }

}

int delete_in_order (unsigned int* arr, int len, int pos) {
  while (pos < len-1){
    arr[pos] = arr[pos+1];
    pos++;
  }
  return len-1;
}

int delete_value (unsigned int* arr, int len, unsigned int value) {
  int index = bin_search(arr, 0, len-1, value);
  if (index == -1) return len;
  return delete_in_order(arr, len, index);
}

void insert_in_index (unsigned int* arr, int len, int pos, unsigned int elem) {
  for (int i = len; i > pos; i--) {
      arr[i] = arr[i-1];
  }
  arr[pos] = elem;
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

int delete_repeated (unsigned int* arr, size_t len) {
  // TODO: GOTTA GO FASTERRRRRR

  for(size_t i = 0; i < len; i++){
    for(size_t j = i+1; j < len; j++){
      if(arr[i] == arr[j]){
         for(size_t k = j; k < len; k++){
            arr[k] = arr[k+1];
         }
         j--;
         len--;
      }
    }
  }
  return  len;
}

void zero_repeated_ordered (unsigned int* arr, size_t len) {

  for(size_t i=0;i<len;i++){
    for(size_t j = i+1; j < len; j++){
      if (arr[j] == arr[i]) arr[j] = 0;
      else break;
    }
  }
}

size_t delete_repeated_ordered (unsigned int arr[10], size_t len) {

  for(size_t i=0;i<len;i++){
    size_t reps = 0;
    for(size_t j = i+1; j < len; j++){
      if (arr[j] == arr[i]) reps++;
      else break;
    }
    if (reps){
      for (int m = 0; i+reps+1+m < len; m++) {
        arr[i+1+m] = arr[i+reps+1+m];
      }
      len -= reps;
    }
  }
  return len;
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

unsigned int* uint_random_nr_in_range(unsigned int* n, unsigned int min, unsigned int max){

  unsigned int* arr = malloc(*n*sizeof(unsigned int));
  for (unsigned int i = 0; i < *n; i++) {
    arr[i] = random_in_range(min, max);
  }
  uint_qsort(arr, *n);
  *n = delete_repeated_ordered(arr, *n);
  knuth_shuffle(arr, *n);
  return arr;
}

/* VECTOR */

typedef struct vector {
  void** data;
  size_t n_elements;
  size_t size;
} vector;

vector* init_vector (size_t size) {
  vector* vec = malloc(sizeof(vector));
  vec->data = malloc( sizeof(void*) * size);
  vec->n_elements = 0;
  vec->size = size;
  return vec;
}

void free_vector (vector* vec) {
  free(vec->data);
  free(vec);
}

void vector_insert_at (vector* vec, size_t index, void* elem) {

  if (vec->n_elements == vec->size){
    void** new_buffer = malloc(vec->size * 2 * sizeof(void*));
    memcpy(new_buffer, vec->data, index * sizeof(void*));
    new_buffer[index] = elem;
    memcpy(new_buffer+index+1, vec->data+index+1, (vec->size - index) * sizeof(void*));
    vec->size *= 2;
    free(vec->data);
    vec->data = new_buffer;
  }
  else {
    for (size_t i = vec->n_elements; i > index; i--) {
      vec->data[i] = vec->data[i-1];
    }
    vec->data[index] = elem;
  }
  vec->n_elements++;
}

void vector_push (vector* vec, void* elem) {
  vector_insert_at(vec, vec->n_elements, elem);
}

void* vector_get (vector* vec, int pos) {
  return vec->data[pos];
}

void vector_delete_at (vector* vec, size_t index) {
  for (size_t i = index; i < vec->n_elements-2; i++) {
    vec->data[i] = vec->data[i+1];
  }
  vec->n_elements--;
}

void* vector_shift (vector* vec) {
  void * data = vec->data[vec->n_elements-1];
  vector_delete_at(vec, 0);
  return data;
}

void* vector_pop (vector* vec) {
  return vec->data[--vec->n_elements];
}

/* uint VECTOR */

uint_vector* init_uint_vector (size_t size) {
  uint_vector* vec = malloc(sizeof(uint_vector));
  vec->data = malloc( sizeof(unsigned int) * size);
  vec->n_elements = 0;
  vec->size = size;
  return vec;
}

void uint_vector_insert_at (uint_vector* vec, size_t index, unsigned int elem) {

  if (vec->n_elements == vec->size){
    vec->data =  realloc(vec->data, vec->size * 2 * sizeof(unsigned int));
    /*unsigned int* new_buffer = malloc(vec->size * 2 * sizeof(unsigned int));
    memcpy(new_buffer, vec->data, index * sizeof(unsigned int));
    new_buffer[index] = elem;
    memcpy(new_buffer+index+1, vec->data+index+1, (vec->size - index) * sizeof(unsigned int));
    free(vec->data);
    vec->data = new_buffer;*/
    vec->size *= 2;
  }
  else {
    for (size_t i = vec->n_elements; i > index; i--) {
      vec->data[i] = vec->data[i-1];
    }
    vec->data[index] = elem;
  }
  vec->n_elements++;
}

void uint_vector_push (uint_vector* vec, unsigned int elem) {
  uint_vector_insert_at(vec, vec->n_elements, elem);
}

unsigned int uint_vector_get (uint_vector* vec, int pos) {
  return vec->data[pos];
}

unsigned int uint_vector_delete_at (uint_vector* vec, size_t index) {
  unsigned int tmp = vec->data[index]; 
  if (index == vec->n_elements-1) {
    tmp = vec->data[index];
    --vec->n_elements;
    return tmp;
  }
  for (size_t i = index; i < vec->n_elements-2; i++) {
    vec->data[i] = vec->data[i+1];
  }
  vec->n_elements--;
  return tmp;
}

void uint_vector_shift (uint_vector* vec) {
  uint_vector_delete_at(vec, 0);
}

unsigned int uint_vector_pop (uint_vector* vec) {
  return  uint_vector_delete_at(vec, vec->n_elements-1);
}

bool uint_vector_empty (uint_vector* vec) {
  return vec->n_elements == 0;
}

bool uint_vector_has (uint_vector* vec, unsigned int elem) {

  for (int i = 0; i < vec->n_elements; i++) {
    if (vec->data[i] == elem) return true;
  }
  return false;
}

void free_uint_vector (uint_vector* vec) {
  free(vec->data);
  free(vec);
}

/* uint STACK */

typedef struct uint_stack {
  unsigned int* data;
  int index;
  size_t size;
} stack;

typedef struct uint_stack_iterator {
  unsigned int* data;
  size_t iter_index;
} stack_iterator;

stack* init_stack (size_t size){
  stack* st = (stack* )malloc(sizeof(stack));
  unsigned int* data = (unsigned int* )malloc(sizeof(unsigned int)*size);
  memset(data, (char)-1, size);
  st->index = -1;
  st->data = data;
  st->size = size;

  return st;
}

void free_stack (stack* st) { 
  free(st->data);
  free(st);
}

void stack_push (stack* st, unsigned int elem){
  assert((long int)st->index < (long int)st->size);
  st->data[++st->index] = elem;
}

unsigned int stack_pop(stack* st){
  return st->data[st->index--];
}

unsigned int stack_peek(stack* st){
  return st->data[st->index];
}

unsigned int stack_peek_base (stack* st) {
  return st->data[0];
}

bool stack_empty (stack* st) {
  return st->index == -1;
}

void free_stack_iter(stack_iterator* it){
  free(it->data);
  free(it);
}

bool stack_iter_finished (stack_iterator* it){
  return it->iter_index == -1; 
}

stack_iterator* stack_iter (stack* st) {
  stack_iterator* it = malloc(sizeof(stack_iterator));
  it->data =  (unsigned int* )malloc(sizeof(unsigned int)*st->index+1);
  it->iter_index = st->index;
  memcpy(st->data, it->data, st->size);

  return it;
}

unsigned int stack_next (stack_iterator* iter) {
  return iter->data[iter->iter_index--];
}

/* REAL STACK */

typedef struct rstack {
  void** data;
  int index;
  size_t size;
} rstack;

rstack* init_rstack (size_t size){
  rstack* st = (rstack* )malloc(sizeof(rstack));
  void** data = malloc(sizeof(void*)*size);
  st->index = -1;
  st->data = data;
  st->size = size;

  return st;
}
void rstack_push (rstack* st, void* elem){
  //assert((long int)st->index < (long int)st->size-1);

  if ( st->index == st->size-1 ) {
    st->data = realloc(st->data, st->size*2);
    st->size *= 2;
  }

  st->data[++st->index] = elem;
}

void* rstack_pop(rstack* st){
  if (st->index == -1) return NULL;
  return st->data[st->index--];
}

void* rstack_peek(rstack* st){
  if (st->index == -1) return NULL;
  return st->data[st->index];
}

void* rstack_peek_base (rstack* st) {
  if (st->index == -1) return NULL;
  return st->data[0];
}

void free_rstack (rstack* st) { 
  free(st->data);
  free(st);
}

bool rstack_empty (rstack* st) {
  return st->index == -1;
}

/* QUEUE */

typedef struct queue {
  void** data;
  size_t size;
  size_t n_elements;
  size_t pop_index;
  size_t push_index;
} queue;

queue* init_queue (size_t size){
  queue* q = (queue* )malloc(sizeof(queue));
  void** data = malloc(sizeof(void*)*size);
  q->pop_index = 0;
  q->push_index = -1;
  q->n_elements = 0;
  q->size = size;
  q->data = data;

  return q;
}

void free_queue (queue* q){
  free(q->data);
  free(q);
}

void queue_push (queue* q, void* elem){
  if (q->n_elements == q->size) {
    void** new_buff = malloc( sizeof(void*)*q->size*2);
    if (q->pop_index > q->push_index) {
      memcpy(new_buff, q->data + q->pop_index, (q->size - q->pop_index) * sizeof(void*));
      memcpy(new_buff + (q->size - q->pop_index), q->data, q->pop_index * sizeof(void*));
      q->push_index = q->n_elements-1;
    }
    else {
      memcpy(new_buff, q->data + q->pop_index, q->n_elements * sizeof(void*));
      q->push_index = q->n_elements-1;
    }
    q->pop_index = 0;
    q->size *= 2;
    free(q->data);
    q->data = new_buff;
  }
  if (q->push_index == (q->size-1)) q->push_index = -1; 
  q->data[++q->push_index] = elem;
  q->n_elements++;
}

void* queue_pop(queue* q){
  //assert(q->n_elements > 0);
  if(q->pop_index > q->size-1) q->pop_index = 0;
  q->n_elements--;
  return q->data[q->pop_index++];
}

void* queue_peek_tail(queue* q){
  return q->data[q->push_index];
}

bool queue_is_empty(queue* q) {
  return q->n_elements == 0;
}

void queue_empty(queue* q) {
  q->pop_index = 0;
  q->push_index = -1;
  q->n_elements = 0;
}

/* LINKED LIST*/

typedef struct l_list_node {
  struct l_list_node* next;
  void* data;
  int key;
} l_list_node;

typedef struct linked_list {
  l_list_node* head;
  int n_elements;
} linked_list;

linked_list* init_linked_list () {
  linked_list* list = malloc(sizeof(linked_list));
  list->n_elements = 0;
  list->head = NULL;
  return list;
}

l_list_node* init_l_list_node (unsigned int key, void* data) {
  l_list_node* node = malloc(sizeof(l_list_node));
  node->data = data;
  node->key = key;
  node->next = NULL;

  return node;
}

void llist_insert (unsigned int key, void* data, linked_list* list) {

  l_list_node* node = init_l_list_node(key, data);
  list->n_elements++;
  
  if (list->head == NULL) {
    list->head = node; 
    return;
  }
  
  l_list_node* cursor = list->head;
  while (cursor->next != NULL) cursor = cursor->next;
  
  cursor->next = node;
  
}

bool llist_delete (unsigned int key, linked_list* list) {

  l_list_node* cursor;
  l_list_node* tmp;
  assert(list);
  if (list->head == NULL) return false; 

  cursor = list->head->next;
  tmp = list->head;

  if (list->head->key == key){
    list->head = cursor;
    list->n_elements--;
    free(tmp);
    return true;
  }

  while (cursor->key != key && cursor->next != NULL) {
    tmp = cursor;
    cursor = cursor->next;
  }

  if (cursor->key == key) {
    tmp->next = cursor->next;
    free(cursor);
    list->n_elements--;
    return true;
  }
  return false;
}

void* ll_search (unsigned int key, linked_list* list) {

  l_list_node* cursor = list->head;
  if (list->head == NULL) return NULL;
  while (cursor->next != NULL && cursor->key != key ) cursor = cursor->next;
  
  if (cursor->key == key) return cursor->data;
  else return NULL;

}

bool llist_empty (linked_list* list) {
  return list->n_elements == 0;
}

/* avlbintree */

/* b_factor = left-right; */
typedef struct avl_node {
  struct avl_node* left;
  struct avl_node* right;  
  void* data;
  long int key;
  char b_factor;
  int height;
} avl_node;

typedef struct bin_tree {
  avl_node* root;
  size_t n_elements;
} avl_bin_tree;

avl_node* init_avl_node (long int key, void* data) {

  avl_node* node = malloc(sizeof(avl_node));
  node->left = NULL;
  node->right = NULL;
  node->data = data;
  node->key = key;
  node->b_factor = 0;
  node->height = 0;

  return node;
}

avl_bin_tree* init_avl_bin_tree () {

  avl_bin_tree* tree = (avl_bin_tree*)malloc(sizeof(avl_bin_tree));
  tree->root = NULL;
  tree->n_elements = 0;

  return tree;
}

char calc_b_factor (avl_node* node) {
  int left_val = node->left == NULL ? -1 : node->left->height;
  int right_val = node->right == NULL ? -1 : node->right->height;

  return (char)(left_val - right_val);
}

int calc_height (avl_node* node) {
  return MAX(node->left == NULL ? -1 : node->left->height, node->right == NULL ? -1 : node->right->height) + 1;
}

bool avl_has_no_children (avl_node* node) {
  if (node->left == NULL && node->right == NULL) return true;
  return false;
}

bool avl_right_heavy (avl_node* node) {
  if (node == NULL) return false;
  return node->b_factor < -1;
}

bool avl_left_heavy (avl_node* node) {
  if (node == NULL) return false;
  return node->b_factor > 1;
}
void avl_node_update(avl_node* node) {
  node->height = calc_height(node);
  node->b_factor = calc_b_factor(node);
}
int avl_has_one_child (avl_node* node) {

  if (node->left == NULL && node->right != NULL) return 1;
  else if (node->left != NULL && node->right == NULL) return -1;

  return 0;
}

void* avl_search (long int key, avl_bin_tree* tree) {
  
  avl_node* cursor = tree->root;
  if(cursor == NULL) return NULL;
  
  while(true) {
    if (key > cursor->key && cursor->right != NULL) cursor = cursor->right;
    else if (key < cursor->key && cursor->left != NULL) cursor = cursor->left;
    else break;
  }
  if (cursor->key == key) return cursor->data;
  else return NULL;
}

avl_node* avl_find (long int key, avl_bin_tree* tree) {
/* gets closest or equals key node */
  
  avl_node* cursor = tree->root;
  if(cursor == NULL) return NULL;

  while(true) {
    if (key > cursor->key && cursor->right != NULL) cursor = cursor->right;
    else if (key < cursor->key && cursor->left != NULL) cursor = cursor->left;

    else return cursor;
  }
}

rstack* avl_find_ (long int key, avl_bin_tree* tree) {
/* gets closest or equals key node */
  
  avl_node* cursor = tree->root;
  if(cursor == NULL) return NULL;
  rstack* path = init_rstack(cursor->height+1);

  while(true) {
    rstack_push(path, cursor);
    if (key > cursor->key && cursor->right != NULL) cursor = cursor->right;
    else if (key < cursor->key && cursor->left != NULL) cursor = cursor->left;
    else break;
  }
  return path;
}

avl_node* avl_left_rotation (avl_node* node_x, avl_bin_tree* tree, rstack* path) {
  /*returns new root of the subtree*/
  avl_node* parent = rstack_peek(path);
  avl_node* node_y = node_x->right;
  avl_node* node_b = node_y->left;

  if (parent) {
    if (node_x->key > parent->key) parent->right = node_y;
    else parent->left = node_y;
  }
  else tree->root = node_y;

  node_y->left = node_x;
  node_x->right = node_b;

  avl_node_update(node_x);
  avl_node_update(node_y);

  return node_y;
}

avl_node* avl_right_rotation (avl_node* node_y, avl_bin_tree* tree, rstack* path) {
  /*returns new root of the subtree*/
  avl_node* parent = rstack_peek(path);
  avl_node* node_x = node_y->left;
  avl_node* node_b = node_x->right;

  if(parent){
    if (node_x->key > parent->key) parent->right = node_x;
    else parent->left = node_x;
  }
  else tree->root = node_x;

  node_x->right = node_y;
  node_y->left = node_b;
  
  avl_node_update(node_y);
  avl_node_update(node_x);
  
  return node_x;
}

void rebalance (avl_bin_tree* tree, rstack* path) {
  //TODO: MAYBE WE CAN REMOVE THIS UPDATE AND UPDATE IT IN THE ROTATIONSM, WE HAVE PARENT ANYWAY
  // paths from leaf to root 
  avl_node* cursor;

  while ((cursor = rstack_pop(path)) != NULL) {
    avl_node_update(cursor);
    // case b_factor > 1   do   right or left-right rotation
    if (avl_left_heavy(cursor)) {
      if (avl_right_heavy(cursor->left)) {
        rstack_push(path, cursor);
        avl_left_rotation(cursor->left, tree, path);
        avl_node_update(cursor);
        rstack_pop(path);
        avl_right_rotation(cursor, tree, path);
      }
      else avl_right_rotation(cursor, tree, path);
    }
    // case b_factor < -1   do   left or right-left rotation
    else if (avl_right_heavy(cursor)) {
      if (avl_left_heavy(cursor->right)) {
        rstack_push(path, cursor);
        avl_right_rotation(cursor->right, tree, path);
        avl_node_update(cursor);
        rstack_pop(path);
        avl_left_rotation(cursor, tree, path);
      }
      else avl_left_rotation(cursor, tree, path);
    }
  }
}

void avl_insert (long int key, void* data, avl_bin_tree* tree) {

  rstack* path = avl_find_(key, tree);
  avl_node* cursor = NULL;
  avl_node* node = NULL;
  
  if (path == NULL) {
    // empty tree
    node = init_avl_node(key, data);
    tree->root = node;
    tree->n_elements++;
    return;
  }
  
  cursor = rstack_peek(path);

  if (cursor->key == key) {
    // update vale of existing node. 
    cursor->data = data;
    return;
  }

  // create a new node.
  node = init_avl_node(key, data);

  if (key > cursor->key) cursor->right = node;
  else cursor->left = node;

  tree->n_elements++;
  rebalance(tree, path);
  free_rstack(path);
}

void avl_inorder_traversal(avl_node* node, queue* q) {
  if(node->left != NULL) avl_inorder_traversal(node->left, q);
  queue_push(q, node->data);
  if(node->right != NULL) avl_inorder_traversal(node->right, q);
}

queue* avl_inorder (avl_bin_tree* tree) {
  // sets elemets to an array of ordered elements
  queue* q = init_queue(tree->n_elements);
  if (tree->n_elements > 0) avl_inorder_traversal(tree->root, q);
  return q;
}

void avl_find_removal_succesor (avl_node* node, rstack* path) {
  //assumes node has left and right subtrees. uses the min of right tree.

  avl_node* cursor = node->right;

  rstack_push(path, cursor);

  while (cursor->left != NULL) {
    cursor = cursor->left;
    rstack_push(path, cursor);
  }
}

bool avl_delete (long int key, avl_bin_tree* tree) {

  rstack* path =  avl_find_(key, tree);
  avl_node* node;
  avl_node* parent;
  avl_node* successor = NULL;
  avl_node* successor_parent = NULL;
  avl_node* xchange_tmp = NULL;

  char tmp_child_side;
  if(path == NULL || (node = rstack_pop(path))->key != key) {
    free_rstack(path);
    return false;
  }

  parent = rstack_peek(path);

  if (avl_has_no_children(node)){
    if (!parent) {
      //ĺast element in tree;
      tree->root = NULL;
    }
    else {
      //has parent, we only need to delete the reference
      if (node->key > parent->key) parent->right = NULL;
      else parent->left = NULL;
    }
    free(node);
  }
  else if ((tmp_child_side = avl_has_one_child(node))) {
    // node one child; exchange current node with child.
    if (tmp_child_side == -1) xchange_tmp = node->left;
    else xchange_tmp = node->right;
    
    node->key = xchange_tmp->key;
    node->data = xchange_tmp->data;
    node->left = xchange_tmp->left;
    node->right = xchange_tmp->right;

    rstack_push(path, node);
    free(xchange_tmp);
  }
  else {
    //two children
    //successor is not always a left child
    //can be the first right child of node
    rstack_push(path, node);
    avl_find_removal_succesor(node, path);
    successor = rstack_pop(path);
    successor_parent = rstack_peek(path);
    //has one child
    //will always be a right child
    if (node->right == successor) successor_parent->right = successor->right;
    else successor_parent->left = successor->right;

    node->data = successor->data;
    node->key = successor->key;

    free(successor);
  }
  rebalance(tree, path);
  free_rstack(path);
  tree->n_elements--;
  return true;
}

void* avl_min (long int* key, avl_bin_tree* tree) {

  avl_node* cursor = tree->root;
  if (cursor == NULL) return NULL;
  
  while (cursor->left != NULL) cursor = cursor->left;

  *key = cursor->key;
  return cursor->data;
}

void* avl_max (long int* key, avl_bin_tree* tree) {

  avl_node* cursor = tree->root;
  if (cursor == NULL) return NULL;
  
  while (cursor->right != NULL) cursor = cursor->right;
  
  *key = cursor->key;
  return cursor->data;
}



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

extern inline bool atomic_try_lock (volatile bool* atomic_lock) {
  bool FREE = 0;
  bool LOCKED = 1;

  return __atomic_compare_exchange_n(atomic_lock, &FREE, LOCKED, false,__ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

bool file_exists (char *filename) {
  struct stat   buffer;
  return (stat (filename, &buffer) == 0);
}

/* REGISTER, KEEPS A LIST OF KEYS ITEMS, LIKE A HASH TABLE BUT NO VALUES,
   IT CANT DELETE KEYS, USED TO INSERT AND CHECK IF ITEM IS REGISTERED*/

/* ONCE YOU START POPPING IT WILL IGNORE NEW ADDS THAT LAND AFTER THE ITER_I*/

uint_reg* init_uint_reg (unsigned int size) {

  uint_reg* reg = malloc(sizeof(uint_reg));
  reg->data = calloc(1, size* 2 * sizeof(unsigned int));
  reg->size = size;
  reg->buffer_size = size * 2;
  reg->n_elements = 0;
  reg->iter_i = size * 2 - 1;

  return reg;
}

void uint_reg_insert (uint_reg* reg, unsigned int value) {

  unsigned int* old_data;
  unsigned int old_size;
  unsigned int old_buffer_size;

  if (reg->n_elements == reg->size) {

    old_data = reg->data;
    old_size = reg->size;
    old_buffer_size = reg->buffer_size;

    reg->size *= 2;
    reg->buffer_size *= 2;
    reg->n_elements = 0;
    reg->iter_i = reg->buffer_size-1;
    reg->data = calloc(reg->buffer_size*sizeof(unsigned int), 1);

    for (unsigned int i = 0; i < old_buffer_size; i++) {
      if (old_data[i] != 0) uint_reg_insert(reg, old_data[i]);
    }
    free(old_data);
  }

  unsigned int index = value % reg->buffer_size;

  while (reg->data[index]!=0) {
    ++index;
    index == reg->buffer_size && (index = 0);
  }

  reg->data[index] = value;
  ++reg->n_elements;
}

bool uint_reg_has (uint_reg* reg, unsigned int value) {

  unsigned int index = value % reg->buffer_size;
  while (reg->data[index] != 0 ) {
    if(reg->data[index] == value) return true;
    ++index;
    index == reg->buffer_size && (index = 0);
  }

  return false;
}

unsigned int uint_reg_pop (uint_reg* reg) {

  if(reg->iter_i == 0) return 0;

  while (reg->data[--reg->iter_i] == 0 && reg->iter_i != 0);

  return reg->data[reg->iter_i];
}

void free_uint_reg (uint_reg* reg) {
  free(reg->data);
  free(reg);
}