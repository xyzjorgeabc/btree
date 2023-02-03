#include <stdio.h>
#include <stdbool.h>

#ifndef B_H
#define B_H


typedef struct uint_vector {
  unsigned int * data;
  size_t n_elements;
  size_t size;
} uint_vector;

typedef struct uint_reg {
  unsigned int* data;
  unsigned int size;
  unsigned int buffer_size;
  unsigned int iter_i;
  unsigned int n_elements;
} uint_reg;

uint_vector* init_uint_vector (size_t size);
void uint_vector_insert_at (uint_vector* vec, size_t index, unsigned int elem);
unsigned int uint_vector_get (uint_vector* vec, int pos);
void uint_vector_push (uint_vector* vec, unsigned int elem);
unsigned int uint_vector_delete_at (uint_vector* vec, size_t index);
unsigned int uint_vector_pop (uint_vector* vec);
bool uint_vector_empty (uint_vector* vec);
bool uint_vector_has (uint_vector* vec, unsigned int elem);
void free_uint_vector (uint_vector* vec);
uint_reg* init_uint_reg (unsigned int size);
void uint_reg_insert (uint_reg* reg, unsigned int value);
bool uint_reg_has (uint_reg* reg, unsigned int value);
void free_uint_reg (uint_reg* reg);
unsigned int uint_reg_pop (uint_reg* reg);

#endif