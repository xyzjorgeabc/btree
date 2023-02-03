#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include "pager.h"
#include "btree.h"
#include "cache.h"
#include "catalog.h"
#include "utils.h"

#define JOURNAL_ENTRY_SIZE (PAGE_SIZE + SECTOR_SIZE)
#define JOURNAL_ENTRY_ID_SIZE (sizeof(unsigned int))
#define POINTER_SIZE (sizeof(unsigned int))

uint_reg* pages_written;
uint_reg* pages_created_this_transaction;
char journal_entry[JOURNAL_ENTRY_SIZE] = {0};
char journal_header[DB_HEADER_SIZE] = {0};
unsigned int current_transaction_id = 0;
unsigned int file_offset;
FILE* journal_pages_fp;
FILE* journal_header_fp;

void journal_start_transaction () {

  journal_pages_fp = fopen("journal.bbdb", "wb+");
  journal_header_fp = fopen("header.bbdb", "wb+");
  file_offset = 0;
  pages_written = init_uint_reg(2);
  pages_created_this_transaction = init_uint_reg(2);
  ++current_transaction_id;
}

void journal_write_header (){

  if (journal_header_fp != NULL) return;
  serialize_db_header(journal_header);
  fseek(journal_header_fp, 0, 0);
  fwrite(journal_header, 1, DB_HEADER_SIZE, journal_header_fp);

}

void journal_register_created_page (unsigned int page_id) {
  uint_reg_insert(pages_created_this_transaction, page_id);
}

void journal_write_page (btree_node* bnode) {

  if (uint_reg_has(pages_written, bnode->page_id) ||
      uint_reg_has(pages_created_this_transaction, bnode->page_id)) return;

  btree_config* conf = bnode->conf;
  char* journal_entry_cursor = journal_entry;
  uint_reg_insert(pages_written, bnode->page_id);

  memcpy(journal_entry_cursor, &current_transaction_id, JOURNAL_ENTRY_ID_SIZE);
  journal_entry_cursor += JOURNAL_ENTRY_ID_SIZE;
  memcpy(journal_entry, &bnode->page_id, POINTER_SIZE);
  journal_entry_cursor += POINTER_SIZE;

  serialize_node(journal_entry_cursor, bnode, conf);

  fseek(journal_pages_fp, file_offset*JOURNAL_ENTRY_SIZE, 0);
  fwrite(journal_entry, 1, JOURNAL_ENTRY_SIZE, journal_pages_fp);
  
  ++file_offset;
}

void journal_commit_transaction () {

  unsigned int page_id;

  while ((page_id = uint_reg_pop(pages_written))) {
    cache_force_flush(page_id);
  }

  write_db_header();

  free_uint_reg(pages_written);
  free_uint_reg(pages_created_this_transaction);
  fclose(journal_header_fp);
  fclose(journal_pages_fp);
  remove("journal.bbdb");
  remove("header.bbdb");
}

void journal_rollback () {

  struct stat stats;
  if(stat("journal.bbdb", &stats) != 0) return;

  char* journal_entry_cursor = journal_entry;

  for (unsigned int i = 0; i < pages_written->n_elements; i++) {

    fseek(journal_pages_fp, i*JOURNAL_ENTRY_SIZE, 0);
    fread(journal_entry, 1, JOURNAL_ENTRY_SIZE, journal_pages_fp);

    journal_entry_cursor += JOURNAL_ENTRY_ID_SIZE;
    cache_delete_page(*(unsigned int*)journal_entry_cursor);
    pager_real_write(*(unsigned int*)journal_entry_cursor, journal_entry_cursor+POINTER_SIZE);
    
    journal_entry_cursor = journal_entry;
  }

  _load_db_header(journal_header);

  free_uint_reg(pages_written);
  free_uint_reg(pages_created_this_transaction);
  fclose(journal_header_fp);
  fclose(journal_pages_fp);
  remove("journal.bbdb");
  remove("header.bbdb");
}
