#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include "catalog.c"

extern FILE* db_fd;
extern db_header dbh;

void init_pager () {

  init_catalog();
}

void load_page (unsigned int id, char* page) {

  unsigned long _seek = (off_t)DB_HEADER_SIZE + ((off_t)dbh.page_size * (off_t)id);
  fseek(db_fd, _seek, 0);
  assert(fread(page, 1, dbh.page_size, db_fd) == dbh.page_size);

}

void pager_real_write(unsigned int id, const char* page) {

  off_t _seek = (off_t)DB_HEADER_SIZE + ((off_t)dbh.page_size * (off_t)id);
  fseek(db_fd, _seek, 0);
  assert(fwrite(page, 1, dbh.page_size, db_fd) == dbh.page_size);

}

unsigned int pager_reserve_page () {
  off_t n_pages = (dbh.first_free_byte - DB_HEADER_SIZE) / dbh.page_size;
  dbh.first_free_byte += dbh.page_size;
  return n_pages+1;
}
