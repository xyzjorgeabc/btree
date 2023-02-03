#include "btree.h"

void journal_start_transaction ();
void journal_commit_transaction ();
void journal_rollback ();
void journal_write_page (btree_node* bnode);
void journal_write_header ();
