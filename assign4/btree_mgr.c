#include "btree_mgr.h"

// init and shutdown index manager
RC initIndexManager(void *mgmtData) {
    // TODO: Implement this function
    return RC_OK;
}

RC shutdownIndexManager() {
    // TODO: Implement this function
    return RC_OK;
}

// create, destroy, open, and close a btree index
RC createBtree(char *idxId, DataType keyType, int n) {
    // TODO: Implement this function
    return RC_OK;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
    // TODO: Implement this function
    return RC_OK;
}

RC closeBtree(BTreeHandle *tree) {
    // TODO: Implement this function
    return RC_OK;
}

RC deleteBtree(char *idxId) {
    // TODO: Implement this function
    return RC_OK;
}

// access information about a b-tree
RC getNumNodes(BTreeHandle *tree, int *result) {
    // TODO: Implement this function
    return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result) {
    // TODO: Implement this function
    return RC_OK;
}

RC getKeyType(BTreeHandle *tree, DataType *result) {
    // TODO: Implement this function
    return RC_OK;
}

// index access
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    // TODO: Implement this function
    return RC_OK;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    // TODO: Implement this function
    return RC_OK;
}

RC deleteKey(BTreeHandle *tree, Value *key) {
    // TODO: Implement this function
    return RC_OK;
}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    // TODO: Implement this function
    return RC_OK;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    // TODO: Implement this function
    return RC_OK;
}

RC closeTreeScan(BT_ScanHandle *handle) {
    // TODO: Implement this function
    return RC_OK;
}

// debug and test functions
char *printTree(BTreeHandle *tree) {
    // TODO: Implement this function
    return NULL;
}
