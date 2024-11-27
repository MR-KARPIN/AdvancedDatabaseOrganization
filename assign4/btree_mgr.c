#include "btree_mgr.h"
#include <buffer_mgr.h>
#include <stdlib.h>

#include "storage_mgr.h"

typedef struct IndexManager {
    BM_PageHandle page_handle_ptr;  // Buffer Manager page_handle_ptr
    BM_BufferPool bufferPool;  // Buffer Manager Buffer Pool (NOT a pointer)
} IndexManager;

IndexManager* index_mgr;  // Global pointer to BTree Manager

//Constants
#define MAX_NUMBER_OF_PAGES 10
#define PAGE_SIZE 4096  // Example size for pages, adjust as needed

// init and shutdown index manager
RC initIndexManager(void *mgmtData) {
    // Allocate memory for the IndexManager structure
    index_mgr = (IndexManager*) malloc(sizeof(IndexManager));
    if (index_mgr == NULL) printf("Error: Memory allocation failed for index manager.\n");

    return RC_OK;  // Return success if initialization succeeds
}

// Shutdown the index manager and free-associated resources.
RC shutdownIndexManager() {
    if (index_mgr == NULL)
        printf("Error: Buffer pool not initialized or already shut down.\n");

    // Shutdown the buffer pool
    RC rc = shutdownBufferPool(&index_mgr->bufferPool);
    if ((rc) != RC_OK) return rc;  // Return the error code from buffer pool shutdown

    return RC_OK;  // Return success if shutdown is successful
}


// create, destroy, open, and close a btree index

// This function creates a Btree
    // |-----------------------|
    //  number of nodes
    // |-----------------------|
    //  number of entries
    // |-----------------------|
    //  key type
    // |-----------------------|
    //  n (order btree)
    // |-----------------------|
    //  rootPage
    // |-----------------------|

RC createBtree(char *idxId, DataType keyType, int n) {
    if (idxId == NULL || keyType != DT_INT || n < 0) return -99;

    // Initialize the buffer pool directly (no need for malloc, as bufferPool is not a pointer)
    RC rc = initBufferPool(&index_mgr->bufferPool, idxId, MAX_NUMBER_OF_PAGES, RS_FIFO, NULL);
    if (rc != RC_OK) {
        printf("Error: Failed to initialize buffer pool. Error code: %d\n", rc);
        free(index_mgr);  // Free allocated memory on failure
        return rc;
    }

    char data[PAGE_SIZE];
    char *page_handle_ptr = data;

    *(int *)page_handle_ptr = 1;  // We initialize with 1 nodes (root)
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = 0;  // We initialize with 0 entries
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = keyType;  // Set key type
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = n;  // Set n (order btree)
    page_handle_ptr += sizeof(int);

    // TODO??: Store the first page using the BufferManager

    SM_FileHandle fileHandle;
    rc = createPageFile(idxId);
    if (rc != RC_OK) {
        printf("Error: Failed to create page file '%s' with error code %d.\n", idxId, rc);
        return rc;
    }

    rc = openPageFile(idxId, &fileHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to open page file '%s' with error code %d.\n", idxId, rc);
        return rc;
    }
    SM_PageHandle pageHandle = data;  // Point to data directly
    rc = writeBlock(0, &fileHandle, pageHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to write to block 0 of file '%s' with error code %d.\n", idxId, rc);
        closePageFile(&fileHandle);
        return rc;
    }
    if (closePageFile(&fileHandle) != RC_OK) {
        printf("Error: Failed to close page file '%s'.\n", idxId);
    }
    return rc;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
    if (idxId == NULL) {
        printf("Error: Index ID is null.\n");
        return RC_FILE_NOT_FOUND;
    }

    // Allocate memory for the IndexManager metadata
    IndexManager *indexMgr = malloc(sizeof(IndexManager));
    if (indexMgr == NULL) {
        printf("Error: Memory allocation failed for IndexManager.\n");
        return -99;
    }

    // Initialize the buffer pool for the index
    RC rc = initBufferPool(&indexMgr->bufferPool, idxId, MAX_NUMBER_OF_PAGES, RS_FIFO, NULL);
    if (rc != RC_OK) {
        printf("Error: Failed to initialize buffer pool for BTree index. Error code: %d\n", rc);
        free(indexMgr);
        return rc;
    }

    // Open the index file and read the metadata
    SM_FileHandle fileHandle;
    rc = openPageFile(idxId, &fileHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to open index file '%s'. Error code: %d\n", idxId, rc);
        shutdownBufferPool(&indexMgr->bufferPool);
        free(indexMgr);
        return rc;
    }

    // Read the first page to retrieve metadata
    SM_PageHandle pageData = (SM_PageHandle)malloc(PAGE_SIZE);
    if (readBlock(0, &fileHandle, pageData) != RC_OK) {
        printf("Error: Failed to read metadata from index file.\n");
        free(pageData);
        closePageFile(&fileHandle);
        return -99;
    }
    BTreeManagementData *btreeMgmtData = (*tree)->mgmtData;
    // Extract metadata from the first page
    char *page_ptr = pageData;

    // Number of nodes
    btreeMgmtData->nodes = *((int *)page_ptr);
    page_ptr += sizeof(int);

    // Number of entries
    btreeMgmtData->entries = *((int *)page_ptr);
    page_ptr += sizeof(int);

    // Key type
    (*tree)->keyType = *((DataType *)page_ptr);
    page_ptr += sizeof(int);

    // Order of the B-tree (n)
    btreeMgmtData->n = *((int *)page_ptr);

    // TODO??: Retrieve the first page using the BufferManager

    // Clean up and close the file
    free(pageData);
    closePageFile(&fileHandle);

    // Attach the index ID to the BTreeHandle
    (*tree)->idxId = idxId;

    return RC_OK;
}

RC closeBtree(BTreeHandle *tree) {
    // Check if the table is valid
    if (tree == NULL || tree->idxId == NULL) return RC_FILE_NOT_FOUND;

    // Access the existing buffer pool for the table
    BM_BufferPool *bufferPool = &index_mgr->bufferPool;
    forceFlushPool(bufferPool);

    // Free the tree
    free(tree);
    return RC_OK;
}

RC deleteBtree(char *idxId) {
    // Check if the tree name is valid
    if (idxId == NULL) return RC_FILE_NOT_FOUND;

    // Use destroyPageFile to delete the tree file from disk
    RC rc = destroyPageFile(idxId);
    if (rc != RC_OK) return rc;  // Return the error code if file deletion fails

    return RC_OK;
}

// access information about a b-tree
RC getNumNodes(BTreeHandle *tree, int *result) {
    BTreeManagementData *btreeMgmtData = tree->mgmtData;
    result = &btreeMgmtData->nodes;
    return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result) {
    BTreeManagementData *btreeMgmtData = tree->mgmtData;
    result = &btreeMgmtData->entries;
    return RC_OK;
}

RC getKeyType(BTreeHandle *tree, DataType *result) {
    result = &tree->keyType;
    return RC_OK;
}

// index access
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    // TODO: Implement this function
    return RC_OK;
}

// JIO
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    // TODO: Implement this function
    return RC_OK;
}

// JIO
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
