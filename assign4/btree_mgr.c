#include "btree_mgr.h"
#include <buffer_mgr.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "storage_mgr.h"

typedef struct IndexManager {
    BM_PageHandle page_handle_ptr;  // Buffer Manager page_handle_ptr
    BM_BufferPool bufferPool;  // Buffer Manager Buffer Pool (NOT a pointer)
    Node rootNode;
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
    //  root node pointer
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

    *(int *)page_handle_ptr = 1;  // We initialize with 1 node (root)
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = 0;  // We initialize with 0 entries
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = keyType;  // Set key type
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = n;  // Set n (order of B-tree)
    page_handle_ptr += sizeof(int);

    // Initialize the root node
    Node *rootNode = malloc(sizeof(Node));
    if (rootNode == NULL) {
        printf("Error: Failed to allocate memory for root node.\n");
        shutdownBufferPool(&index_mgr->bufferPool);
        return -99;
    }

    // Initialize the keys array with zeros (size n)
    rootNode->keys = (DataType *)calloc(n, sizeof(DataType));
    if (rootNode->keys == NULL) {
        printf("Error: Failed to allocate memory for root node keys.\n");
        free(rootNode);
        shutdownBufferPool(&index_mgr->bufferPool);
        return -99;
    }

    // Initialize the children array with zeros (size n+1)
    rootNode->children = (Node **)calloc(n + 1, sizeof(Node *));
    if (rootNode->children == NULL) {
        printf("Error: Failed to allocate memory for root node children.\n");
        free(rootNode->keys);
        free(rootNode);
        shutdownBufferPool(&index_mgr->bufferPool);
        return -99;
    }

    // Set the node properties
    rootNode->isLeaf = true;
    rootNode->numKeys = 0;

    // Serialize the root node into page_handle_ptr
    *(bool *)page_handle_ptr = rootNode->isLeaf;  // Store whether it's a leaf node
    page_handle_ptr += sizeof(bool);

    *(int *)page_handle_ptr = rootNode->numKeys;  // Store the number of keys
    page_handle_ptr += sizeof(int);

    // Store the keys (initialized to 0)
    for (int i = 0; i < n; i++) {
        *(Value *)page_handle_ptr = rootNode->keys[i];
        page_handle_ptr += sizeof(Value);
    }

    // Store the children pointers (initialized to NULL)
    for (int i = 0; i < n + 1; i++) {
        *(Node **)page_handle_ptr = rootNode->children[i];
        page_handle_ptr += sizeof(Node *);
    }

    // Save the root node in the index manager
    index_mgr->rootNode = *rootNode;

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

    // Allocate the BTreeManagementData structure
    BTreeManagementData *btreeMgmtData = malloc(sizeof(BTreeManagementData));
    if (btreeMgmtData == NULL) {
        printf("Error: Memory allocation failed for BTreeManagementData.\n");
        free(pageData);
        closePageFile(&fileHandle);
        return -99;
    }

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
    page_ptr += sizeof(int);

    // Retrieve the root node pointer
    btreeMgmtData->rootNode = (Node *) page_ptr;

    // Clean up and close the file
    free(pageData);
    closePageFile(&fileHandle);

    // Attach the BTreeManagementData to the BTreeHandle
    (*tree)->mgmtData = btreeMgmtData;
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

bool isKey1GreaterOrEqualTo2(Value key1, Value key2) {
    if (key1.dt == DT_INT && key2.dt == DT_INT) return key1.v.intV >= key2.v.intV;
    return false;
}

bool isKey1EqualTo2(Value key1, Value key2) {
    if (key1.dt == DT_INT && key2.dt == DT_INT) return key1.v.intV == key2.v.intV;
    return false;
}

// index access
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    BTreeManagementData *btreeMgmtData = tree->mgmtData;
    Node currentNode = *btreeMgmtData->rootNode;
    // First we search for the leaf node that should contain the key
    while(!currentNode.isLeaf) {
        // We loop through the keys of each node searching for the node
        for (int i=0; i < currentNode.numKeys; i++) {
            // If the key we are searching is lesser than the current, it has to be in the last children
            if (isKey1GreaterOrEqualTo2(currentNode.keys[i], *key)) {
                currentNode=*currentNode.children[i];
                break;
            } if (i==currentNode.numKeys-1) { // If we are at the last one, retrieve the n+1 children
                currentNode=*currentNode.children[i+1];
                break;
            }
        }
    }
    // When we have the node that contains the key, we retrieve it
    for (int i=0; i < currentNode.numKeys; i++) {
        if (isKey1EqualTo2(currentNode.keys[i], *key)) {
            result = &currentNode.leafRIDList[i];
            return RC_OK;
        }
    }
    return RC_IM_KEY_NOT_FOUND;
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

void traverseAndCollectKeys(Node *node, RID *keysList, int *keyCount, int order) {
    if (node == NULL) return;  // Base case: no node to process
    if (node->isLeaf) {
        // If the node is a leaf, collect all its keys
        for (int i = 0; i < node->numKeys; i++) {
            keysList[*keyCount] = node->leafRIDList[i];
            (*keyCount)++;
        }
    } else {
        // If the node is not a leaf, recursively call this function on all children
        for (int i = 0; i <= node->numKeys; i++) {  // Iterate through children
            traverseAndCollectKeys(node->children[i], keysList, keyCount, order);
        }
    }
}

Value *getAllKeys(BTreeHandle *tree, int *numKeys) {

}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    BTreeManagementData *btreeMgmtData = tree->mgmtData;

    // Estimate the maximum number of keys (order * number of nodes)
    int maxKeys = btreeMgmtData->entries; // Watch out! this might produce seg fault, things of working with c i guess

    // Allocate memory for the keys list
    RID *keysList = malloc(maxKeys * sizeof(Value));
    if (keysList == NULL) {
        printf("Error: Memory allocation failed for keys list.\n");
        return NULL;
    }

    // Initialize the key count
    int keyCount = 0;

    // Start traversal from the root node
    traverseAndCollectKeys(btreeMgmtData->rootNode, keysList, &keyCount, btreeMgmtData->n);

    //Store it in the handle
    BT_ScanHandle hand = **handle;
    BT_ScanHandleManagementData* handle_mgmt_data = hand.mgmtData;
    handle_mgmt_data->list = keysList;
    handle_mgmt_data->current = 0;
    handle_mgmt_data->total = keyCount;
    return RC_OK;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    BT_ScanHandleManagementData* handle_mgmt_data = handle->mgmtData;
    if (handle_mgmt_data->current == handle_mgmt_data->total) return RC_RM_NO_MORE_TUPLES;
    result = &handle_mgmt_data->list[handle_mgmt_data->current];
    handle_mgmt_data->current++;
    return RC_OK;
}

RC closeTreeScan(BT_ScanHandle *handle) {
    if (handle == NULL || handle->mgmtData == NULL) {
        printf("Error: Scan handle or its management data is NULL.\n");
        return -99;
    }

    // Access the management data
    BT_ScanHandleManagementData *handle_mgmt_data = handle->mgmtData;

    // Free the allocated list of keys
    if (handle_mgmt_data->list != NULL) {
        free(handle_mgmt_data->list);
        handle_mgmt_data->list = NULL;
    }

    // Free the handle's management data
    free(handle_mgmt_data);
    handle->mgmtData = NULL;

    // Free the handle itself
    free(handle);

    return RC_OK;
}

#define MAX_STRING_LENGTH 10000  // Adjust as needed to handle large trees

typedef struct PrintNode {
    int pos;
    char *content;
} PrintNode;

// Recursive function to perform DFS and generate the string representation
void dfsPrint(Node *node, PrintNode **printList, int *nodeCounter) {
    if (node == NULL) return; // Base case: no node to process

    // Start this node's string representation
    char nodeString[512]; // Temporary buffer for this node
    PrintNode *printNode = malloc(sizeof(PrintNode));
    printNode->pos = *nodeCounter;
    printNode->content = malloc(512*sizeof(char));

    int currentNodePosition = *nodeCounter; // Current node position in DFS
    (*nodeCounter)++;

    if (node->isLeaf) {
        // For leaf nodes, include keys and RIDs
        for (int i = 0; i < node->numKeys; i++) { //iterate through all the keys
            char *keyString = serializeValue(&node->keys[i]); // Serialize the key

            char ridString[20]; // Serialize the RID (assuming it's a structure with page and slot fields)
            sprintf(ridString, "=%d.%d , ", node->leafRIDList[i].page, node->leafRIDList[i].slot);

            // Add key and RID to this node's string
            strcat(nodeString, ridString);
            strcat(nodeString, keyString);

            if (i == node->numKeys - 1) {
                sprintf(ridString, "=%d.%d", node->leafRIDList[i+1].page, node->leafRIDList[i+1].slot);
                strcat(nodeString, ridString); // Add last RID
            }
            free(keyString); // Clean up serialized key memory
        }
        strcpy(printNode->content, nodeString); //Store it in
    } else {
        // For internal nodes, include child pointers and keys
        for (int i = 0; i <= node->numKeys; i++) { //For each of the children
            char pointerString[20]; // Serialize the RID (assuming it's a structure with page and slot fields)
            sprintf(pointerString, "%d , ", nodeCounter+1);
            strcat(nodeString, pointerString);

            dfsPrint(node->children[i], printList, nodeCounter); //Recursively call to know the pos of the childrens

            char *keyString = "";
            if (i != node->numKeys - 1) keyString = strcat(serializeValue(&node->keys[i]), ", "); // Serialize the key
            strcat(nodeString, keyString);
            free(keyString); // Clean up serialized key memory
        }
    }

    // Close this node's string representation
    strcat(nodeString, "]\n");

    // Add this node's string to the overall result
    printList[currentNodePosition] = printNode;
}

// Main function to generate the tree string
char *printTree(BTreeHandle *tree) {
    if (tree == NULL || tree->mgmtData == NULL) {
        printf("Error: Tree or management data is NULL.\n");
        return NULL;
    }
    BTreeManagementData *btreeMgmtData = tree->mgmtData;
    PrintNode* printList = malloc(sizeof(PrintNode)*btreeMgmtData->nodes*2);

    // Allocate memory for the resulting string
    char *result = malloc(MAX_STRING_LENGTH);
    if (result == NULL) {
        printf("Error: Memory allocation failed for tree string.\n");
        return NULL;
    }
    result[0] = '\0';  // Initialize the result string

    // Start the DFS from the root node
    int nodeCounter = 0;  // Used to assign node positions in depth-first pre-order
    dfsPrint(btreeMgmtData->rootNode, printList, &nodeCounter);

    //Create the resulting string
    for (int i = 0; i < sizeof(printList); i++) {
        strcat(result, "(");
        char posString[10];
        sprintf(posString, "%d", printList[i].pos);
        strcat(result, posString);
        strcat(result, ") [");
        strcat(result, printList[i].content);
        strcat(result, "]\n");
    }
    return result;
}


