#include "record_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h" 
#include "storage_mgr.h" 
#include "dberror.h"
#include <stdlib.h>

// TABLE AND MANAGER

// Global buffer pool pointer for the record manager

// This is the custom data structure for the Record Manager.
typedef struct RecordManager {
    BM_PageHandle page_handle_ptr;  // Buffer Manager page_handle_ptr
    BM_BufferPool bufferPool;  // Buffer Manager Buffer Pool (NOT a pointer)
} RecordManager;

RecordManager* record_mgr;  // Global pointer to Record Manager

// Constants 
#define MAX_NUMBER_OF_PAGES 10
#define PAGE_SIZE 4096  // Example size for pages, adjust as needed

// Initialize the record manager.
RC initRecordManager() {
    // Allocate memory for the RecordManager structure
    record_mgr = (RecordManager*) malloc(sizeof(RecordManager));
    if (record_mgr == NULL) 
        printf("Error: Memory allocation failed for record manager.\n");

    // Initialize the buffer pool directly (no need for malloc, as bufferPool is not a pointer)
    RC rc = initBufferPool(&record_mgr->bufferPool, "tablefile", MAX_NUMBER_OF_PAGES, RS_FIFO, NULL);
    if (rc != RC_OK) {
        printf("Error: Failed to initialize buffer pool. Error code: %d\n", rc);
        free(record_mgr);  // Free allocated memory on failure
        return rc;
    }

    return RC_OK;  // Return success if initialization succeeds
}

// Shutdown the record manager and free associated resources.
RC shutdownRecordManager() {
    if (&record_mgr->bufferPool == NULL)
        printf("Error: Buffer pool not initialized or already shut down.\n");

    // Shutdown the buffer pool
    RC rc = shutdownBufferPool(&record_mgr->bufferPool);
    if (rc != RC_OK) return rc;  // Return the error code from buffer pool shutdown

    free(&record_mgr->bufferPool);
    
    return RC_OK;  // Return success if shutdown is successful
}


// This function creates a table with table name "name" having schema specified by "schema"
extern RC createTable(char *name, Schema *schema) {
    
    // Prepare data buffer for storing schema information
    char data[PAGE_SIZE];
    char *page_handle_ptr = data;

    // |-----------------------|
    //  n_scores = 0
    // |-----------------------|
    //  where scores start = 1
    // |-----------------------|
    //  n_attributes 
    // |-----------------------|
    //  key size
    // |-----------------------|
    //  attribute list
    //     ...
    // |-----------------------|
    
    *(int*)page_handle_ptr = 0;  // Number of scores starts at 0 at the beggining
    page_handle_ptr += sizeof(int);  

    *(int*)page_handle_ptr = 1; // Setting first page for actual data (0th page is for schema)
    page_handle_ptr += sizeof(int);  

    *(int*)page_handle_ptr = schema->numAttr; // Setting the number of attributes
    page_handle_ptr += sizeof(int);  

    *(int*)page_handle_ptr = schema->keySize; // Setting the key size of the attributes
    page_handle_ptr += sizeof(int);  

    int attr_size = floor(getRecordSize(schema) / schema->numAttr);
    // Write schema information for each attribute
    for (int i = 0; i < schema->numAttr; i++) {
        
        strncpy(page_handle_ptr, schema->attrNames[i], attr_size - 1); // Setting attribute name
        page_handle_ptr[attr_size - 1] = '\0';  // Ensure null-termination
        page_handle_ptr += attr_size;

        // Setting data type of attribute
        *(int*)page_handle_ptr = (int)schema->dataTypes[i];
        page_handle_ptr += sizeof(int);

        // Setting length of the attribute type (e.g., for strings)
        *(int*)page_handle_ptr = schema->typeLength[i];
        page_handle_ptr += sizeof(int);
    }

    SM_FileHandle fileHandle; // File handling using the storage manager

    RC rc = createPageFile(name); // Creating a page file with the table name using the storage manager
    if (rc != RC_OK) return rc;

    rc = openPageFile(name, &fileHandle); // Opening the newly created page file
    if (rc != RC_OK) return rc;

    rc = writeBlock(0, &fileHandle, data); // Writing the schema to the first page of the page file
    if (rc != RC_OK) {
        closePageFile(&fileHandle);  // Close the file before returning
        return rc;
    }

    rc = closePageFile(&fileHandle); // Closing the page file after writing the schema
    if (rc != RC_OK) return rc;
    
    return RC_OK;  // Return success
}


// This function opens a table and loads its schema and metadata into the RM_TableData structure
RC openTable(RM_TableData *rel, char *name) {
    SM_FileHandle fileHandle;
    SM_PageHandle pageHandle;    
    int attributeCount, keySize, k;
    RC rc;
    // Allocate and copy the table's name
    rel->name = (char *) malloc(strlen(name) + 1);
    strcpy(rel->name, name);

    // Open the page file for the table using storage manager and if fails return the fail
    if ((rc = openPageFile(name, &fileHandle)) != RC_OK) return rc;

    // Pinning a page (putting it in Buffer Pool using Buffer Manager)
    RC rc = pinPage(&record_mgr->bufferPool, &pageHandle, 0);
    if (rc != RC_OK) {
        free(rel->name);  // Clean up
        return rc;
    }

    // Set the initial pointer to 0th location
    pageHandle = 0;

    // Retrieving total number of tuples from the page file
    int tuplesCount = *(int*)pageHandle;
    pageHandle += sizeof(int);

    // Getting the free page from the page file
    int freePage = *(int*)pageHandle;
    pageHandle += sizeof(int);

    // Getting the number of attributes from the page file
    attributeCount = *(int*)pageHandle;
    pageHandle += sizeof(int);

    // Getting the key size (number of attributes making up the primary key)
    keySize = *(int*)pageHandle;
    pageHandle += sizeof(int);

    // Allocating memory space for the schema
    Schema *schema = (Schema*) malloc(sizeof(Schema));
    if (schema == NULL) {
        unpinPage(&record_mgr->bufferPool, &pageHandle);
        free(rel->name);
        return -99;
    }

    // Allocating memory for schema components
    schema->numAttr = attributeCount;
    schema->keySize = keySize;
    schema->attrNames = (char**) malloc(sizeof(char*) * attributeCount);
    schema->dataTypes = (DataType*) malloc(sizeof(DataType) * attributeCount);
    schema->typeLength = (int*) malloc(sizeof(int) * attributeCount);
    schema->keyAttrs = (int*) malloc(sizeof(int) * keySize);

    // Check if memory allocations were successful
    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL || schema->keyAttrs == NULL) {
        printf("Error: Memory allocation failed for schema attributes.\n");
        free(schema);
        unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
        free(rel->name);
        return RC_MEMORY_ERROR;
    }

    // Allocate memory space for storing attribute name for each attribute
    for (k = 0; k < attributeCount; k++) {
        schema->attrNames[k] = (char*) malloc(ATTRIBUTE_SIZE);
        if (schema->attrNames[k] == NULL) {
            printf("Error: Memory allocation failed for attribute name.\n");
            for (int i = 0; i < k; i++) {
                free(schema->attrNames[i]);
            }
            free(schema->attrNames);
            free(schema->dataTypes);
            free(schema->typeLength);
            free(schema->keyAttrs);
            free(schema);
            unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
            free(rel->name);
            return RC_MEMORY_ERROR;
        }
    }

    // Reading the attributes from the page file
    for (k = 0; k < schema->numAttr; k++) {
        // Setting attribute name
        strncpy(schema->attrNames[k], pageHandle, ATTRIBUTE_SIZE);
        pageHandle += ATTRIBUTE_SIZE;

        // Setting data type of attribute
        schema->dataTypes[k] = *(DataType*)pageHandle;  // Use the correct DataType
        pageHandle += sizeof(int);

        // Setting length of datatype (length of STRING) of the attribute
        schema->typeLength[k] = *(int*)pageHandle;
        pageHandle += sizeof(int);
    }

    // Reading primary key information (key attribute indices)
    for (k = 0; k < schema->keySize; k++) {
        schema->keyAttrs[k] = *(int*)pageHandle;
        pageHandle += sizeof(int);
    }

    // Setting the newly created schema to the table's schema
    rel->schema = schema;

    // Unpinning the page (removing it from the buffer pool)
    rc = unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to unpin page for table '%s'. Error code: %d\n", name, rc);
        freeSchema(schema);
        free(rel->name);
        return rc;
    }

    return RC_OK;
}


RC closeTable (RM_TableData *rel){
    // TODO    
}
RC deleteTable (char *name){
    // TODO    
}
int getNumTuples (RM_TableData *rel){
    // TODO    
}

// HANDLING RECORDS IN A TABLE
RC insertRecord (RM_TableData *rel, Record *record){
    // TODO    
}
RC deleteRecord (RM_TableData *rel, RID id){
    // TODO    
}
RC updateRecord (RM_TableData *rel, Record *record){
    // TODO    
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
    // TODO    
}

// SCANS
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    // TODO    
}
RC next (RM_ScanHandle *scan, Record *record){
    // TODO    
}
RC closeScan (RM_ScanHandle *scan){
    // TODO    
}

// DEALING WITH SCHEMAS
int getRecordSize (Schema *schema){
    // TODO    
}
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    // TODO    
}
RC freeSchema (Schema *schema){
    // TODO    
}

// DEALING WITH RECORDS AND ATTRIBUTE VALUES
RC createRecord (Record **record, Schema *schema){
    // TODO    
}
RC freeRecord (Record *record){
    // TODO    
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    // TODO    
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    // TODO    
}

