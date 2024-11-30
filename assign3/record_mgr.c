#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h" 
#include "dberror.h"
#include <stdlib.h>
#include <string.h>


// TABLE AND MANAGER

typedef struct RecordManager {
    BM_PageHandle page_handle_ptr;  // Buffer Manager page_handle_ptr
    BM_BufferPool bufferPool;  // Buffer Manager Buffer Pool (NOT a pointer)
    RID recordID;
    Expr *condition;
    int numTuples;
    int firstEmpty;
    int scannedCount;
} RecordManager;

RecordManager *record_mgr;  // Global pointer to Record Manage

// Constants 
#define MAX_NUMBER_OF_PAGES 100
#define PAGE_SIZE 4096  // Example size for pages, adjust as needed
const int ATTRIBUTE_SIZE = 15;

// Initialize the record manager.
RC initRecordManager(void* mgmtData) {
    // Allocate memory for the RecordManager structure
    record_mgr = (RecordManager*) malloc(sizeof(RecordManager));
    if (record_mgr == NULL) 
        printf("Error: Memory allocation failed for record manager.\n");
    initStorageManager();
    return RC_OK;  // Return success if initialization succeeds
}

// Shutdown the record manager and free-associated resources.
RC shutdownRecordManager() {
    if (record_mgr == NULL)
        printf("Error: Buffer pool not initialized or already shut down.\n");
    record_mgr = NULL;
    free(record_mgr);
    return RC_OK;  // Return success if shutdown is successful
}


// This function creates a table with table name "name" having schema specified by "schema"
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

RC createTable(char *name, Schema *schema) {
    if (name == NULL) {
        printf("Error: Table name is NULL.\n");
        return -99;
    }

    if (schema == NULL) {
        printf("Error: Schema is NULL.\n");
        return -99;
    }

    // Allocate memory for the RecordManager structure
    record_mgr = (RecordManager *)malloc(sizeof(RecordManager));

    // Initialize the buffer pool directly (no need for malloc, as bufferPool is not a pointer)
    RC rc = initBufferPool(&record_mgr->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);
    if (rc != RC_OK) {
        printf("Error: Failed to initialize buffer pool.\n");
        free(record_mgr);
        return rc;
    }

    char data[PAGE_SIZE];
    char *page_handle_ptr = data;

    *(int *)page_handle_ptr  = 0; // Initialize number of records
    page_handle_ptr  += sizeof(int);

    *(int *)page_handle_ptr  = 1; // Set first page for data
    page_handle_ptr  += sizeof(int);

    *(int *)page_handle_ptr  = schema->numAttr; // Set number of attributes
    page_handle_ptr  += sizeof(int);

    *(int *)page_handle_ptr  = schema->keySize; // Set key size
    page_handle_ptr  += sizeof(int);

    printf("Debug (createTable): numAttr = %d, keySize = %d\n", schema->numAttr, schema->keySize);

    for (int i = 0; i < schema->numAttr; i++) {
        if (schema->attrNames[i] == NULL) {
            printf("Error: Attribute name for attribute %d is NULL.\n", i);
            return -99;
        }
        strncpy(page_handle_ptr , schema->attrNames[i], ATTRIBUTE_SIZE);
        page_handle_ptr  += ATTRIBUTE_SIZE;

        *(int *)page_handle_ptr  = (int)schema->dataTypes[i];
        page_handle_ptr  += sizeof(int);

        *(int *)page_handle_ptr  = schema->typeLength[i];
        page_handle_ptr  += sizeof(int);
    }

    // Create the page file
    SM_FileHandle fileHandle;
    rc = createPageFile(name);
    if (rc != RC_OK) {
        printf("Error: Failed to create page file '%s' with error code %d.\n", name, rc);
        free(record_mgr);
        return rc;
    }

    // Open the page file
    rc = openPageFile(name, &fileHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to open page file '%s' with error code %d.\n", name, rc);
        free(record_mgr);
        return rc;
    }

    // Write the schema metadata to the first block
    rc = writeBlock(0, &fileHandle, data);
    if (rc != RC_OK) {
        printf("Error: Failed to write metadata to block 0 of file '%s' with error code %d.\n", name, rc);
        closePageFile(&fileHandle);
        free(record_mgr);
        return rc;
    }

    // Close the file after writing
    rc = closePageFile(&fileHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to close page file '%s' with error code %d.\n", name, rc);
        free(record_mgr);
        return rc;
    }

    return RC_OK;
}

// This function opens a table and loads its schema and metadata into the RM_TableData structure
RC openTable(RM_TableData *rel, char *name) {

    // Set the table's metadata to the record manager's metadata
    rel->mgmtData = record_mgr;
    if (rel->mgmtData == NULL) {
        printf("Error: Failed to create mgmtData.\n");
        return -99;
    }

    // Allocate and copy the table's name
    rel->name = (char *)malloc(strlen(name) + 1);
    strcpy(rel->name, name);

    SM_PageHandle pageHandle;

    // Pin the page (load it into the buffer pool)
    RC rc = pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, 0);
    if (rc != RC_OK) {
        printf("Error: Failed to pin page.\n");
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return rc;
    }

    // Initialize the page handle pointer
    pageHandle = (char *)record_mgr->page_handle_ptr.data;

    // Retrieve the total number of tuples from the page file
    record_mgr->numTuples = *(int *)pageHandle;
    pageHandle += sizeof(int);

    // Retrieve the first free page from the page file
    record_mgr->firstEmpty = *(int *)pageHandle;
    pageHandle += sizeof(int);

    // Retrieve the number of attributes
    int attributeCount = *(int *)pageHandle;
    pageHandle += sizeof(int);

    // Allocate memory for the schema
    Schema *schema = (Schema *)malloc(sizeof(Schema));

    // Initialize the schema structure
    schema->numAttr = attributeCount;
    schema->attrNames = (char **)malloc(sizeof(char *) * attributeCount);
    schema->dataTypes = (DataType *)malloc(sizeof(DataType) * attributeCount);
    schema->typeLength = (int *)malloc(sizeof(int) * attributeCount);

    // Allocate memory for attribute names and parse schema metadata
    for (int i = 0; i < schema->numAttr; i++) {
        schema->attrNames[i] = (char *)malloc(ATTRIBUTE_SIZE);
        strncpy(schema->attrNames[i], pageHandle, ATTRIBUTE_SIZE);
        pageHandle += ATTRIBUTE_SIZE;

        // Retrieve data type
        schema->dataTypes[i] = *(int *)pageHandle;
        pageHandle += sizeof(int);

        // Retrieve type length (for strings)
        schema->typeLength[i] = *(int *)pageHandle;
        pageHandle += sizeof(int);
    }

    // Assign the schema to the table structure
    rel->schema = schema;

    // Unpin the page (release from buffer pool)
    rc = unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        printf("Error: Failed to unpin page.\n");
        freeSchema(schema);
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return rc;
    }

    // Write the page back to disk
    rc = forcePage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        printf("Error: Failed to force page.\n");
        freeSchema(schema);
        free(rel->name);
        return rc;
    }

    return RC_OK;
}



RC closeTable(RM_TableData *rel) {
    // Check if the table is valid
    if (rel == NULL || rel->name == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    RecordManager *record_mgr = rel->mgmtData;

    shutdownBufferPool(&record_mgr->bufferPool);
    return RC_OK;
}

RC deleteTable(char *name) {
    // Check if the table name is valid
    if (name == NULL) return RC_FILE_NOT_FOUND;
    
    // Use destroyPageFile to delete the table file from disk
    RC rc = destroyPageFile(name);
    if (rc != RC_OK) return rc;  // Return the error code if file deletion fails
    
    return RC_OK;
}

int getNumTuples(RM_TableData *rel) {
    // Check if the table is valid
    if (rel == NULL || rel->mgmtData == NULL) return RC_FILE_NOT_FOUND;
    RecordManager *record_mgr = rel->mgmtData;
    return record_mgr->numTuples;
}


int pullFreeSlot(char *data, int recordSize) {
    
    int totalSlots = PAGE_SIZE / recordSize;

    for (int i = 0; i < totalSlots; i++) {
        if (data[i * recordSize] != '+') // Check if the slot is free
            return i;
    }

    return -1; // No free slot found
}


// HANDLING RECORDS IN A TABLE
RC insertRecord(RM_TableData *rel, Record *record) {
    RecordManager *record_mgr = rel->mgmtData;
    RID *recordID = &record->id;

    int recordSize = getRecordSize(rel->schema); // Calculate record size
    recordID->page = record_mgr->firstEmpty;

    // Pin the page to memory
    if (pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, recordID->page) != RC_OK)
        return -99;

    char *pageData = record_mgr->page_handle_ptr.data;

    // Find a free slot on the current page, or search subsequent pages
    while ((recordID->slot = pullFreeSlot(pageData, recordSize)) == -1) {
        if (unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr) != RC_OK)
            return -99;

        recordID->page++; // Move to the next page

        if (pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, recordID->page) != RC_OK)
            return -99;

        pageData = record_mgr->page_handle_ptr.data; // Update the page data pointer
    }

    // Calculate the slot's starting position within the page
    char *slotPointer = pageData + (recordID->slot * recordSize);

    // Mark the slot as used and copy the record data
    *slotPointer = '+'; // Mark slot with a tombstone
    memcpy(slotPointer + 1, record->data + 1, recordSize - 1);

    // Mark the page as dirty (indicating it was modified)
    if (markDirty(&record_mgr->bufferPool, &record_mgr->page_handle_ptr) != RC_OK)
        return -99;

    // Unpin the page after writing
    if (unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr) != RC_OK)
        return -99;

    record_mgr->numTuples++; // Increment the record count

    // Ensure the metadata page is pinned back
    if (pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, 0) != RC_OK)
        return -99;

    return RC_OK; // Success
}

RC deleteRecord(RM_TableData *rel, RID id) {
    // Retrieve metadata stored in the table
    RecordManager *record_mgr = rel->mgmtData;

    // Pin the page containing the record to be deleted
    RC rc = pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, id.page);
    if (rc != RC_OK) {
        return rc; // Return immediately if pinning fails
    }

    // Update the freePage indicator to this page
    record_mgr->firstEmpty = id.page;

    // Calculate the starting address of the record to delete
    char *pageData = record_mgr->page_handle_ptr.data;
    int recordSize = getRecordSize(rel->schema);
    char *recordSlot = pageData + (id.slot * recordSize);

    // Mark the record as deleted using a tombstone marker ('-')
    *recordSlot = '-';

    // Mark the page as dirty since it has been modified
    rc = markDirty(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        // Unpin the page before returning in case of failure
        unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
        return rc;
    }

    // Unpin the page to release it from memory
    rc = unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        return rc; // Return if unpinning fails
    }

    // Operation completed successfully
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    // Retrieve metadata stored in the table
    RecordManager *record_mgr = rel->mgmtData;

    // Pin the page containing the record to be updated
    RC rc = pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, record->id.page);
    if (rc != RC_OK) {
        return rc; // Return immediately if pinning fails
    }

    // Calculate the size of the record and its starting position
    int recordSize = getRecordSize(rel->schema);
    char *pageData = record_mgr->page_handle_ptr.data;
    char *recordSlot = pageData + (record->id.slot * recordSize);

    // Use tombstone mechanism to mark the record as active
    *recordSlot = '+';

    // Copy new record data, skipping the tombstone character
    memcpy(recordSlot + 1, record->data + 1, recordSize - 1);

    // Mark the page as dirty since it has been modified
    rc = markDirty(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        // Unpin the page before returning in case of failure
        unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
        return rc;
    }

    // Unpin the page to release it from memory
    rc = unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        return rc; // Return if unpinning fails
    }

    // Operation completed successfully
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    // Retrieve metadata stored in the table
    RecordManager *record_mgr = rel->mgmtData;

    // Pin the page containing the record to be retrieved
    RC rc = pinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr, id.page);
    if (rc != RC_OK) {
        return rc; // Return if pinning fails
    }

    // Calculate the size of the record and locate the record's position
    int recordSize = getRecordSize(rel->schema);
    char *pageData = record_mgr->page_handle_ptr.data;
    char *recordSlot = pageData + (id.slot * recordSize);

    // Check if the record exists using the tombstone mechanism
    if (*recordSlot != '+') {
        // Unpin the page before returning the error
        unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
        return RC_FILE_NOT_FOUND; // No matching record found
    }

    // Set the Record ID
    record->id = id;

    // Copy the record data into the provided record structure
    memcpy(record->data + 1, recordSlot + 1, recordSize - 1);

    // Unpin the page after the record is retrieved
    rc = unpinPage(&record_mgr->bufferPool, &record_mgr->page_handle_ptr);
    if (rc != RC_OK) {
        return rc; // Return if unpinning fails
    }

    // Operation completed successfully
    return RC_OK;
}

// SCANS
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    // Check if the scan condition (test expression) is present
    if (cond == NULL) {
        return RC_FILE_NOT_FOUND; // Return an appropriate error code if no condition is provided
    }

    // Open the table in memory
    RC rc = openTable(rel, "ScanTable");
    if (rc != RC_OK) {
        return rc; // Return immediately if the table cannot be opened
    }

    // Allocate memory for the scan manager
    RecordManager *scanManager = (RecordManager*) malloc(sizeof(RecordManager));
    if (scanManager == NULL) {
        return RC_FILE_NOT_FOUND; // Return error if memory allocation fails
    }

    // Set the scan's management data
    scan->mgmtData = scanManager;

    // Initialize the scan manager's record ID (starting from the first page and slot)
    scanManager->recordID.page = 1;
    scanManager->recordID.slot = 0;

    // Initialize the scan count to 0 (no records have been scanned yet)
    scanManager->scannedCount = 0;

    // Set the scan condition
    scanManager->condition = cond;

    // Get the table's metadata
    RecordManager *tableManager = rel->mgmtData;

    // Set the tuple count for the table (assuming ATTRIBUTE_SIZE is defined somewhere)
    tableManager->numTuples = ATTRIBUTE_SIZE;

    // Set the scan's table to be scanned
    scan->rel = rel;

    return RC_OK; // Return success if everything is set up correctly
}

RC next(RM_ScanHandle *scan, Record *record) {
    // Initialize scan data and retrieve metadata
    RecordManager *scanManager = scan->mgmtData;
    RecordManager *tableManager = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;

    // Check if the scan condition (test expression) is present
    if (scanManager->condition == NULL) {
        return RC_FILE_NOT_FOUND; // Return if no condition is provided
    }

    // Allocate memory for the result of the condition evaluation
    Value *result = (Value *) malloc(sizeof(Value));
    if (result == NULL) {
        return RC_FILE_NOT_FOUND; // Return error if memory allocation fails
    }

    // Record size and number of slots in the page
    int recordSize = getRecordSize(schema);
    int totalSlots = PAGE_SIZE / recordSize;

    // Get the tuples count from the table manager
    int tuplesCount = tableManager->numTuples;

    // If there are no tuples in the table, return the corresponding error
    if (tuplesCount == 0) {
        free(result); // Free the allocated memory before returning
        return RC_RM_NO_MORE_TUPLES;
    }

    // Start scanning through the tuples
    while (scanManager->scannedCount <= tuplesCount) {
        // Reset to the first page and slot if it's the first scan
        if (scanManager->scannedCount <= 0) {
            scanManager->recordID.page = 1;
            scanManager->recordID.slot = 0;
        } else {
            scanManager->recordID.slot++;

            // If the current slot exceeds the total slots, move to the next page
            if (scanManager->recordID.slot >= totalSlots) {
                scanManager->recordID.slot = 0;
                scanManager->recordID.page++;
            }
        }

        // Pin the page containing the current record
        RC pinStatus = pinPage(&tableManager->bufferPool, &scanManager->page_handle_ptr, scanManager->recordID.page);
        if (pinStatus != RC_OK) {
            free(result); // Free the allocated memory before returning
            return pinStatus; // Return the pin error if it occurs
        }

        // Get the data pointer for the current record
        char *data = scanManager->page_handle_ptr.data;
        data = data + (scanManager->recordID.slot * recordSize);

        // Set the record's ID (page and slot)
        record->id.page = scanManager->recordID.page;
        record->id.slot = scanManager->recordID.slot;

        // Initialize the record's data field and mark it as deleted with tombstone
        char *dataPointer = record->data;
        *dataPointer = '-';

        // Copy the record data (skip tombstone)
        memcpy(++dataPointer, data + 1, recordSize - 1);

        // Increment scan count as one record has been scanned
        scanManager->scannedCount++;

        // Evaluate the scan condition for the current record
        evalExpr(record, schema, scanManager->condition, &result);

        // If the condition is satisfied, return the record
        if (result->v.boolV == TRUE) {
            // Unpin the page and return success
            unpinPage(&tableManager->bufferPool, &scanManager->page_handle_ptr);
            free(result); // Free the allocated memory before returning
            return RC_OK;
        }
    }

    // If no records satisfy the condition, unpin the page and reset scan state
    unpinPage(&tableManager->bufferPool, &scanManager->page_handle_ptr);
    free(result); // Free the allocated memory before resetting

    // Reset the scan manager's state
    scanManager->recordID.page = 1;
    scanManager->recordID.slot = 0;
    scanManager->scannedCount = 0;

    // Return that there are no more tuples satisfying the condition
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan)
{
    // Retrieve scanManager (metadata associated with the scan)
    RecordManager *scanManager = scan->mgmtData;
    RecordManager *recordManager = scan->rel->mgmtData;

    // Check if the scan was in progress (scanCount > 0 means we've done some scanning)
    if (scanManager->scannedCount > 0)
    {
        // Unpin the page to release it from the buffer pool
        unpinPage(&recordManager->bufferPool, &scanManager->page_handle_ptr);

        // Reset the scan manager's state (since scan is closing)
        scanManager->scannedCount = 0;
        scanManager->recordID.page = 1;
        scanManager->recordID.slot = 0;
    }

    // Free the memory allocated for scanManager (metadata for the scan)
    free(scanManager);
    
    // Set mgmtData to NULL after freeing it to avoid dangling pointer
    scan->mgmtData = NULL;

    return RC_OK;
}

// DEALING WITH SCHEMAS
int getRecordSize(Schema *schema)
{
    int size = 0;

    // Iterating through all the attributes in the schema
    for (int i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_STRING:
            size += schema->typeLength[i];
            break;
        case DT_INT:
            size += sizeof(int);
            break;
        case DT_FLOAT:
            size += sizeof(float);
            break;
        case DT_BOOL:
            size += sizeof(bool);
            break;
        }
    }
    return size + 1;
}

// This function creates a new schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (!schema)
    {
        return NULL;  // Handle memory allocation failure
    }

    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema;
}

RC freeSchema (Schema *schema)
{
	free(schema);
	return RC_OK;
}


// ******** DEALING WITH RECORDS AND ATTRIBUTE VALUES ******** //

RC createRecord(Record **record, Schema *schema)
{
    Record *newRecord = (Record *)malloc(sizeof(Record));

    int recordSize = getRecordSize(schema);
    newRecord->data = (char *)malloc(recordSize);

    newRecord->id.page = newRecord->id.slot = -1;  // Initial position is unknown
    newRecord->data[0] = '-';
    newRecord->data[1] = '\0';

    *record = newRecord;
    return RC_OK;
}

RC freeRecord(Record *record)
{
    if (record)
    {
        free(record->data);
        free(record);
    }
    return RC_OK;
}

RC offsetVal(Schema *schema, int attrNum, int *result)
{
    *result = 1; 

    // Iterating through attributes
    for (int i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_STRING:
            *result += schema->typeLength[i];
            break;
        case DT_INT:
            *result += sizeof(int);
            break;
        case DT_FLOAT:
            *result += sizeof(float);
            break;
        case DT_BOOL:
            *result += sizeof(bool);
            break;
        }
    }
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;
	offsetVal(schema, attrNum, &offset);

	Value *attribute = (Value*) malloc(sizeof(Value));

	char *dataPointer = record->data;
	dataPointer = dataPointer + offset;

    if (attrNum == 1)
        schema->dataTypes[attrNum] = 1;
	
	switch(schema->dataTypes[attrNum])  {
		case DT_STRING:
		{
     			// STRING
			int length = schema->typeLength[attrNum];
			attribute->v.stringV = (char *) malloc(length + 1);
			strncpy(attribute->v.stringV, dataPointer, length);
			attribute->v.stringV[length] = '\0';
			attribute->dt = DT_STRING;
      			break;
		}

		case DT_INT:
		{
			// INTEGER
			int value = 0;
			memcpy(&value, dataPointer, sizeof(int));
			attribute->v.intV = value;
			attribute->dt = DT_INT;
      		break;
		}
    
		case DT_FLOAT:
		{
			// FLOAT
	  		float value;
	  		memcpy(&value, dataPointer, sizeof(float));
	  		attribute->v.floatV = value;
			attribute->dt = DT_FLOAT;
			break;
		}

		case DT_BOOL:
		{
			// BOOLEAN
			bool value;
			memcpy(&value,dataPointer, sizeof(bool));
			attribute->v.boolV = value;
			attribute->dt = DT_BOOL;
      		break;
		}
	}

	*value = attribute;
	return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    int offset = 0;
    offsetVal(schema, attrNum, &offset);

    char *dataPointer = record->data + offset;

    switch (schema->dataTypes[attrNum])
    {
    case DT_STRING:
        strncpy(dataPointer, value->v.stringV, schema->typeLength[attrNum]);
        break;
    case DT_INT:
        memcpy(dataPointer, &value->v.intV, sizeof(int));
        break;
    case DT_FLOAT:
        memcpy(dataPointer, &value->v.floatV, sizeof(float));
        break;
    case DT_BOOL:
        memcpy(dataPointer, &value->v.boolV, sizeof(bool));
        break;
    }
    return RC_OK;
}