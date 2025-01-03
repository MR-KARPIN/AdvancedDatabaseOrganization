#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h" 
#include "dberror.h"
#include <stdlib.h>
#include <string.h>


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
RC initRecordManager(void* mgmtData) {
    // Allocate memory for the RecordManager structure
    record_mgr = (RecordManager*) malloc(sizeof(RecordManager));
    if (record_mgr == NULL) 
        printf("Error: Memory allocation failed for record manager.\n");

    return RC_OK;  // Return success if initialization succeeds
}

// Shutdown the record manager and free-associated resources.
RC shutdownRecordManager() {
    if (record_mgr == NULL)
        printf("Error: Buffer pool not initialized or already shut down.\n");

    // Shutdown the buffer pool
    RC rc = shutdownBufferPool(&record_mgr->bufferPool);
    if (rc != RC_OK) return rc;  // Return the error code from buffer pool shutdown

    
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
extern RC createTable(char *name, Schema *schema) {
    if (name == NULL) {
        printf("Error: Table name is NULL.\n");
        return -99;
    }

    if (schema == NULL) {
        printf("Error: Schema is NULL.\n");
        return -99;
    }

    // Initialize the buffer pool directly (no need for malloc, as bufferPool is not a pointer)
    RC rc = initBufferPool(&record_mgr->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_FIFO, NULL);
    if (rc != RC_OK) {
        printf("Error: Failed to initialize buffer pool. Error code: %d\n", rc);
        free(record_mgr);  // Free allocated memory on failure
        return rc;
    }

    char data[PAGE_SIZE];
    char *page_handle_ptr = data;

    *(int *)page_handle_ptr = 0;  // Initialize number of records
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = 1;  // Set first page for data
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = schema->numAttr;  // Set number of attributes
    page_handle_ptr += sizeof(int);

    *(int *)page_handle_ptr = schema->keySize;  // Set key size
    page_handle_ptr += sizeof(int);

    for (int i = 0; i < schema->numAttr; i++) {
        if (schema->attrNames[i] == NULL) {
            printf("Error: Attribute name for attribute %d is NULL.\n", i);
            return -99;
        }
        int nameLength = strlen(schema->attrNames[i]) + 1;
        *(int *)page_handle_ptr = nameLength;
        page_handle_ptr += sizeof(int);

        strncpy(page_handle_ptr, schema->attrNames[i], nameLength);
        page_handle_ptr += nameLength;

        *(int *)page_handle_ptr = (int)schema->dataTypes[i];
        page_handle_ptr += sizeof(int);

        *(int *)page_handle_ptr = schema->typeLength[i];
        page_handle_ptr += sizeof(int);
    }

    SM_FileHandle fileHandle;
    rc = createPageFile(name);
    if (rc != RC_OK) {
        printf("Error: Failed to create page file '%s' with error code %d.\n", name, rc);
        return rc;
    }

    rc = openPageFile(name, &fileHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to open page file '%s' with error code %d.\n", name, rc);
        return rc;
    }
    SM_PageHandle pageHandle = data;  // Point to data directly
    rc = writeBlock(0, &fileHandle, pageHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to write to block 0 of file '%s' with error code %d.\n", name, rc);
        closePageFile(&fileHandle);
        return rc;
    }

    if (closePageFile(&fileHandle) != RC_OK) {
        printf("Error: Failed to close page file '%s'.\n", name);
    }

    return rc;
}





// This function opens a table and loads its schema and metadata into the RM_TableData structure
RC openTable(RM_TableData *rel, char *name) {
    // Allocate memory for mgmtData
    rel->mgmtData = (TableMgmtData *) malloc(sizeof(TableMgmtData));
    if (rel->mgmtData == NULL) {
        printf("Error: Failed to create mgmtData.\n");
        return -99;
    }

    SM_FileHandle fileHandle;
    BM_PageHandle *pageHandle = malloc(sizeof(BM_PageHandle));
    if (pageHandle == NULL) {
        printf("Error: Failed to create BM_PageHandle.\n");
        free(rel->mgmtData);
        return -99;
    }

    RC rc = openPageFile(name, &fileHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to open page file '%s' with error code %d.\n", name, rc);
        free(rel->mgmtData);
        free(pageHandle);
        return rc;
    }


    // Allocate and copy the table's name
    rel->name = (char *) malloc(strlen(name) + 1);
    strcpy(rel->name, name);

    // Pin the page (load it into the buffer pool)
    rc = pinPage(&record_mgr->bufferPool, pageHandle, 0);
    if (rc != RC_OK) {
        printf("Error: Failed to pin page.\n");
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return rc;
    }

    // Retrieve attributeCount and keySize from the beginning of pageHandle->data
    char *data = pageHandle->data;

    // Retrieve the number of tuples (this is optional based on your schema layout)
    rel->mgmtData->numTuples = *(int *)data;
    data += sizeof(int);

    // Retrieve the start page for data
    int startPage = *(int *)data;
    data += sizeof(int);

    // Retrieve number of attributes and key size
    int attributeCount = *(int *)data;
    data += sizeof(int);
    int keySize = *(int *)data;
    data += sizeof(int);


    // Validate attributeCount and keySize values to prevent out-of-bounds allocations
    if (attributeCount <= 0 || attributeCount > 1000 || keySize < 0 || keySize > attributeCount) {
        printf("Error: Invalid values for attributeCount (%d) or keySize (%d).\n", attributeCount, keySize);
        unpinPage(&record_mgr->bufferPool, pageHandle);
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return -99;
    }

    // Allocate memory for schema
    Schema *schema = malloc(sizeof(Schema));
    if (schema == NULL) {
        printf("Error: Memory allocation failed for schema.\n");
        unpinPage(&record_mgr->bufferPool, pageHandle);
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return -99;
    }

    // Initialize schema structure
    schema->numAttr = attributeCount;
    schema->keySize = keySize;
    schema->attrNames = (char **)malloc(sizeof(char *) * attributeCount);
    schema->dataTypes = (DataType *)malloc(sizeof(DataType) * attributeCount);
    schema->typeLength = (int *)malloc(sizeof(int) * attributeCount);
    schema->keyAttrs = (int *)malloc(sizeof(int) * keySize);

    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL || schema->keyAttrs == NULL) {
        printf("Error: Memory allocation failed for schema components.\n");
        freeSchema(schema);
        unpinPage(&record_mgr->bufferPool, pageHandle);
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return -99;
    }

    // Retrieve attributes from the page
    for (int i = 0; i < attributeCount; i++) {
        // Retrieve and allocate space for the attribute name
        int nameLength = *(int *)data;
        data += sizeof(int);

        schema->attrNames[i] = (char *)malloc(nameLength);
        if (schema->attrNames[i] == NULL) {
            printf("Error: Memory allocation failed for attribute name.\n");
            freeSchema(schema);
            unpinPage(&record_mgr->bufferPool, pageHandle);
            free(rel->name);
            free(rel->mgmtData);
            free(pageHandle);
            return -99;
        }
        strncpy(schema->attrNames[i], data, nameLength - 1);
        schema->attrNames[i][nameLength - 1] = '\0'; // Ensure null termination
        data += nameLength;

        // Retrieve the data type and type length
        schema->dataTypes[i] = *(DataType *)data;
        data += sizeof(int);

        schema->typeLength[i] = *(int *)data;
        data += sizeof(int);
    }

    // Retrieve primary key attribute indices
    for (int i = 0; i < keySize; i++) {
        schema->keyAttrs[i] = *(int *)data;
        data += sizeof(int);
    }

    // Assign schema to table structure
    rel->schema = schema;
    rel->mgmtData->recordSize = getRecordSize(schema);

    // Unpin and free resources
    rc = unpinPage(&record_mgr->bufferPool, pageHandle);
    if (rc != RC_OK) {
        printf("Error: Failed to unpin page.\n");
        freeSchema(schema);
        free(rel->name);
        free(rel->mgmtData);
        free(pageHandle);
        return rc;
    }

    // Free file and page handles as they are no longer needed
    free(pageHandle);

    return RC_OK;
}



RC closeTable(RM_TableData *rel) {
    // Check if the table is valid
    if (rel == NULL || rel->name == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Access the existing buffer pool for the table
    BM_BufferPool *bufferPool = &record_mgr->bufferPool;
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) &bufferPool->mgmtData;

    forceFlushPool(bufferPool);

    // Free the schema and other allocated resources
    freeSchema(rel->schema);
    rel->schema = NULL;

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
    
    return (int) rel->mgmtData->numTuples;
}


RID pullFreeRID(RM_TableData *rel) {
    
    // Retrieve the first free RID
    RID freeRID = rel->mgmtData->freeRecords[0];
    
    // Shift the remaining RIDs in the array to remove the first one
    for (int i = 1; i < rel->mgmtData->numFreeRecords; i++) rel->mgmtData->freeRecords[i - 1] = rel->mgmtData->freeRecords[i];    
    rel->mgmtData->numFreeRecords--; // Decrement the number of free records

    if (rel->mgmtData->numFreeRecords == 0) {
        // Expand the freeRecords array with a new page of RIDs
        int newPage = freeRID.page + 1;
        int numRecords = rel->mgmtData->numRecords;
        int newTotalRecords = rel->mgmtData->numFreeRecords + numRecords;

        // Reallocate memory to accommodate the new RIDs
        rel->mgmtData->freeRecords = realloc(rel->mgmtData->freeRecords, newTotalRecords * sizeof(RID));

        // Initialize new RIDs for the new page
        for (int i = 0; i < numRecords; i++) {
            rel->mgmtData->freeRecords[rel->mgmtData->numFreeRecords + i].page = newPage;
            rel->mgmtData->freeRecords[rel->mgmtData->numFreeRecords + i].slot = i;
        }
        rel->mgmtData->numFreeRecords += numRecords; // Update the number of free records
    }
    return freeRID;
}


// HANDLING RECORDS IN A TABLE
RC insertRecord(RM_TableData *rel, Record *record) {
    TableMgmtData tb_data = *(TableMgmtData*) &rel->mgmtData;
    
    // Retrieve information from the table management structure
    int recordSize = tb_data.recordSize;        // Size of each record
    BM_PageHandle *page = tb_data.pageHandle;  // Page handle for buffer operations
    BM_BufferPool *bm = (BM_BufferPool *) &tb_data.bufferManager;    // Buffer pool for the table

    RID nextRecord = pullFreeRID(rel);

    if (pinPage(bm, page, nextRecord.page) != RC_OK) return -99; // Pin the page into memory

    char *pageData = page->data; // Pointer for convenient page data access, no malloc/free required

    // Calculate offset where the record will be inserted within the page
    int offset = nextRecord.slot * recordSize; // Slot start position, with slot numbers starting at 0

    // Copy record data to the designated position in the page
    memcpy(pageData + offset, record->data, recordSize);

    // Mark the page as dirty since it has been modified
    RC rc = markDirty(bm, page) ;
    if (rc != RC_OK) return rc;
    
    // Unpin the page after modification
    rc = unpinPage(bm, page);
    if (rc != RC_OK) return rc;
    
    record->id.page = nextRecord.page;
    record->id.slot = nextRecord.slot;

    tb_data.totalNumRecords++; // Update the total record count in the table
    
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
    TableMgmtData *tb_data = (TableMgmtData *)rel->mgmtData;

    // Retrieve information from the table management structure
    BM_PageHandle *page = tb_data->pageHandle;
    BM_BufferPool *bm = tb_data->bufferManager;
    int recordSize = tb_data->recordSize;

    // Pin the page containing the record
    RC rc = pinPage(bm, page, id.page);
    if (rc != RC_OK) return rc;

    // Calculate the offset within the page for this record
    char *pageData = page->data;
    int offset = id.slot * recordSize;

    // Mark the record as deleted (e.g., zero out its memory)
    memset(pageData + offset, 0, recordSize);

    // Mark page as dirty and unpin it
    rc = markDirty(bm, page);
    if (rc != RC_OK) return rc;

    rc = unpinPage(bm, page);
    if (rc != RC_OK) return rc;

    // Add the deleted RID back to the first position in the free list
    tb_data->freeRecords = realloc(tb_data->freeRecords, (tb_data->numFreeRecords + 1) * sizeof(RID));
    if (tb_data->freeRecords == NULL) return -99;

    // Shift existing RIDs to the right by one position
    for (int i = tb_data->numFreeRecords; i > 0; i--) tb_data->freeRecords[i] = tb_data->freeRecords[i - 1];

    // Insert the new RID at the first position
    tb_data->freeRecords[0] = id;
    tb_data->numFreeRecords++;

    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    TableMgmtData *tb_data = (TableMgmtData *)rel->mgmtData;
    
    // Retrieve information from the table management structure
    BM_PageHandle *page = tb_data->pageHandle;
    BM_BufferPool *bm = tb_data->bufferManager;
    int recordSize = tb_data->recordSize;
    
    // Pin the page containing the record to update
    RC rc = pinPage(bm, page, record->id.page);
    if (rc != RC_OK) return rc;

    // Calculate the offset within the page for this record
    char *pageData = page->data;
    int offset = record->id.slot * recordSize;

    // Copy the new record data into the page at the calculated offset
    memcpy(pageData + offset, record->data, recordSize);

    // Mark page as dirty and unpin it
    rc = markDirty(bm, page);
    if (rc != RC_OK) return rc;

    rc = unpinPage(bm, page);
    if (rc != RC_OK) return rc;

    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    TableMgmtData *tb_data = (TableMgmtData *)rel->mgmtData;
    
    // Retrieve information from the table management structure
    BM_PageHandle *page = tb_data->pageHandle;
    BM_BufferPool *bm = tb_data->bufferManager;
    int recordSize = tb_data->recordSize;

    // Pin the page containing the record
    RC rc = pinPage(bm, page, id.page);
    if (rc != RC_OK) return rc;

    // Calculate the offset within the page for this record
    char *pageData = page->data;
    int offset = id.slot * recordSize;

    // Copy the record data from the page into the provided record structure
    memcpy(record->data, pageData + offset, recordSize);

    // Set the RID in the record to match the requested RID
    record->id = id;

    // Unpin the page after reading the data
    rc = unpinPage(bm, page);
    if (rc != RC_OK) return rc;

    return RC_OK;
}

// SCANS
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    // Set up the scan handle
    scan->rel = rel;

    // Allocate memory for scan management data
    ScanMgmtData *scanData = (ScanMgmtData *)malloc(sizeof(ScanMgmtData));
    if (scanData == NULL) return -99;

    // Initialize scan data fields
    scanData->currentRecord.page = 1;     // Start at the first page
    scanData->currentRecord.slot = 0;     // Start at the first slot in the page
    scanData->condition = cond;    // Set the condition for filtering records

    // Attach the scan data to the scan handle
    scan->mgmtData = scanData;

    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
    RM_TableData *rel = scan->rel;
    ScanMgmtData *scanData = (ScanMgmtData *)scan->mgmtData;
    BM_PageHandle page;

    BM_BufferPool *bm = rel->mgmtData->bufferManager;
    int recordSize = rel->mgmtData->recordSize;
    int recordsPerPage = rel->mgmtData->numRecords;
    int totalNumRecords = rel->mgmtData->totalNumRecords;

    // Calculate the starting position based on currentPage and currentSlot
    int startRecordIndex = (scanData->currentRecord.page * recordsPerPage) + scanData->currentRecord.slot;

    // Iterate over all records starting from the current scan position
    for (int recordIndex = startRecordIndex; recordIndex < totalNumRecords; recordIndex++) {
        int pageNum = recordIndex / recordsPerPage;
        int slot = recordIndex % recordsPerPage;

        // Pin the page where the current record is located
        RC rc = pinPage(bm, &page, pageNum);
        if (rc != RC_OK) return rc;

        // Calculate the offset within the page
        char *pageData = page.data;
        int offset = slot * recordSize;

        // Copy the record's data from the page into the Record structure
        memcpy(record->data, pageData + offset, recordSize);
        record->id.page = pageNum;
        record->id.slot = slot;

        // Evaluate the condition, if specified
        if (scanData->condition != NULL) {
            Value *result = NULL;
            rc = evalExpr(record, rel->schema, scanData->condition, &result);
            if (rc != RC_OK) {
                unpinPage(bm, &page);
                return rc;
            }

            bool matches = (result->v.boolV == TRUE);
            free(result);

            if (!matches) {
                unpinPage(bm, &page);
                continue; // Skip this record if it doesn't match
            }
        }

        // Update scan position for the next call
        scanData->currentRecord.page = pageNum;
        scanData->currentRecord.slot = slot + 1;

        // Unpin the page and return the matching record
        unpinPage(bm, &page);
        return RC_OK;
    }

    // No more matching records
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan) {
    // Free the scan management data
    if (scan->mgmtData != NULL) {
        free(scan->mgmtData);
        scan->mgmtData = NULL;
    }
    
    return RC_OK;
}

// DEALING WITH SCHEMAS
int getRecordSize (Schema *schema)
{
	int size = 0; // offset set to zero
	
	// Iterating through all the attributes in the schema
	for(int i = 0; i < schema->numAttr; i++)
	{
		switch(schema->dataTypes[i])    // Adding the size of attribute depending on type
		{
            // Case switch
			case DT_STRING:
				size = size + schema->typeLength[i];
				break;
			case DT_INT:
				size = size + sizeof(int);
				break;
			case DT_FLOAT:
				size = size + sizeof(float);
				break;
			case DT_BOOL:
				size = size + sizeof(bool);
				break;
		}
	}
	return ++size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    // Allocate memory for the schema
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    if (!schema) return NULL;

    // Pointers to arrays of following attributes
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema; 
}

RC freeSchema (Schema *schema){
    for (int i = 0; i < schema->numAttr; i++) if (schema->attrNames[i] != NULL) free(schema->attrNames[i]);
    free(schema->attrNames);
    free(schema);
    return RC_OK;
}

// DEALING WITH RECORDS AND ATTRIBUTE VALUES
RC createRecord (Record **record, Schema *schema){

    Record *rec = (Record*)malloc(sizeof(Record));      // Creating a new record
    int recordSize = getRecordSize(schema);     // Get size of new record
    rec -> data = (char*)malloc(recordSize);    // Allocating record size
	memset(rec -> data, 0, recordSize);
	*record = rec;

    return RC_OK;
    
}
RC freeRecord (Record *record){
    free(record->data);
    free(record);
    return RC_OK;  
}

//this method is used to get an attribute
//this method is used to get the value of one attribute in a record
//int attrNum is the the position os this attribute, and the result stores in value
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;
	int i = 0;

	// Allocating space for value data where attribute values will be stored
	Value *val = (Value*)malloc(sizeof(Value));

	offset += (attrNum +1); // Adding empty spaces to the offset

	for(i = 0; i < attrNum; i++) // Adding the length of attributes to offset
	{
		switch( schema -> dataTypes[i])
        {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema -> typeLength[i];
                break;
            default:
                break;
        }
	}

	char* result;
	// Getting values according to the dataTypes in schema
	switch(schema -> dataTypes[attrNum])
	{
		case DT_INT:
		  val -> dt = DT_INT;
		  result = (char*)malloc(sizeof(int) +1);
		  memcpy(result, record -> data + offset,sizeof(int));
		  result[sizeof(int) +1] = '\0';
		  val -> v.intV = atoi(result);
		  result = NULL;
		  break;
		case DT_FLOAT:
		  val -> dt = DT_FLOAT;	 
		  memcpy(&(val->v.floatV),record->data+ offset,sizeof(float));
		  break;
		case DT_BOOL:
		  val -> dt = DT_BOOL;
		   memcpy(&(val->v.boolV),record -> data + offset,sizeof(bool));
		  break;
		case DT_STRING:
		  val -> dt = DT_STRING;
		  int size = schema -> typeLength[i];
          char *result = malloc(size+1);
          strncpy(result, record->data+ offset, size); 
          result[size]='\0';
          val->v.stringV = result;
		  break;
		
	}
	*value = val;
	return RC_OK;
	
}


//this method is used to set an attribute
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) 
{
    int offset = 0;

    //calculate the offset of this attribute
    offset += (attrNum+1);
    for(int i = 0; i < attrNum; i++)
	{
		switch( schema -> dataTypes[i])
        {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema -> typeLength[i];
                break;
            default:
                break;
        }
	}

 
    char *pos = record -> data;
    pos += offset;

    // Writing data to the record according to the datatype
    switch(value->dt)   {

        case DT_INT:
            sprintf(pos,"%d",value->v.intV);
            break;

        case DT_FLOAT:
            sprintf(pos,"%f",value->v.floatV);     
            break;

        case DT_BOOL:
            sprintf(pos,"%i",value->v.boolV);
            break;

        case DT_STRING: 
            sprintf(pos,"%s",value->v.stringV);
            break;
        }

    return RC_OK;
}