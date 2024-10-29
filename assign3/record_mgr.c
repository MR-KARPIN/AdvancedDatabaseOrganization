#include "record_mgr.h"

// TABLE AND MANAGER
RC initRecordManager (void *mgmtData){
    
}
RC shutdownRecordManager (){
    
}
RC createTable (char *name, Schema *schema){
    
}
RC openTable (RM_TableData *rel, char *name){
    
}
RC closeTable (RM_TableData *rel){
    
}
RC deleteTable (char *name){
    
}
int getNumTuples (RM_TableData *rel){
    
}

// HANDLING RECORDS IN A TABLE
RC insertRecord (RM_TableData *rel, Record *record){
    
}
RC deleteRecord (RM_TableData *rel, RID id){
    
}
RC updateRecord (RM_TableData *rel, Record *record){
    
}
RC getRecord (RM_TableData *rel, RID id, Record *record){
    
}

// SCANS
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    
}
RC next (RM_ScanHandle *scan, Record *record){
    
}
RC closeScan (RM_ScanHandle *scan){
    
}

// DEALING WITH SCHEMAS
int getRecordSize (Schema *schema){
    
}
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    
}
RC freeSchema (Schema *schema){
    
}

// DEALING WITH RECORDS AND ATTRIBUTE VALUES
RC createRecord (Record **record, Schema *schema){
    
}
RC freeRecord (Record *record){
    
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    
}

