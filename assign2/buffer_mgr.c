#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

// Structure to hold buffer pool management data
typedef struct BufferPoolMgmtData {
    BM_PageHandle *pageFrames;   // Array of page frames to store pages in memory
    int numReadIO;   // Number of Reads fow the statistics
	int numWriteIO;   // Number of Writes fow the statistics
} BufferPoolMgmtData;

// Initialize the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
   
    // Allocate memory for the page file name and copy it
    bm->pageFile = (char *) malloc(strlen(pageFileName) + 1);
    strcpy(bm->pageFile, pageFileName);
    
    // Initialize the number of pages and the replacement strategy
    bm->numPages = numPages;
    bm->strategy = strategy;

    // Initialize management data for the buffer pool
    bm->mgmtData = malloc(sizeof(BufferPoolMgmtData)); 
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // Allocate memory for page frames in the buffer pool
    mgmtData->pageFrames = malloc(numPages * sizeof(BM_PageHandle)); 
    for (int i = 0; i < numPages; i++) {
        mgmtData->pageFrames[i].pageNum = NO_PAGE; // Initialize all frames as empty
        mgmtData->pageFrames[i].dirtyFlag = false; // Pages are clean initially
        mgmtData->pageFrames[i].fixCount = 0; // No pages are pinned initially
    }

    // Initialize statistics for read/write IO
    mgmtData->numReadIO = 0;
    mgmtData->numWriteIO = 0;

    return RC_OK;
}

// Shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    //Flush all the pages before deleting the BufferPool
    forceFlushPool(bm);

    // Free up memory for page frames and other management data
    free(mgmtData->pageFrames);
    free(bm->mgmtData);
    free(bm->pageFile);

    return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // Loop through all page frames and flush dirty pages
    for (int i = 0; i < bm->numPages; i++)
        if (mgmtData->pageFrames[i].fixCount == 0 && mgmtData->pageFrames[i].dirtyFlag) 
            forcePage(bm, &mgmtData->pageFrames[i]); // Write the page back to disk
    
    return RC_OK;
}


// Mark a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;
}

// Unpin a page from the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;
}


// Force a specific page to be written to disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    return RC_OK;
}


// Pin a page into the buffer pool
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, 
           const PageNumber pageNum) {
    return RC_OK;
}


// Get the frame contents of the buffer pool
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    PageNumber *frameContents = (PageNumber *) malloc(bm->numPages * sizeof(PageNumber));

    for (int i = 0; i < bm->numPages; i++) {
        frameContents[i] = mgmtData->pageFrames[i].pageNum;
    }

    return frameContents;
}


// Get dirty flags for the buffer pool
bool *getDirtyFlags(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    bool *dirtyFlags = (bool *) malloc(bm->numPages * sizeof(bool));

    for (int i = 0; i < bm->numPages; i++){
        if (mgmtData->pageFrames[i].dirtyFlag){
            dirtyFlags[i] = true; // Write true if the frame is dirty
        } else {
            dirtyFlags[i] = false; // Write false if the frame is not dirty
        }
    }
    return dirtyFlags;
}


// Get fix counts for the buffer pool
int *getFixCounts(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    int *fixCounts = (int *) malloc(bm->numPages * sizeof(int));

    for (int i = 0; i < bm->numPages; i++)
        fixCounts[i] = mgmtData->pageFrames[i].fixCount;

    return fixCounts;
}


// Get the number of read I/O operations
int getNumReadIO(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    return mgmtData->numReadIO;
}


// Get the number of write I/O operations
int getNumWriteIO(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    return mgmtData->numWriteIO;
}
