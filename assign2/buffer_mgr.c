#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <time.h>  

// Structure to hold buffer pool management data
typedef struct BufferPoolMgmtData {
    BM_PageHandle *pageFrames;   // Array of page frames to store pages in memory
    int numReadIO;   // Number of Reads fow the statistics
	int numWriteIO;   // Number of Writes fow the statistics
    int fifoIndex;
    int lastUsed;
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
        mgmtData->pageFrames[i].data = malloc(PAGE_SIZE); // No pages are pinned initially
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

     for (int i = 0; i < bm->numPages; i++)
        free(mgmtData->pageFrames[i].data);


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
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    // First, let's loop through the buffer...
    for (int i = 0; i < bm->numPages; i++) {
        // and if we found the desired page
        if (mgmtData->pageFrames[i].pageNum == page->pageNum) {
            // Mark it as DIRTY
            mgmtData->pageFrames[i].dirtyFlag = true;
            return RC_OK;
        }
    }
    // Error if page isn't found
    return RC_READ_NON_EXISTING_PAGE;
}

// Unpin a page from the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    
    // Loop for the page we want to unpin
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == page->pageNum) {
            // If pinned...
            if (mgmtData->pageFrames[i].fixCount > 0) {
                // Decrease fixCount
                mgmtData->pageFrames[i].fixCount--;
                return RC_OK;
            }
        }
    }
    // Error if page not found / already unpinned
    return RC_READ_NON_EXISTING_PAGE;
}


// Force a specific page to be written to disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // Loop again for the page we are looking for...
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == page->pageNum) {
            // If dirty flag is true...
            if (mgmtData->pageFrames[i].dirtyFlag) {
                // Writing the page back to disk
                mgmtData->numWriteIO++; // Incrementing write IO count...
                mgmtData->pageFrames[i].dirtyFlag = false; // and setting dirty flag to false
                return RC_OK;
            }
        }
    }
    // Error in case page not found
    return RC_READ_NON_EXISTING_PAGE; // If page is not found
}

void writePageToDisk(BM_BufferPool *const bm, BM_PageHandle *const page) {
    SM_FileHandle fh;

    // Open the page file
    if (openPageFile(bm->pageFile, &fh) != RC_OK) {
        printf("Error: Could not open page file.\n");
        return;
    }

    // Write the page to disk using the appropriate storage manager function
    if (writeBlock(page->pageNum, &fh, page->data) != RC_OK) {
        printf("Error: Could not write page to disk.\n");
    }

    // Close the page file
    closePageFile(&fh);
}

void readPageFromDisk(const char *pageFile, PageNumber pageNum, char *data) {
    SM_FileHandle fh;

    // Open the page file
    if (openPageFile(pageFile, &fh) != RC_OK) {
        return;
    }

    // Ensure the page exists
    if (pageNum >= fh.totalNumPages) {
        closePageFile(&fh);
        return;
    }

    // Read the page from disk into the data buffer
    if (readBlock(pageNum, &fh, data) != RC_OK) {
        printf("Error: Could not read page from disk.\n");
    }

    // Close the page file
    closePageFile(&fh);
}

int findFIFOFrame(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmt = (BufferPoolMgmtData *) bm->mgmtData;

    // Use fifoIndex to keep track of which frame is the next to be replaced
    int frameIndex = mgmt->fifoIndex;

    // We need to ensure that the frame selected has a fixCount of 0 (i.e., it is not pinned)
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmt->pageFrames[frameIndex].fixCount == 0) {
            // Move fifoIndex to the next frame for the next replacement
            mgmt->fifoIndex = (mgmt->fifoIndex + 1) % bm->numPages;
            return frameIndex;
        }
        // Increment frameIndex in a circular fashion
        frameIndex = (frameIndex + 1) % bm->numPages;
    }
    // If no unpinned frame is found, return an error (though this situation shouldn't happen)
    return -1;  // Error: No unpinned frame available
}


int findLRUFrame(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmt = (BufferPoolMgmtData *) bm->mgmtData;
    int lruFrame = -1;
    long oldestTime = 100000000;  // Initialize with the maximum possible value

    // Iterate through all frames to find the one with the oldest lastUsed time and fixCount 0
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmt->pageFrames[i].fixCount == 0 && mgmt->pageFrames[i].lastUsed < oldestTime) {
            oldestTime = mgmt->pageFrames[i].lastUsed;
            lruFrame = i;
        }
    }
    return lruFrame; // Return the index of the least recently used frame
}




void updateLRU(BM_BufferPool *const bm, int frameIndex) {
    BufferPoolMgmtData *mgmt = (BufferPoolMgmtData *) bm->mgmtData;

    static long counter = 0;
    mgmt->pageFrames[frameIndex].lastUsed = ++counter;

}

// Find a page to replace based on the replacement strategy
int findPageToReplace(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // Simple LRU implementation: replace the page that was least recently used
    int lruPage = mgmtData->lastUsed;
    return lruPage;
}

// Pin a page into the buffer pool
// Pin a page in the buffer pool
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // Check if the page is already in memory (search in page frames)
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == pageNum) {
            // Page found in memory, update fix count and return
            mgmtData->pageFrames[i].fixCount++;
            page->pageNum = pageNum;
            page->data = mgmtData->pageFrames[i].data;
            return RC_OK;
        }
    }

    // If the page is not found, read from disk and replace if needed
    int freeFrame = -1;
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == NO_PAGE) {
            freeFrame = i;
            break;
        }
    }

    if (freeFrame == -1) {
        // No free frame, use LRU or other replacement strategy
        freeFrame = findPageToReplace(bm);  // Implement this function to handle LRU, FIFO, etc.
    }

    // Load the page from disk into the buffer
    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    ensureCapacity(pageNum + 1, &fh);
    readBlock(pageNum, &fh, mgmtData->pageFrames[freeFrame].data);
    closePageFile(&fh);

    // Update frame details
    mgmtData->pageFrames[freeFrame].pageNum = pageNum;
    mgmtData->pageFrames[freeFrame].fixCount = 1;
    mgmtData->pageFrames[freeFrame].dirtyFlag = false;

    // Return the page to the page handle
    page->pageNum = pageNum;
    page->data = mgmtData->pageFrames[freeFrame].data;

    // Update LRU or FIFO index (for replacement strategy)
    updateLRU(bm, freeFrame);  // Implement LRU update function

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

