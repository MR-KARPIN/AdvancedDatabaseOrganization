#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

#define MAX_ALLOWED_PAGES 1000  // or a suitable upper limit



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
        mgmtData->pageFrames[i].data = malloc(PAGE_SIZE);
    }

    mgmtData->LRU = malloc(numPages * sizeof(PageNumber));
    if (mgmtData->LRU == NULL) fprintf(stderr, "Memory allocation for LRU failed\n");
    
    


    // Initialize statistics for read/write IO
    mgmtData->numReadIO = 0;
    mgmtData->numWriteIO = 0;
    mgmtData->next = 0;

    return RC_OK;
}

// Shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    printf("Before forceFlushPool\n");
    printPoolContent(bm);
    
    // Flush all the pages before deleting the BufferPool
    forceFlushPool(bm);
    printf("After forceFlushPool\n");
    printPoolContent(bm);

    // Free memory for page frames
    for (int i = 0; i < bm->numPages; i++)
        free(mgmtData->pageFrames[i].data); // Free individual page data

    printf("Freeing page frames\n");
    free(mgmtData->pageFrames); // Free page frames

    printf("Freeing management data\n");

    printf("Freeing page file\n");
    free(bm->pageFile); // Free the page file string

    printPoolContent(bm);

    free(bm->mgmtData); // Free management data
    printf("Buffer pool shutdown completed\n");
    
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


// Pin a page into the buffer pool
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, 
           const PageNumber pageNum) {
    switch(bm->strategy)
	{
	case RS_FIFO:
		pinPageFIFO(bm, page, pageNum);
		break;

	case RS_LRU:
		pinPageLRU(bm, page, pageNum);
    default:
        break;
	}
	return RC_OK;
}

RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // First things first... check if page is in the buffer...
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == pageNum) {
            // If it's in the buffer then we increase fix count
            mgmtData->pageFrames[i].fixCount++;
            page->pageNum = pageNum; // Update page handle
            page->data = mgmtData->pageFrames[i].data; // Point to data
            return RC_OK;
        }
    }

    // If page is not in buffer... then we find a empty frame to load it
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == NO_PAGE) {
            // Loading page from disk
            // Load the page into the empty frame
            mgmtData->pageFrames[i].pageNum = pageNum;
            // Fix count becomes 1
            mgmtData->pageFrames[i].fixCount = 1;
            // Now... setting the page
            page->pageNum = pageNum;
            // As well as data pointer
            page->data = mgmtData->pageFrames[i].data;

            mgmtData->numReadIO++; // Increment read IO count
            return RC_OK;
        }
    }

    while (true) {
        printf("\nIndex %i\n", mgmtData->next);
        
        // Check if the page at `next` is pinned
        if (mgmtData->pageFrames[mgmtData->next].fixCount == 0) {
            // If the page is dirty, write it to disk
            if (mgmtData->pageFrames[mgmtData->next].dirtyFlag) {
                forcePage(bm, &mgmtData->pageFrames[mgmtData->next]);
            }

            // Pin the new page in this frame (set fixCount, update page number, etc.)
            mgmtData->pageFrames[mgmtData->next].pageNum = pageNum;
            mgmtData->pageFrames[mgmtData->next].fixCount = 1;
            mgmtData->pageFrames[mgmtData->next].dirtyFlag = false;

            // Set the `page` handle to this frame
            page->pageNum = pageNum;
            page->data = mgmtData->pageFrames[mgmtData->next].data;

            // Move to the next frame in FIFO order
            mgmtData->next = (mgmtData->next + 1) % bm->numPages;
            break;
        } else {
            // Move to the next frame if current one is pinned
            mgmtData->next = (mgmtData->next + 1) % bm->numPages;
        }
    }

    mgmtData->numReadIO++;
    return RC_OK;
}

RC pinPageLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;

    // First things first... check if page is in the buffer...
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == pageNum) {
            // If it's in the buffer then we increase fix count
            mgmtData->pageFrames[i].fixCount++;
            printf("\nPage read index %i \n", i);
            mgmtData->LRU[mgmtData->next] = i;
            mgmtData->next++;
            page->pageNum = pageNum; // Update page handle
            page->data = mgmtData->pageFrames[i].data; // Point to data
            return RC_OK;
        }
    }

    // If page is not in buffer... then we find a empty frame to load it
    for (int i = 0; i < bm->numPages; i++) {
        if (mgmtData->pageFrames[i].pageNum == NO_PAGE) {
            // Loading page from disk
            // Load the page into the empty frame
            mgmtData->pageFrames[i].pageNum = pageNum;
            // Fix count becomes 1
            mgmtData->pageFrames[i].fixCount = 1;
            // Now... setting the page
            page->pageNum = pageNum;
            // As well as data pointer
            page->data = mgmtData->pageFrames[i].data;

            mgmtData->numReadIO++; // Increment read IO count
            return RC_OK;
        }
    }

    while (true) {
        printf("\nIndex %i\n", mgmtData->LRU[0]);
        
        if (mgmtData->pageFrames[mgmtData->LRU[0]].dirtyFlag) {
            forcePage(bm, &mgmtData->pageFrames[mgmtData->LRU[0]]);
        }

            // Pin the new page in this frame (set fixCount, update page number, etc.)
            mgmtData->pageFrames[mgmtData->LRU[0]].pageNum = pageNum;
            mgmtData->pageFrames[mgmtData->LRU[0]].fixCount = 1;
            mgmtData->pageFrames[mgmtData->LRU[0]].dirtyFlag = false;

            // Set the `page` handle to this frame
            page->pageNum = pageNum;
            page->data = mgmtData->pageFrames[mgmtData->LRU[0]].data;

            int temp = mgmtData->LRU[0];
            for (int i = 0; i < bm->numPages - 1; i++) {
                mgmtData->LRU[i] = mgmtData->LRU[i + 1];
            }
            mgmtData->LRU[bm->numPages - 1] = temp;

            break;
        }

    mgmtData->numReadIO++;
    return RC_OK;
}

// Get the frame contents of the buffer pool
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    if (bm->numPages <= 0 || bm->numPages > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }

    PageNumber *frameContents = (PageNumber *) malloc(bm->numPages * sizeof(PageNumber));

    for (int i = 0; i < bm->numPages; i++) {
        frameContents[i] = mgmtData->pageFrames[i].pageNum;
    }

    return frameContents;
}


// Get dirty flags for the buffer pool
bool *getDirtyFlags(BM_BufferPool *const bm) {
    BufferPoolMgmtData *mgmtData = (BufferPoolMgmtData *) bm->mgmtData;
    
    if (bm->numPages <= 0 || bm->numPages > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }
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
    if (bm->numPages <= 0 || bm->numPages > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }
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