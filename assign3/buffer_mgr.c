#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define MAX_ALLOWED_PAGES 1000  // or a suitable upper limit

typedef struct Page
{
	SM_PageHandle data; // Data of page
	PageNumber pageNum; // ID for Page
	int dirtyFlag; // Modified?
	int fixCount; // Clients using page
    int hitNum; // For LRU Algorithm
} PageFrame;

int numReadIO = 0;
int numWriteIO = 0;
int hit;

void pinPageFIFO(BM_BufferPool *const bm, PageFrame *page) {
    // Get the start of the buffer pool's page frame array
    PageFrame *pageFrame = (PageFrame *) bm->mgmtData;

    // Calculate the initial index based on FIFO logic
    int next = numReadIO % bm->numPages;

    // Iterate through the buffer pool pages to find a replacable frame
    for (int i = 0; i < bm->numPages; i++) {
        // Check if the current page frame can be replaced
        if (pageFrame[next].fixCount == 0) {
            // If the page is dirty, write it back to disk
            if (pageFrame[next].dirtyFlag == 1) {
                SM_FileHandle fh;
                if (openPageFile(bm->pageFile, &fh) == RC_OK) {
                    writeBlock(pageFrame[next].pageNum, &fh, pageFrame[next].data);
                    numWriteIO++;
                }
            }

            // Replace the page frame with the new page data
            pageFrame[next].data = page->data;
            pageFrame[next].pageNum = page->pageNum;
            pageFrame[next].dirtyFlag = page->dirtyFlag;
            pageFrame[next].fixCount = page->fixCount;
            return; // Exit the function once the page is pinned
        }

        // Move to the next frame using FIFO logic
        next = (next + 1) % bm->numPages;
    }
}

void pinPageLRU(BM_BufferPool *const bm, PageFrame *page) {
    PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
    int leastHitIndex, leastHitNum;

    // Find the least recently used (LRU) page that can be replaced
    for (int i = 0; i < bm->numPages; i++) {
        if (pageFrame[i].fixCount == 0 && pageFrame[i].hitNum < leastHitNum) {
            leastHitIndex = i;
            leastHitNum = pageFrame[i].hitNum;
        }
    }

    // Write back the page if it is dirty
    if (pageFrame[leastHitIndex].dirtyFlag) {
        SM_FileHandle fh;
        if (openPageFile(bm->pageFile, &fh) == RC_OK) {
            numWriteIO++;
        }
    }

    // Update the selected page frame with the new page data
    pageFrame[leastHitIndex].data = page->data;
    pageFrame[leastHitIndex].pageNum = page->pageNum;
    pageFrame[leastHitIndex].dirtyFlag = page->dirtyFlag;
    pageFrame[leastHitIndex].fixCount = page->fixCount;
    pageFrame[leastHitIndex].hitNum = page->hitNum;
}

// Initialize the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
    // Allocate memory for the page frames
    PageFrame *page = (PageFrame *)malloc(sizeof(PageFrame) * numPages);

    // Initialize each page frame in the buffer pool
    for (int i = 0; i < numPages; i++) {
        page[i].pageNum = NO_PAGE;        // Mark the frame as unused
        page[i].dirtyFlag = false;       // Initially, pages are clean
        page[i].fixCount = 0;            // No pages are pinned initially
        page[i].hitNum = 0;              // Reset usage count
        page[i].data = malloc(PAGE_SIZE); // Allocate memory for page data
    }

    // Initialize the buffer pool structure
    bm->pageFile = strdup(pageFileName); // Duplicate the page file name
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = page;

    // Reset read/write IO counters
    numReadIO = 0;
    numWriteIO = 0;

    return RC_OK;
}

// Shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm) {
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	forceFlushPool(bm); // Write all pages marked dirty back to disk

	for(int i = 0; i < bm->numPages; i++) {
		// If fixCount is not 0, then it hasn't been written back
		if(pageFrame[i].fixCount != 0)
			return RC_FILE_NOT_FOUND;
	}
    // Free space allocated by page
	free(pageFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	// Loop through buffer and flush dirty pages
	for(int i = 0; i < bm->numPages; i++) {
		if(pageFrame[i].fixCount == 0 && pageFrame[i].dirtyFlag) {
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
			pageFrame[i].dirtyFlag = false; // Mark the page as not dirty
			numWriteIO++;
		}
	}	
	return RC_OK;
}



// Mark a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    // First, let's loop through the buffer...
    for (int i = 0; i < bm->numPages; i++) {
        // and if we found the desired page
        if (pageFrame[i].pageNum == page->pageNum) {
            // Mark it as DIRTY
            pageFrame[i].dirtyFlag = true;
            return RC_OK;
        }
    }
}

// Unpin a page from the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Loop for the page we want to unpin
    for (int i = 0; i < bm->numPages; i++) {
        if (pageFrame[i].pageNum == page->pageNum) {
            // If pinned...
            if (pageFrame[i].fixCount > 0) {
                // Decrease fixCount
                pageFrame[i].fixCount--;
                break;
            }
        }
    }

    return RC_OK;
}


// Force a specific page to be written to disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Loop again for the page we are looking for...
    for (int i = 0; i < bm->numPages; i++) {
        if (pageFrame[i].pageNum == page->pageNum) {
            SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
            // If dirty flag is true...
            // Writing the page back to disk
            numWriteIO++; // Incrementing write IO count...
            pageFrame[i].dirtyFlag = false; // and setting dirty flag to false
        }
    }
    return RC_OK;
}


// Pin a page into the buffer pool
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	// Buffer pool is empty and thus first page will be pinned
	if(pageFrame[0].pageNum == -1)  {
		// Reading page from disk. Initializing page frame's content in the buffer pool
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		pageFrame[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, pageFrame[0].data);
		pageFrame[0].pageNum = pageNum;
		pageFrame[0].fixCount++;
		numReadIO = hit = 0;
		pageFrame[0].hitNum = hit;	
		page->pageNum = pageNum;
		page->data = pageFrame[0].data;
		
		return RC_OK;		
	}
	else {	
		bool bufferFull;
		for(int i = 0; i < bm->numPages; i++)   {
			if(pageFrame[i].pageNum != -1)  {	
				// Is page in memory?
				if(pageFrame[i].pageNum == pageNum) {
					// If so, increase fixCount
					pageFrame[i].fixCount++;
					bufferFull = false;
					hit++; // Incrementing hit for LRU
					if(bm->strategy == RS_LRU)
						pageFrame[i].hitNum = hit;
					page->pageNum = pageNum;
					page->data = pageFrame[i].data;
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				pageFrame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, pageFrame[i].data);
				pageFrame[i].pageNum = pageNum;
				pageFrame[i].fixCount = 1;
				numReadIO++;	
				hit++; // Incrementing hit for LRU
				if(bm->strategy == RS_LRU)
					pageFrame[i].hitNum = hit;				
				page->pageNum = pageNum;
				page->data = pageFrame[i].data;
				bufferFull = false;
				break;
			}
		}
		
		// If buffer is full then we must find a page to replace using the given strategy
		if(bufferFull == true)  {
			// Newly created page to store data
			PageFrame *newPage = (PageFrame *) malloc(sizeof(PageFrame));		

			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			newPage->data = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, newPage->data);
			newPage->pageNum = pageNum;
			newPage->dirtyFlag = 0;		
			newPage->fixCount = 1;
			numReadIO++;
			hit++;
			if(bm->strategy == RS_LRU)
				newPage->hitNum = hit;				
			page->pageNum = pageNum;
			page->data = newPage->data;			

			// Use the strategy function that was selected
			switch(bm->strategy)  {			
				case RS_FIFO:
					pinPageFIFO(bm, newPage);
					break;
				case RS_LRU:
					pinPageLRU(bm, newPage);
					break;
				default:
					printf("\nAlgorithm Not Implemented\n");
					break;
			}
						
		}		
		return RC_OK;
	}	
}


// Get the frame contents of the buffer pool
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    if (bm->numPages <= 0 || bm->numPages > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }

    PageNumber *frameContents = (PageNumber *) malloc(bm->numPages * sizeof(PageNumber));

    for (int i = 0; i < bm->numPages; i++) {
        frameContents[i] = pageFrame[i].pageNum;
    }

    return frameContents;
}


// Get dirty flags for the buffer pool
bool *getDirtyFlags(BM_BufferPool *const bm) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    
    if (bm->numPages <= 0 || bm->numPages > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }
    bool *dirtyFlags = (bool *) malloc(bm->numPages * sizeof(bool));

    for (int i = 0; i < bm->numPages; i++){
        if (pageFrame[i].dirtyFlag){
            dirtyFlags[i] = true; // Write true if the frame is dirty
        } else {
            dirtyFlags[i] = false; // Write false if the frame is not dirty
        }
    }
    return dirtyFlags;
}


// Get fix counts for the buffer pool
int *getFixCounts(BM_BufferPool *const bm) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    if (bm->numPages <= 0 || bm->numPages > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }
    int *fixCounts = (int *) malloc(bm->numPages * sizeof(int));

    for (int i = 0; i < bm->numPages; i++)
        fixCounts[i] = pageFrame[i].fixCount;

    return fixCounts;
}


// Get the number of read I/O operations
int getNumReadIO(BM_BufferPool *const bm) {
    return (numReadIO + 1);
}


// Get the number of write I/O operations
int getNumWriteIO(BM_BufferPool *const bm) {
    return numWriteIO;
}