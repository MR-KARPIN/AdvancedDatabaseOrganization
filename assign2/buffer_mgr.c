#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define MAX_ALLOWED_PAGES 1000  // or a suitable upper limit

typedef struct Page
{
	SM_PageHandle data; // Actual data of the page
	PageNumber pageNum; // An identification integer given to each page
	int dirtyFlag; // Used to indicate whether the contents of the page has been modified by the client
	int fixCount; // Used to indicate the number of clients using that page at a given instance
    int hitNum; // Used by LRU algorithm to get the least recently used page
} PageFrame;

int bufferSize = 0;
int numReadIO = 0;
int numWriteIO = 0;
int hit;

void pinPageFIFO(BM_BufferPool *const bm, PageFrame *page)
{
    //printf("FIFO Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int next = numReadIO % bufferSize;

	// Interating through all the page frames in the buffer pool
	for(int i = 0; i < bufferSize; i++)
	{
		if(pageFrame[next].fixCount == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[next].dirtyFlag == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[next].pageNum, &fh, pageFrame[next].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				numWriteIO++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[next].data = page->data;
			pageFrame[next].pageNum = page->pageNum;
			pageFrame[next].dirtyFlag = page->dirtyFlag;
			pageFrame[next].fixCount = page->fixCount;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			next++;
			next = (next % bufferSize == 0) ? 0 : next;
		}
	}

}

void pinPageLRU(BM_BufferPool *const bm, PageFrame *page) {
    PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int i, leastHitIndex, leastHitNum;

	// Interating through all the page frames in the buffer pool.
	for(i = 0; i < bufferSize; i++)
	{
		// Finding page frame whose fixCount = 0 i.e. no client is using that page frame.
		if(pageFrame[i].fixCount == 0)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNum;
			break;
		}
	}	

	// Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
	for(i = leastHitIndex + 1; i < bufferSize; i++)
	{
		if(pageFrame[i].hitNum < leastHitNum)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNum;
		}
	}

	// If page in memory has been modified (dirtyFlag == true), then write page to disk
	if(pageFrame[leastHitIndex].dirtyFlag == true)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHitIndex].pageNum, &fh, pageFrame[leastHitIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		numWriteIO++;
	}
	
	// Setting page frame's content to new page's content
	pageFrame[leastHitIndex].data = page->data;
	pageFrame[leastHitIndex].pageNum = page->pageNum;
	pageFrame[leastHitIndex].dirtyFlag = page->dirtyFlag;
	pageFrame[leastHitIndex].fixCount = page->fixCount;
	pageFrame[leastHitIndex].hitNum = page->hitNum;

}

// Initialize the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {
   
    // Allocate memory for the page file name and copy it
    bm->pageFile = (char *) (char *)pageFileName;
    
    // Initialize the number of pages and the replacement strategy
    bm->numPages = numPages;
    bm->strategy = strategy;

    PageFrame *page = malloc(sizeof(PageFrame) * numPages);

    // Initialize management data for the buffer pool
    bufferSize = numPages;

    for (int i = 0; i < bufferSize; i++) {
        page[i].pageNum = NO_PAGE; // Initialize all frames as empty
        page[i].dirtyFlag = false; // Pages are clean initially
        page[i].fixCount = 0; // No pages are pinned initially
        page[i].hitNum = 0;
        page[i].data = malloc(PAGE_SIZE);
    }

    bm->mgmtData = page;

    // Initialize statistics for read/write IO
    numReadIO = numWriteIO = 0;

    return RC_OK;
}

// Shut down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    printf("Before forceFlushPool\n");
    printPoolContent(bm);
    
    // Flush all the pages before deleting the BufferPool
    forceFlushPool(bm);
    printf("After forceFlushPool\n");
    printPoolContent(bm);

    free(pageFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	// Loop through buffer and flush dirty pages
	for(int i = 0; i < bufferSize; i++)
	{
		if(pageFrame[i].fixCount == 0 && pageFrame[i].dirtyFlag)
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
			// Mark the page as not dirty
			pageFrame[i].dirtyFlag = false;
			numWriteIO++;
		}
	}	
	return RC_OK;
}



// Mark a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    // First, let's loop through the buffer...
    for (int i = 0; i < bufferSize; i++) {
        // and if we found the desired page
        if (pageFrame[i].pageNum == page->pageNum) {
            // Mark it as DIRTY
            pageFrame[i].dirtyFlag = true;
            return RC_OK;
        }
    }
    // Error if page isn't found
    return RC_READ_NON_EXISTING_PAGE;
}

// Unpin a page from the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Loop for the page we want to unpin
    for (int i = 0; i < bufferSize; i++) {
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
    for (int i = 0; i < bufferSize; i++) {
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
	if(pageFrame[0].pageNum == -1)
	{
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
	else
	{	
		int i;
		bool isBufferFull = true;
		
		for(i = 0; i < bufferSize; i++)
		{
			if(pageFrame[i].pageNum != -1)
			{	
				// Checking if page is in memory
				if(pageFrame[i].pageNum == pageNum)
				{
					// Increasing fixCount i.e. now there is one more client accessing this page
					pageFrame[i].fixCount++;
					isBufferFull = false;
					hit++; // Incrementing hit - used by LRU algorithm to determine the least recently used page

					if(bm->strategy == RS_LRU)
						// LRU uses the value of hit to determine the least recently used page	
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
				hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

				if(bm->strategy == RS_LRU)
					// LRU algorithm uses the value of hit to determine the least recently used page
					pageFrame[i].hitNum = hit;				
				else if(bm->strategy == RS_CLOCK)
					// hitNum = 1 to indicate that this was the last page frame examined (added to the buffer pool)
					pageFrame[i].hitNum = 1;
						
				page->pageNum = pageNum;
				page->data = pageFrame[i].data;
				
				isBufferFull = false;
				break;
			}
		}
		
		// If isBufferFull = true, then it means that the buffer is full and we must replace an existing page using page replacement strategy
		if(isBufferFull == true)
		{
			// Create a new page to store data read from the file.
			PageFrame *newPage = (PageFrame *) malloc(sizeof(PageFrame));		
			
			// Reading page from disk and initializing page frame's content in the buffer pool
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
				// LRU algorithm uses the value of hit to determine the least recently used page
				newPage->hitNum = hit;				
			page->pageNum = pageNum;
			page->data = newPage->data;			

			// Call appropriate algorithm's function depending on the page replacement strategy selected (passed through parameters)
			switch(bm->strategy)
			{			
				case RS_FIFO: // Using FIFO algorithm
					pinPageFIFO(bm, newPage);
					break;
				
				case RS_LRU: // Using LRU algorithm
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
    if (bufferSize <= 0 || bufferSize > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }

    PageNumber *frameContents = (PageNumber *) malloc(bm->numPages * sizeof(PageNumber));

    for (int i = 0; i < bufferSize; i++) {
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

    for (int i = 0; i < bufferSize; i++){
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
    if (bufferSize <= 0 || bufferSize > MAX_ALLOWED_PAGES) {
        fprintf(stderr, "Error: Invalid number of pages (%d)\n", bm->numPages);
        return NULL;
    }
    int *fixCounts = (int *) malloc(bm->numPages * sizeof(int));

    for (int i = 0; i < bufferSize; i++)
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