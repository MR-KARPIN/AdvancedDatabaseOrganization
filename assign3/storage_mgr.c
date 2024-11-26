#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_SIZE 4096  // Define the page size as 4096 bytes

/* MANIPULATING PAGE FILES */
/***************************/

FILE *filePointer;

// Initialize any necessary variables or data structures
void initStorageManager(void) {
    filePointer = NULL;
}

// Create a new page file with one page of PAGE_SIZE bytes
RC createPageFile(char *fileName) {

    filePointer = fopen(fileName, "w+"); // w+ used to open file for read/write
    if (filePointer == NULL)
        return RC_FILE_NOT_FOUND; // If unable to open file, filePointer will be NULL and return File not Found

    // But if successful...
	SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    if(fwrite(emptyPage, sizeof(char), PAGE_SIZE, filePointer) < PAGE_SIZE)
		printf("Failed \n");
	else
		printf("Succeeded");
	
    // Clean up
    fclose(filePointer);                                    // Close connection
    free(emptyPage);                                            // Free block to avoid memory leaks
    return RC_OK;
}

// Open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // Open the file in read+write mode
    FILE *filePointer = fopen(fileName, "r+");
    if (filePointer == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Set file handle properties
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;

    // Move to the end of the file to determine its size
    if (fseek(filePointer, 0, SEEK_END) != 0) {
        fclose(filePointer);
        return RC_FILE_NOT_FOUND;
    }

    // Get the file size using ftell
    long fileSize = ftell(filePointer);
    if (fileSize < 0) {
        fclose(filePointer);
        return RC_FILE_NOT_FOUND;
    }

    // Calculate the total number of pages
    fHandle->totalNumPages = fileSize / PAGE_SIZE;

    // Close the file
    fclose(filePointer);

    return RC_OK;
}


// Close the page file
RC closePageFile(SM_FileHandle *fHandle) {
    filePointer = fopen(fHandle->fileName, "r+");    // Opening the file
    int fileClosed = fclose(filePointer);           // Return code to store value of fclose
	if (fileClosed != 0)                            // If not closed due to fclose failing...
		return RC_FILE_NOT_FOUND;           
    return RC_OK;                                   // File successfully closed
}

//Destroying Page file
RC destroyPageFile(char *fileName) {
	// Opening file stream in read mode. 'r' mode creates an empty file for reading only.	
	filePointer = fopen(fileName, "r");
	
	if(filePointer == NULL)
		return RC_FILE_NOT_FOUND; 
	
	// Deleting the given filename so that it is no longer accessible.	
	remove(fileName);
	return RC_OK;
}

/* READING BLOCKS FROM DISC */
/****************************/

// Read a specific block from the file
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_READ_NON_EXISTING_PAGE;

	// Opening file stream in read mode. 'r' mode opens file for reading only.	
	filePointer = fopen(fHandle->fileName, "r");

	// Checking if file was successfully opened.
	if(filePointer == NULL)
		return RC_FILE_NOT_FOUND;
	
	// Setting the cursor(pointer) position of the file stream. Position is calculated by Page Number x Page Size
	// And the seek is success if fseek() return 0
	int isSeekSuccess = fseek(filePointer, (pageNum * PAGE_SIZE), SEEK_SET);
	if(isSeekSuccess == 0) {
		// We're reading the content and storing it in the location pointed out by memPage.
		if(fread(memPage, sizeof(char), PAGE_SIZE, filePointer) < PAGE_SIZE)
			return RC_FILE_NOT_FOUND;
	} else {
		return RC_READ_NON_EXISTING_PAGE; 
	}
    	
	// Setting the current page position to the cursor(pointer) position of the file stream
	fHandle->curPagePos = ftell(filePointer); 
	
	// Closing file stream so that all the buffers are flushed.     	
	fclose(filePointer);
	
    	return RC_OK;

}

// Get the current block position in the file
int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos; 
}

// Read the first block from the file
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL)                    // Ensure fHandle isn't NULL...
        return RC_FILE_NOT_FOUND;
    else if (fHandle->totalNumPages <= 0)   // Or empty
        return RC_READ_NON_EXISTING_PAGE;
    else {
        fseek(filePointer, 0, SEEK_SET);    // filePointer will point to first position on file
        RC firstBlock = fread(memPage, sizeof(char), PAGE_SIZE, filePointer);   // Getting first block
        fHandle->curPagePos = 0;        // Update the current position to the first
        if (firstBlock < 0 || firstBlock > PAGE_SIZE)   // Ensure right size, too
            return RC_READ_NON_EXISTING_PAGE;
        return RC_OK;       // :)
    }
}

// Read the previous block relative to the current page position
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if ((fHandle->curPagePos - 1) < 0)
        return RC_READ_NON_EXISTING_PAGE;
    else 
        return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

// Read the next block relative to the current page position
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if ((fHandle->curPagePos + 1) > fHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;
    else
        return readBlock(fHandle->curPagePos - 1, fHandle, memPage); 
}

// Read the last block in the file
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/* WRITING BLOCKS TO A PAGE FILE */
/*********************************/

// Write a block at a specific position
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_WRITE_FAILED;
	
	filePointer = fopen(fHandle->fileName, "r+");
	
	if(filePointer == NULL)
		return RC_FILE_NOT_FOUND;

	int startPosition = pageNum * PAGE_SIZE;

	if(pageNum == 0) { 
		fseek(filePointer, startPosition, SEEK_SET);	
		int i;
		for(i = 0; i < PAGE_SIZE; i++) 
		{
			if(feof(filePointer))
				 appendEmptyBlock(fHandle);	
			fputc(memPage[i], filePointer);
		}

		fHandle->curPagePos = ftell(filePointer); 

		fclose(filePointer);	
	} else {	
		fHandle->curPagePos = startPosition;
		fclose(filePointer);
		writeCurrentBlock(fHandle, memPage);
	}
	return RC_OK;
}

// Write the current block
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    filePointer = fopen(fHandle->fileName, "r+");

	// Checking if file was successfully opened.
	if(filePointer == NULL)
		return RC_FILE_NOT_FOUND;
	
	// Appending an empty block to make some space for the new content.
	appendEmptyBlock(fHandle);

	// Initiliazing file pointer
	fseek(filePointer, fHandle->curPagePos, SEEK_SET);
	
	// Writing memPage contents to the file.
	fwrite(memPage, sizeof(char), strlen(memPage), filePointer);
	
	// Setting the current page position to the cursor(pointer) position of the file stream
	fHandle->curPagePos = ftell(filePointer);
    	
	fclose(filePointer);
	return RC_OK;
}

// Append an empty block at the end of the file
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	
	int isSeekSuccess = fseek(filePointer, 0, SEEK_END);
	
	if( isSeekSuccess == 0 ) {
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, filePointer);
	} else {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}
	
	free(emptyBlock);
	
	fHandle->totalNumPages++;
	return RC_OK;
}

// Ensure that the file has a certain number of pages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    for (int i = numberOfPages-fHandle->totalNumPages; i>0; i--){
        RC response = appendEmptyBlock(fHandle);
        if (response != RC_OK)
            return response;
    }
    return RC_OK;
}
