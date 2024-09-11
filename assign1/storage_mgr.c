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
    char *block = malloc(PAGE_SIZE * sizeof(char));         // Block of memory with PAGE_SIZE size
    memset(block, '\0', PAGE_SIZE);                         // Intialize block with 0 bytes
    fwrite(block, sizeof(char), PAGE_SIZE, filePointer);    // Write contents of the block to file

    // Clean up
    free(block);                                            // Free block to avoid memory leaks
    fclose(filePointer);                                    // Close connection
    return RC_OK;
}

// Open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {

    filePointer = fopen(fileName, "r+");    // Opening the file
    if (filePointer == NULL)                       // :(
        return RC_FILE_NOT_FOUND;
    
    // But if again successful...
    fseek(filePointer, 0, SEEK_END);    // filePointer will be pointed to the end of file

    // Declaring helpful variables for metadata
    int finalByte = ftell(filePointer); // Current position of the file pointer
    int numPages = (finalByte+1) / PAGE_SIZE;  // Number of pages is calculated using PAGE_SIZE

    // Let's set the metadata
    fHandle->fileName = fileName;
    fHandle->totalNumPages = numPages;
    fHandle->curPagePos = 0;

    // Set file pointer to the beginning of file
    rewind(filePointer);
    return RC_OK;
}

// Close the page file
RC closePageFile(SM_FileHandle *fHandle) {
	int fileClosed = fclose(filePointer);           // Return code to store value of fclose
	if (fileClosed != 0)                            // If not closed due to fclose failing...
		return RC_FILE_NOT_FOUND;           
    return RC_OK;                                   // File successfully closed
}

//Destroying Page file
RC destroyPageFile(char *fileName) {
	if (remove(fileName) != 0)                      // Same as method above, but testing remove instead
		return RC_FILE_NOT_FOUND;
	return RC_OK;
}

/* READING BLOCKS FROM DISC */
/****************************/

// Read a specific block from the file
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

// Get the current block position in the file
int getBlockPos(SM_FileHandle *fHandle) {
    // TODO 
}

// Read the first block from the file
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

// Read the previous block relative to the current page position
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

// Read the next block relative to the current page position
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

// Read the last block in the file
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

/* WRITING BLOCKS TO A PAGE FILE */
/*********************************/

// Write a block at a specific position
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

// Write the current block
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // TODO 
}

// Append an empty block at the end of the file
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    // TODO 
}

// Ensure that the file has a certain number of pages
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    // TODO 
}
