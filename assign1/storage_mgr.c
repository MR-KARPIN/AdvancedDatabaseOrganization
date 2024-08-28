#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_SIZE 4096  // Define the page size as 4096 bytes

/* MANIPULATING PAGE FILES */
/***************************/

// Initialize any necessary variables or data structures
void initStorageManager(void) {
    // TODO 
}

// Create a new page file with one page of PAGE_SIZE bytes
RC createPageFile(char *fileName) {
    // TODO 
}

// Open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // TODO 
}

// Close the page file
RC closePageFile(SM_FileHandle *fHandle) {
    // TODO 
}

// Delete the page file from disk
RC destroyPageFile(char *fileName) {
    // TODO 

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
