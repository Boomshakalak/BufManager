/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */
/*
This file is the implemetation of the bufManager via clock algorithmn.
Zhongya Wang 9077060979
Chenxing Zhang 9071940093
*/
#include <memory>
#include <iostream>
#include "buffer.h"
#include "stdlib.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	std::uint32_t itrcnt = 0;
	BufMgr::advanceClock();
	while(itrcnt != numBufs){//flushing all dirty pages in frames
		if (bufDescTable[clockHand].dirty){
			File* temp=bufDescTable[clockHand].file;
			temp->writePage(bufPool[clockHand]);
		}
		BufMgr::advanceClock();
		itrcnt++;
	}//end of while
	delete [] bufPool;
	delete [] bufDescTable;
	delete hashTable;
}

void BufMgr::advanceClock()
{	
	clockHand=(clockHand+1)%numBufs; 
}

void BufMgr::allocBuf(FrameId & frame) 
{
	BufMgr::advanceClock();
	std::uint32_t itrcnt = 0;
	while(bufDescTable[clockHand].valid && (bufDescTable[clockHand].refbit ||
		bufDescTable[clockHand].pinCnt > 0)){//if valid = FALSE or (refbit = FALSE and pinCnt = 0), quit while
		if (bufDescTable[clockHand].refbit){
			bufDescTable[clockHand].refbit = false;
		}
		BufMgr::advanceClock();
		itrcnt ++;
		if (itrcnt == 2*numBufs - 1) //we need to check the clock 2 whole rounds to make sure if free frame exists
			throw BufferExceededException();
	}
	if(bufDescTable[clockHand].dirty ){//flush the page if it's dirty
		bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
	}
	if(bufDescTable[clockHand].valid)
		{
			hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
		}
	frame = clockHand;

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	std::uint32_t frameNo=0;
	try 
	{
		hashTable->lookup(file, pageNo, frameNo);
		bufDescTable[frameNo].refbit=true;
		bufDescTable[frameNo].pinCnt++;
		page=&bufPool[frameNo];
	}
	//if page is not found in the bufPool insert it into a new valid frame
	catch(HashNotFoundException e){
		BufMgr::allocBuf(frameNo);
		bufPool[frameNo]=file->readPage(pageNo);		
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		page=&bufPool[frameNo];
	}
	 
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{	
	std::uint32_t frameNo = 0;
	try
	{
		hashTable->lookup(file, pageNo, frameNo);
		if(bufDescTable[frameNo].pinCnt == 0)	//If no pin, throw exception
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		bufDescTable[frameNo].pinCnt--;
		if(dirty)	//if dirty, set desc.dirty == TRUE
			bufDescTable[frameNo].dirty=true;
	}
	catch(HashNotFoundException e){
		//do nothing;
	}

}

void BufMgr::flushFile(const File* file) 
{
	std::uint32_t itrcnt = 0;
	BufMgr::advanceClock();
	while(itrcnt != numBufs){//flushing all pages in dirty frames
		if (bufDescTable[clockHand].file == file){
			if (bufDescTable[clockHand].pinCnt != 0)
				throw PagePinnedException(file->filename(), bufDescTable[clockHand].pageNo, bufDescTable[clockHand].frameNo);
			if (!bufDescTable[clockHand].valid)
				throw BadBufferException(bufDescTable[clockHand].frameNo,bufDescTable[clockHand].dirty,bufDescTable[clockHand].valid
				, bufDescTable[clockHand].refbit);
			// File* b = const_cast<File*>(file); // remove the const property to be passed to the writepage method.
			if (bufDescTable[clockHand].dirty)
				const_cast<File*>(file)->writePage(bufPool[clockHand]);
			hashTable->remove(file, bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
		}
		BufMgr::advanceClock();
		itrcnt++;
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{	
	Page temp=file->allocatePage();
	pageNo=temp.page_number();
	std::uint32_t frameNo = 0;
	BufMgr::allocBuf(frameNo);
	bufPool[frameNo]=temp;
	hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);
	page = &bufPool[frameNo];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{	
	std::uint32_t frameNo = 0;
	try{
		hashTable->lookup(file, PageNo, frameNo);
		bufDescTable[frameNo].Clear();
		file->deletePage(PageNo);
		hashTable->remove(file,PageNo);
	}
	catch(HashNotFoundException e){
		file->deletePage(PageNo);
	}
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
