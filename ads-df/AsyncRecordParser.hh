#ifndef __ASYNCRECORDPARSER_H__
#define __ASYNCRECORDPARSER_H__

#include <stdint.h>

#include <stdexcept>
#include <iostream>

#include <boost/serialization/serialization.hpp>

#include "RecordBuffer.hh"
#include "RecordType.hh"
#include "RuntimeOperator.hh"
#include "FileSystem.hh"
#include "FileService.hh"
#include "RecordParser.hh"
#include "StreamBufferBlock.hh"

const char * __attribute__ ((noinline)) mymemchr(const char * begin,
						 const char * end,
						 int32_t c,
						 std::size_t * toFind);


// Coroutine-ish parser classes

class ParserState
{
private:
  int32_t mResult;
public:
  static ParserState exhausted()
  {
    return ParserState(1);
  }
  static ParserState success()
  {
    return ParserState(0);
  }
  static ParserState error(int32_t errCode)
  {
    return ParserState(errCode);
  }
  ParserState()
    :
    mResult(1)
  {
  }
  ParserState(int32_t result)
    :
    mResult(result)
  {
  }
  bool isError() const 
  {
    return mResult < 0;
  }
  bool isExhausted() const 
  {
    return mResult == 1;
  }
  bool isSuccess() const 
  {
    return mResult == 0;
  }
  int32_t result() const
  {
    return mResult;
  }
};

class AsyncDataBlock
{
protected:
  // Mark for saving data in block
  uint8_t * mCurrentBlockMark;
  // Start of the data block
  uint8_t * mCurrentBlockStart;
  // End of valid data in the block
  uint8_t * mCurrentBlockEnd;
  // Current position within block
  uint8_t * mCurrentBlockPtr;
public:
  AsyncDataBlock();
  AsyncDataBlock(uint8_t * start, uint8_t * end);
  void rebind(uint8_t * start, uint8_t * end);
  uint8_t * start()
  {
    return mCurrentBlockStart;
  }
  uint8_t * begin()
  {
    return mCurrentBlockPtr;
  }
  uint8_t * end()
  {
    return mCurrentBlockEnd;
  }
  const uint8_t * begin() const
  {
    return mCurrentBlockPtr;
  }
  const uint8_t * end() const
  {
    return mCurrentBlockEnd;
  }
  bool isEmpty() const
  {
    return begin() == end();
  }
  void consume(std::size_t sz) 
  {
    mCurrentBlockPtr += sz;
  }
  void consumeAll() 
  {
    mCurrentBlockPtr = mCurrentBlockEnd;
  }
};

class ImporterDelegate
{
private:
  typedef ParserState (*ImporterStub)(void *, AsyncDataBlock&, RecordBuffer);
  void * mObject;
  ImporterStub mMethod;

  template <class _T, ParserState (_T::*_TMethod)(AsyncDataBlock&, RecordBuffer)>
  static ParserState Stub(void * obj, AsyncDataBlock& source, RecordBuffer target)
  {
    _T* p = static_cast<_T*>(obj);
    return (p->*_TMethod)(source, target);
  }
public:
  ImporterDelegate()
    :
    mObject(NULL),
    mMethod(NULL)
  {
  }
  
  template <class _T, ParserState (_T::*_TMethod)(AsyncDataBlock&, RecordBuffer)>
  static ImporterDelegate fromMethod(_T * obj)
  {
    ImporterDelegate d;
    d.mObject = obj;
    d.mMethod = &Stub<_T, _TMethod>;
    return d;
  }

  ParserState operator()(AsyncDataBlock& targetOffset, RecordBuffer size)
  {
    return (*mMethod)(mObject, targetOffset, size);
  }
};

class ImporterSpec
{
private:
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
  }
protected:
  ImporterSpec()
  {
  }
public:
  virtual ~ImporterSpec() {}
  virtual ImporterDelegate makeObject(void * buf) const =0;
  virtual std::size_t objectSize() const =0;
  virtual std::size_t objectAlignment() const =0;
  virtual bool isConsumeOnly() const { return false; }
  static void createDefaultImport(const RecordType * recordType,
				  const RecordType * baseRecordType,
				  char fieldDelim,
				  char recordDelim,
				  std::vector<ImporterSpec*>& importers);  
};

class ConsumeTerminatedString
{
private:
  enum State { START, READ };
  State mState;
  uint8_t mTerm;

  bool importInternal(AsyncDataBlock& source, RecordBuffer target) 
  {
    uint8_t * start = source.begin();
    uint8_t * found = (uint8_t *) memchr((char *) source.begin(), mTerm,
					 (std::size_t) (source.end()-source.begin()));
    if(found) {
      source.consume(std::size_t(found - start) + 1);
      return true;
    } else {
      source.consumeAll();
      return false;
    }
  }

public:
  ConsumeTerminatedString(uint8_t term);

  ParserState import(AsyncDataBlock& source, RecordBuffer target);
};

class ConsumeTerminatedStringSpec : public ImporterSpec
{
private:
  uint8_t mTerm;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
  }
  ConsumeTerminatedStringSpec()
    :
    mTerm(0)
  {
  }
public:
  ConsumeTerminatedStringSpec(uint8_t term)
    :
    mTerm(term)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    ConsumeTerminatedString * obj = new (buf) ConsumeTerminatedString(mTerm);
    return ImporterDelegate::fromMethod<ConsumeTerminatedString, 
      &ConsumeTerminatedString::import>(obj);
  }
  std::size_t objectSize() const
  {
    return sizeof(ConsumeTerminatedString);
  }
  std::size_t objectAlignment() const
  {
    return boost::alignment_of<ConsumeTerminatedString>::value;
  }
  bool isConsumeOnly() const { return true; }
};

class ImportDecimalInt32
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ_FIRST, READ_DIGITS };
  State mState;
  int32_t mValue;
  uint8_t mTerm;
  bool mNeg;

  bool importInternal(AsyncDataBlock& source, RecordBuffer target) 
  {
    int32_t val = mValue;
    uint8_t * start = source.begin();
    uint8_t * end = source.end();
    for(uint8_t * s = start; s != end; ++s) {
      if (*s > '9' || *s < '0')  {
	// TODO: Right now assuming and not validating a single delimiter character
	// TODO: Protect against overflow	
	mTargetOffset.setInt32(mNeg ? -val : val, target);
	source.consume(std::size_t(s - start));
	mValue = 0;
	mNeg = false;
	return true;
      }
      val = val * 10 + (*s - '0');
    }
    mValue = val;
    source.consumeAll();
    return false;
  }

public:
  ImportDecimalInt32(const FieldAddress& targetOffset,
		     uint8_t term);

  ParserState import(AsyncDataBlock& source, RecordBuffer target);
};

class ImportDecimalInt32Spec : public ImporterSpec
{
private:
  FieldAddress mTargetOffset;
  uint8_t mTerm;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
  }
  ImportDecimalInt32Spec()
    :
    mTerm(0)
  {
  }
public:
  ImportDecimalInt32Spec(const FieldAddress & targetOffset,
			 uint8_t term)
    :
    mTargetOffset(targetOffset),
    mTerm(term)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    ImportDecimalInt32 * obj = new (buf) ImportDecimalInt32(mTargetOffset, mTerm);
    return ImporterDelegate::fromMethod<ImportDecimalInt32, &ImportDecimalInt32::import>(obj);
  }

  std::size_t objectSize() const
  {
    return sizeof(ImportDecimalInt32);
  }
  std::size_t objectAlignment() const
  {
    return boost::alignment_of<ImportDecimalInt32>::value;
  }
};

class ImportDouble
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ };
  State mState;
  std::vector<uint8_t> * mLocal;
  uint8_t mTerm;

public:
  ImportDouble(const FieldAddress& targetOffset,
		     uint8_t term);

  ParserState import(AsyncDataBlock& source, RecordBuffer target);
};

class ImportDoubleSpec : public ImporterSpec
{
private:
  FieldAddress mTargetOffset;
  uint8_t mTerm;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
  }
  ImportDoubleSpec()
    :
    mTerm(0)
  {
  }
public:
  ImportDoubleSpec(const FieldAddress & targetOffset,
			 uint8_t term)
    :
    mTargetOffset(targetOffset),
    mTerm(term)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    ImportDouble * obj = new (buf) ImportDouble(mTargetOffset, mTerm);
    return ImporterDelegate::fromMethod<ImportDouble, &ImportDouble::import>(obj);
  }

  std::size_t objectSize() const
  {
    return sizeof(ImportDouble);
  }
  std::size_t objectAlignment() const
  {
    return boost::alignment_of<ImportDouble>::value;
  }
};

class ImportFixedLengthString
{
private:
  FieldAddress mTargetOffset;
  enum State { START, READ };
  State mState;
  int32_t mSize;
  // For slow path (partial reads); where in the target
  // are we.
  int32_t mRead;
public:
  ImportFixedLengthString(const FieldAddress& targetOffset,
			  int32_t size);

  ParserState import(AsyncDataBlock& source, RecordBuffer target);
};

class ImportFixedLengthStringSpec : public ImporterSpec
{
private:
  FieldAddress mTargetOffset;
  int32_t mSize;
  uint8_t mTerm;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(ImporterSpec);
    ar & BOOST_SERIALIZATION_NVP(mTargetOffset);
    ar & BOOST_SERIALIZATION_NVP(mSize);
    ar & BOOST_SERIALIZATION_NVP(mTerm);
  }
  ImportFixedLengthStringSpec()
    :
    mTerm(0)
  {
  }
public:
  ImportFixedLengthStringSpec(const FieldAddress & targetOffset,
			      int32_t sz,
			      uint8_t term)
    :
    mTargetOffset(targetOffset),
    mSize(sz),
    mTerm(term)
  {
  }

  ImporterDelegate makeObject(void * buf) const
  {
    ImportFixedLengthString * obj = new (buf) ImportFixedLengthString(mTargetOffset, mSize);
    return ImporterDelegate::fromMethod<ImportFixedLengthString, &ImportFixedLengthString::import>(obj);
  }

  std::size_t objectSize() const
  {
    return sizeof(ImportFixedLengthString);
  }
  std::size_t objectAlignment() const
  {
    return boost::alignment_of<ImportFixedLengthString>::value;
  }
};

class CustomImport
{
private:
  FieldAddress mAkidOffset;
  FieldAddress mCreDateOffset;
  FieldAddress mCoopIdOffset;
  enum State { START, SKIP_TWO_READ, SKIP_24_READ, SKIP_LAST_READ,
	       READ_AKID, READ_AKID_TERM,
	       READ_CRE_DATE, READ_CRE_DATE_TERM,
	       READ_FIRST, READ_DIGITS };
  State mState;
  // Loop counter
  int32_t mField;
  // For importing int32
  int32_t mValue;
  // For slow path (partial reads) fixed length; where in the target
  // are we.
  int32_t mRead;
  bool mNeg;

  bool importInternal(AsyncDataBlock& source, RecordBuffer target) 
  {
    int32_t val = mValue;
    uint8_t * start = source.begin();
    uint8_t * end = source.end();
    for(uint8_t * s = start; s != end; ++s) {
      if (*s > '9' || *s < '0')  {
	// TODO: Right now assuming and not validating a single delimiter character
	// TODO: Protect against overflow	
	mCoopIdOffset.setInt32(mNeg ? -val : val, target);
	source.consume(std::size_t(s - start));
	mValue = 0;
	mNeg = false;
	return true;
      }
      val = val * 10 + (*s - '0');
    }
    mValue = val;
    source.consumeAll();
    return false;
  }

  bool importInternalSSE2(AsyncDataBlock& source, 
			  RecordBuffer target,
			  char term) 
  {
    uint8_t * start = source.begin();
    uint8_t * found = (uint8_t *) memchr(source.begin(), term,
    					 (std::size_t) (source.end() - source.begin()));
    if(found) {
      source.consume(std::size_t(found - start) + 1);
      return true;
    } else {
      source.consumeAll();
      return false;
    }
  }
public:
  CustomImport(const RecordType * ty);
  CustomImport(const FieldAddress & akidOffset,
	       const FieldAddress & creDateOffset,
	       const FieldAddress & coopIdOffset);

  ParserState import(AsyncDataBlock& source, RecordBuffer target);
};

// template<class _OpType>
// class GenericAsyncParserOperator : public RuntimeOperator
// {
// private:
//   typedef _OpType operator_type;

//   enum State { START, OPEN_FILE, READ_NEW_RECORD, WRITE, WRITE_EOF };
//   State mState;

//   // The list of files from which I read; retrieved
//   // by calling my operator type.
//   std::vector<boost::shared_ptr<FileChunk> > mFiles;
//   // The file am I working on
//   std::vector<boost::shared_ptr<FileChunk> >::const_iterator mFileIt;
//   // The importer objects themselves
//   uint8_t * mImporterObjects;
//   // Importer delegates
//   std::vector<ImporterDelegate> mImporters;
//   // The field am I currently importing
//   std::vector<ImporterDelegate>::iterator mIt;
//   // Input buffer for the file.
//   AsyncDataBlock mInputBuffer;
//   // File Service for async IO
//   FileService * mFileService;
//   // File handle that is currently open
//   FileServiceFile * mFileHandle;
//   // Record buffer I am importing into
//   RecordBuffer mOutput;
//   // Records imported
//   uint64_t mRecordsImported;

//   const operator_type & getLogParserType()
//   {
//     return *static_cast<const operator_type *>(&getOperatorType());
//   }

// public:
//   GenericAsyncParserOperator(RuntimeOperator::Services& services, 
// 			     const RuntimeOperatorType& opType)
//     :
//     RuntimeOperator(services, opType),
//     mImporterObjects(NULL),
//     mFileService(NULL),
//     mFileHandle(NULL),
//     mRecordsImported(0)
//   {
//     // std::size_t sz = getLogParserType().mCommentLine.size();
//     // if (sz > std::numeric_limits<int32_t>::max()) {
//     //   throw std::runtime_error("Comment line too large");
//     // }
//     // mCommentLineSz = (int32_t) sz;

//     // Convert the import specs into objects and delegates
//     // for fast invocation (that avoids virtual functions/vtable
//     // lookup).
    
//     // We want all importer state to be "local"
//     std::size_t sz = 0;
//     for(typename _OpType::field_importer_const_iterator it = getLogParserType().mImporters.begin();
// 	it != getLogParserType().mImporters.end(); ++it) {
//       std::size_t tmp = (*it)->objectSize();      
//       // TODO: Fix alignment info
//       sz += ((tmp + 7)>>3)<<3;
//     }
//     mImporterObjects = new uint8_t [sz];
//     sz = 0;
//     for(typename _OpType::field_importer_const_iterator it = getLogParserType().mImporters.begin();
// 	it != getLogParserType().mImporters.end(); ++it) {
//       mImporters.push_back((*it)->makeObject(mImporterObjects + sz));
//       std::size_t tmp = (*it)->objectSize();      
//       // TODO: Fix alignment info
//       sz += ((tmp + 7)>>3)<<3;
//     }
//   }

//   ~GenericAsyncParserOperator()
//   {
//     if (mFileService) {
//       FileService::release(mFileService);
//     }
//   }

//   /**
//    * intialize.
//    */
//   void start()
//   {
//     mFiles.clear();
//     // What file(s) am I parsing?
//     typename _OpType::chunk_strategy_type chunkFiles;
//     // Expand file name globbing, then get files for this
//     // partition.
//     chunkFiles.expand(getLogParserType().mFileInput, getNumPartitions());
//     chunkFiles.getFilesForPartition(getPartition(), mFiles);
//     mState = START;
//     mRecordsImported = 0;
//     mFileService = FileService::get();
    
//     onEvent(NULL);
//   }

//   void onEvent(RuntimePort * port)
//   {
//     switch(mState) {
//     case START:
//       for(mFileIt = mFiles.begin();
// 	  mFileIt != mFiles.end();
// 	  ++mFileIt) {
// 	BOOST_ASSERT(mFileHandle == NULL);
// 	// Allocate a new input buffer for the file in question.
// 	if ((*mFileIt)->getBegin() > 0) {
// 	  throw std::runtime_error("Not implemented yet");
// 	  // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
// 	  // 				64*1024,
// 	  // 				(*mFileIt)->getBegin()-1,
// 	  // 				(*mFileIt)->getEnd());
// 	  // RecordBuffer nullRecord;
// 	  // getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
// 	} else {
// 	  // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
// 	  // 				64*1024,
// 	  // 				(*mFileIt)->getBegin(),
// 	  // 				(*mFileIt)->getEnd());
// 	  mFileService->requestOpenForRead((*mFileIt)->getFilename().c_str(), 
// 					   (*mFileIt)->getBegin(),
// 					   (*mFileIt)->getEnd(),
// 					   getCompletionPorts()[0]);
// 	  requestCompletion(0);
// 	  mState = OPEN_FILE;
// 	  return;
// 	case OPEN_FILE:
// 	  {
// 	    RecordBuffer buf;
// 	    read(port, buf);
// 	    mFileHandle = mFileService->getOpenResponse(buf);
// 	  }
// 	  if (getLogParserType().mSkipHeader) {
// 	    throw std::runtime_error("Async skip header not implemented yet");
// 	    //   RecordBuffer nullRecord;
// 	    //   getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
// 	  }
// 	}
	
// 	// Read all of the record in the file.
// 	while(true) {
// 	  // If empty read a block; it is OK to exhaust a file
// 	  // here but not while in the middle of a record, so 
// 	  // we make a separate read attempt here.
// 	  if (mInputBuffer.isEmpty()) {
// 	    mFileService->requestRead(mFileHandle, mInputBuffer.start(), 
// 				      64*1024, getCompletionPorts()[0]);
// 	    requestCompletion(0);
// 	    mState = READ_NEW_RECORD;
// 	    return;
// 	  case READ_NEW_RECORD:
// 	    {
// 	      RecordBuffer buf;
// 	      read(port, buf);
// 	      int32_t bytesRead = mFileService->getReadBytes(buf);
// 	      if (0 == bytesRead) {
// 		break;
// 	      }
// 	      mInputBuffer.rebind(mInputBuffer.start(),
// 				  mInputBuffer.start() + bytesRead);
// 	    }
// 	  }
// 	  // This is our actual record.
// 	  mOutput = getLogParserType().mMalloc.malloc();
// 	  for(mIt = mImporters.begin();
// 	      mIt != mImporters.end();
// 	      ++mIt) {
// 	    while(true) {
// 	      ParserState ret = mIt->import(mInputBuffer, mOutput);
// 	      if (ret.isSuccess()) {
// 		// Successful parse
// 		break;
// 	      } else if (ret.isExhausted()) {
// 		throw std::runtime_error("Read on exhaust not yet implemented");
// 	      } else {
// 		// Bad record
// 		throw std::runtime_error("Parse Error in record");
// 	      }
// 	    }
// 	  }
// 	  // Done cause we had good record
// 	  mRecordsImported += 1;
// 	  requestWrite(0);
// 	  mState = WRITE;
// 	  return;
// 	case WRITE:
// 	  write(port, mOutput, false);
// 	  // TODO: Handle truncated files and empty files
// 	// } else {
// 	//     // Done with this file
// 	//     getLogParserType().mFree.free(mOutput);
// 	//     mOutput = RecordBuffer();
// 	//     break;
// 	//   }
// 	}
// 	// Either EOF or parse failure.  In either
// 	// case done with this file.
// 	// TODO: FIXME
// 	// delete mFileHandle;
// 	mFileHandle = NULL;
//       }
//       // Done with the last file so output EOS.
//       requestWrite(0);
//       mState = WRITE_EOF;
//       return;
//     case WRITE_EOF:
//       write(port, RecordBuffer::create(), true);
//       return;
//     }
//   }

//   void shutdown()
//   {
//   }
// };

// template <class _ChunkStrategy = ExplicitChunkStrategy >
// class GenericAsyncParserOperatorType : public RuntimeOperatorType
// {
//   // Don't really know how to do friends between templates.
// public:
//   typedef _ChunkStrategy chunk_strategy_type;
//   typedef typename _ChunkStrategy::file_input file_input_type;
//   typedef ImporterSpec* field_importer_type;
//   typedef std::vector<ImporterSpec*>::const_iterator field_importer_const_iterator;

//   // What file(s) am I parsing?
//   file_input_type mFileInput;
//   // Importer instructions
//   std::vector<field_importer_type> mImporters;
//   // Importer to read to end of line (when skipping over non "r" log lines).
//   field_importer_type mSkipImporter;
//   // Create new records
//   RecordTypeMalloc mMalloc;
//   RecordTypeFree mFree;
//   // What am I importing
//   const RecordType * mRecordType;
//   // Is there a header to skip?
//   bool mSkipHeader;
//   // Skip lines starting with this.
//   std::string mCommentLine;
//   // Serialization
//   friend class boost::serialization::access;
//   template <class Archive>
//   void serialize(Archive & ar, const unsigned int version)
//   {
//     ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
//     ar & BOOST_SERIALIZATION_NVP(mFileInput);
//     ar & BOOST_SERIALIZATION_NVP(mImporters);
//     ar & BOOST_SERIALIZATION_NVP(mSkipImporter);
//     ar & BOOST_SERIALIZATION_NVP(mMalloc);
//     ar & BOOST_SERIALIZATION_NVP(mFree);
//     ar & BOOST_SERIALIZATION_NVP(mSkipHeader);
//     ar & BOOST_SERIALIZATION_NVP(mCommentLine);
//   }
//   GenericAsyncParserOperatorType()
//   {
//   }  

// public:
//   GenericAsyncParserOperatorType(const typename _ChunkStrategy::file_input& file,
// 			    char fieldSeparator,
// 			    char recordSeparator,
// 			    const RecordType * recordType,
// 			    const RecordType * baseRecordType=NULL,
// 			    const char * commentLine = "")
//     :
//     RuntimeOperatorType("GenericAsyncParserOperatorType"),
//     mFileInput(file),
//     mSkipImporter(NULL),
//     mRecordType(recordType),
//     mSkipHeader(false),
//     mCommentLine(commentLine)
//   {
//     mMalloc = mRecordType->getMalloc();
//     mFree = mRecordType->getFree();

//     // Records have tab delimited fields and newline delimited records
//     ImporterSpec::createDefaultImport(recordType, 
// 				      baseRecordType ? baseRecordType : recordType,
// 				      fieldSeparator, 
// 				      recordSeparator, mImporters);
    
//     // To skip a line we just parse to newline and discard.
//     // We need this when syncing to the middle of a file.
//     mSkipImporter = new ConsumeTerminatedStringSpec(recordSeparator);
//   }

//   ~GenericAsyncParserOperatorType()
//   {
//     for(field_importer_const_iterator it = mImporters.begin(); 
// 	it != mImporters.end(); ++it) {
//       delete *it;
//     }
//     delete mSkipImporter;
//   }

//   const RecordType * getOutputType()
//   {
//     return mRecordType;
//   }

//   void setSkipHeader(bool value) 
//   {
//     mSkipHeader = value;
//   }

//   int32_t numServiceCompletionPorts() const 
//   {
//     return 1;
//   }

//   RuntimeOperator * create(RuntimeOperator::Services & services) const;
// };

class LogicalAsyncParser : public LogicalOperator
{
private:
  const RecordType * mFormat;
  std::string mStringFormat;
  std::string mMode;
  bool mSkipHeader;
  char mFieldSeparator;
  char mRecordSeparator;
  std::string mCommentLine;
public:
  LogicalAsyncParser();
  ~LogicalAsyncParser();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class GenericAsyncParserOperatorType : public RuntimeOperatorType
{
  friend class GenericAsyncParserOperator;
private:
  typedef ImporterSpec* field_importer_type;
  typedef std::vector<ImporterSpec*>::const_iterator field_importer_const_iterator;
  // Importer instructions
  std::vector<field_importer_type> mImporters;
  // Importer to read to end of line (when skipping over non "r" log lines).
  field_importer_type mSkipImporter;
  // Access to stream buffer
  StreamBufferBlock mStreamBlock;
  // Create new records
  RecordTypeMalloc mStreamMalloc;
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  // What am I importing
  const RecordType * mRecordType;
  // Is there a header to skip?
  bool mSkipHeader;
  // Skip lines starting with this.
  std::string mCommentLine;
  // Hack perf testing
  FieldAddress mAkidOffset;
  FieldAddress mCreDateOffset;
  FieldAddress mCoopIdOffset;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mImporters);
    ar & BOOST_SERIALIZATION_NVP(mSkipImporter);
    ar & BOOST_SERIALIZATION_NVP(mStreamBlock);
    ar & BOOST_SERIALIZATION_NVP(mStreamMalloc);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mSkipHeader);
    ar & BOOST_SERIALIZATION_NVP(mCommentLine);
  }
  GenericAsyncParserOperatorType()
  {
  }  

public:
  GenericAsyncParserOperatorType(char fieldSeparator,
				 char recordSeparator,
				 const RecordType * inputStreamType,
				 const RecordType * recordType,
				 const RecordType * baseRecordType=NULL,
				 const char * commentLine = "");

  ~GenericAsyncParserOperatorType();

  void setSkipHeader(bool value) 
  {
    mSkipHeader = value;
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

class LogicalBlockRead : public LogicalOperator
{
private:
  const RecordType * mStreamBlock;
  std::string mFile;
  int32_t mBufferCapacity;
  bool mBucketed;
public:
  LogicalBlockRead();
  ~LogicalBlockRead();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

template<class _OpType>
class GenericAsyncReadOperator : public RuntimeOperator
{
private:
  typedef _OpType operator_type;

  enum State { START, OPEN_FILE, READ_BLOCK, WRITE_BLOCK, WRITE_EOF };
  State mState;

  // The list of files from which I read; retrieved
  // by calling my operator type.
  std::vector<boost::shared_ptr<FileChunk> > mFiles;
  // The file am I working on
  std::vector<boost::shared_ptr<FileChunk> >::const_iterator mFileIt;
  // File Service for async IO
  FileService * mFileService;
  // File handle that is currently open
  FileServiceFile * mFileHandle;
  // Record buffer I am importing into
  RecordBuffer mOutput;

  const operator_type & getLogParserType()
  {
    return *static_cast<const operator_type *>(&getOperatorType());
  }

public:
  GenericAsyncReadOperator(RuntimeOperator::Services& services, 
			     const RuntimeOperatorType& opType)
    :
    RuntimeOperator(services, opType),
    mFileService(NULL),
    mFileHandle(NULL)
  {
  }

  ~GenericAsyncReadOperator()
  {
    if (mFileService) {
      FileService::release(mFileService);
    }
  }

  /**
   * intialize.
   */
  void start()
  {
    mFiles.clear();
    // What file(s) am I parsing?
    typename _OpType::chunk_strategy_type chunkFiles;
    // Expand file name globbing, then get files for this
    // partition.
    chunkFiles.expand(getLogParserType().mFileInput, getNumPartitions());
    chunkFiles.getFilesForPartition(getPartition(), mFiles);
    mState = START;
    mFileService = FileService::get();
    
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      for(mFileIt = mFiles.begin();
	  mFileIt != mFiles.end();
	  ++mFileIt) {
	BOOST_ASSERT(mFileHandle == NULL);
	// Allocate a new input buffer for the file in question.
	if ((*mFileIt)->getBegin() > 0) {
	  throw std::runtime_error("Not implemented yet");
	  // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
	  // 				64*1024,
	  // 				(*mFileIt)->getBegin()-1,
	  // 				(*mFileIt)->getEnd());
	  // RecordBuffer nullRecord;
	  // getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
	} else {
	  // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
	  // 				64*1024,
	  // 				(*mFileIt)->getBegin(),
	  // 				(*mFileIt)->getEnd());
	  mFileService->requestOpenForRead((*mFileIt)->getFilename().c_str(), 
					   (*mFileIt)->getBegin(),
					   (*mFileIt)->getEnd(),
					   getCompletionPorts()[0]);
	  requestCompletion(0);
	  mState = OPEN_FILE;
	  return;
	case OPEN_FILE:
	  {
	    RecordBuffer buf;
	    read(port, buf);
	    mFileHandle = mFileService->getOpenResponse(buf);
	  }
	}
	
	// Read all of the record in the file.
	while(true) {
	  mOutput = getLogParserType().mMalloc.malloc();
	  // If empty read a block; it is OK to exhaust a file
	  // here but not while in the middle of a record, so 
	  // we make a separate read attempt here.
	  mFileService->requestRead(mFileHandle, 
				    (uint8_t *) getLogParserType().mBufferAddress.getCharPtr(mOutput), 
				    getLogParserType().mBufferCapacity, 
				    getCompletionPorts()[0]);
	  requestCompletion(0);
	  mState = READ_BLOCK;
	  return;
	case READ_BLOCK:
	  {
	    RecordBuffer buf;
	    read(port, buf);
	    int32_t bytesRead = mFileService->getReadBytes(buf);
	    if (0 == bytesRead) {
	      getLogParserType().mFree.free(mOutput);
	      mOutput = RecordBuffer();
	      // TODO: Send a message indicating file boundary potentially
	      // Decompressors may need this to resync state (or maybe not).
	      break;
	    }
	    getLogParserType().mBufferSize.setInt32(bytesRead, mOutput);
	  }
	  // Done cause we had good record
	  // Flush always and write through to
	  // avoid local queue; these are big chunks of memory
	  requestWriteThrough(0);
	  mState = WRITE_BLOCK;
	  return;
	case WRITE_BLOCK:
	  write(port, mOutput, true);
	}
	// Either EOF or parse failure.  In either
	// case done with this file.
	// TODO: FIXME
	// delete mFileHandle;
	mFileHandle = NULL;
      }
      // Done with the last file so output EOS.
      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(), true);
      return;
    }
  }

  void shutdown()
  {
  }
};

template <class _ChunkStrategy = ExplicitChunkStrategy >
class GenericAsyncReadOperatorType : public RuntimeOperatorType
{
  // Don't really know how to do friends between templates.
public:
  typedef _ChunkStrategy chunk_strategy_type;
  typedef typename _ChunkStrategy::file_input file_input_type;
  typedef ImporterSpec* field_importer_type;
  typedef std::vector<ImporterSpec*>::const_iterator field_importer_const_iterator;

  // What file(s) am I parsing?
  file_input_type mFileInput;
  // Create new records
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  // Accessors into buffer (size INTEGER, buffer CHAR(N))
  FieldAddress mBufferSize;
  FieldAddress mBufferAddress;
  // Size of buffer to allocate
  int32_t mBufferCapacity;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFileInput);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mBufferSize);
    ar & BOOST_SERIALIZATION_NVP(mBufferAddress);
    ar & BOOST_SERIALIZATION_NVP(mBufferCapacity);
  }
  GenericAsyncReadOperatorType()
  {
  }  

public:
  GenericAsyncReadOperatorType(const typename _ChunkStrategy::file_input& file,
			    const RecordType * streamBlockType)
    :
    RuntimeOperatorType("GenericAsyncReadOperatorType"),
    mFileInput(file),
    mMalloc(streamBlockType->getMalloc()),
    mFree(streamBlockType->getFree()),
    mBufferSize(streamBlockType->getFieldAddress("size")),
    mBufferAddress(streamBlockType->getFieldAddress("buffer")),
    mBufferCapacity(streamBlockType->getMember("buffer").GetType()->GetSize())
  {
  }

  ~GenericAsyncReadOperatorType()
  {
  }

  int32_t numServiceCompletionPorts() const 
  {
    return 1;
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

template <class _ChunkStrategy>
RuntimeOperator * GenericAsyncReadOperatorType<_ChunkStrategy>::create(RuntimeOperator::Services & services) const
{
  return new GenericAsyncReadOperator<GenericAsyncReadOperatorType<_ChunkStrategy> >(services, *this);
}

#endif
