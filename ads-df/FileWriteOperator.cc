/**
 * Copyright (c) 2012, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <sys/types.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

#include "FileSystem.hh"
#include "FileWriteOperator.hh"
#include "RuntimeProcess.hh"

ZLibCompress::ZLibCompress()
  :
  mOutputStart(NULL),
  mOutputEnd(NULL)
{
  std::size_t outputBufferSz=64*1024;
  mOutputStart = new uint8_t [outputBufferSz];
  mOutputEnd = mOutputStart + outputBufferSz;
  mStream.zalloc = Z_NULL;
  mStream.zfree = Z_NULL;
  mStream.opaque = Z_NULL;
  // We use deflateInit2 so we can request a gzip header.  Other
  // params are set to defaults.
  int ret = ::deflateInit2(&mStream, 
			   Z_DEFAULT_COMPRESSION, 
			   Z_DEFLATED,
			   31, // Greater than 15 indicates gzip format
			   8,
			   Z_DEFAULT_STRATEGY);
  if (ret != Z_OK)
    throw std::runtime_error("Error initializing compression library");

  // Bind the buffer to the gzip stream
  mStream.avail_out = (mOutputEnd - mOutputStart);
  mStream.next_out = mOutputStart;
}

ZLibCompress::~ZLibCompress()
{
  deflateEnd(&mStream);
  delete [] mOutputStart;
}

void ZLibCompress::put(const uint8_t * bufStart, std::size_t len, bool isEOS)
{
  mFlush = isEOS ? Z_FINISH : Z_NO_FLUSH;
  mStream.avail_in = (int) len;
  mStream.next_in = const_cast<uint8_t *>(bufStart);
}

bool ZLibCompress::run()
{
  // Try to consume the input.
  int ret=Z_OK;
  do {
    ret = ::deflate(&mStream, mFlush);    
  } while(ret == Z_OK);
  return mStream.avail_in == 0;
}

void ZLibCompress::consumeOutput(uint8_t * & output, std::size_t & len)
{
  // How much data in the compressor state?
  output = mOutputStart;
  len = mStream.next_out - mOutputStart;
  // Reset for more output.
  mStream.avail_out = (mOutputEnd - mOutputStart);
  mStream.next_out = mOutputStart;  
}

class RuntimeWriteOperator : public RuntimeOperator
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  

private:
  enum State { START, READ };
  State mState;
  const RuntimeWriteOperatorType &  getWriteType()
  {
    return *reinterpret_cast<const RuntimeWriteOperatorType *>(&getOperatorType());
  }

  int mFile;
  RuntimePrinter mPrinter;
  ZLibCompress * mCompressor;
  AsyncWriter<RuntimeWriteOperator> mWriter;
  boost::thread * mWriterThread;
  int64_t mRead;
public:
  RuntimeWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType);
  ~RuntimeWriteOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();
};

RuntimeWriteOperator::RuntimeWriteOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mFile(-1),
  mPrinter(getWriteType().mPrint),
  mCompressor(NULL),
  mWriter(*this),
  mWriterThread(NULL),
  mRead(0)
{
}

RuntimeWriteOperator::~RuntimeWriteOperator()
{
  delete mCompressor;
}

void RuntimeWriteOperator::start()
{
  // Are we compressing?
  boost::filesystem::path p (getWriteType().mFile);
  if (boost::algorithm::iequals(".gz", boost::filesystem::extension(p))) {
    mCompressor = new ZLibCompress();
  }
  // Create directories if necessary before opening file.
  if (!p.parent_path().empty()) {
    boost::filesystem::create_directories(p.parent_path());
  }
  mFile = ::open(getWriteType().mFile.c_str(),
		 O_WRONLY|O_CREAT|O_TRUNC,
		 S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP);
  if (mFile == -1) {
    throw std::runtime_error("Couldn't create file");
  }
  // Start a thread that will write
  mWriterThread = new boost::thread(boost::bind(&AsyncWriter<RuntimeWriteOperator>::run, 
						boost::ref(mWriter)));
  mState = START;
  onEvent(NULL);
}

void RuntimeWriteOperator::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (mRead == 0 && getWriteType().mHeader.size() && 
      getWriteType().mHeaderFile.size() == 0) {
    if (mCompressor) {
      mCompressor->put((const uint8_t *) getWriteType().mHeader.c_str(), 
		       getWriteType().mHeader.size(), 
		       false);
      while(!mCompressor->run()) {
	uint8_t * output;
	std::size_t outputLen;
	mCompressor->consumeOutput(output, outputLen);
	::write(mFile, (const uint8_t *) output, outputLen);
      }
    } else {
      ::write(mFile, (const uint8_t *) getWriteType().mHeader.c_str(), 
	      getWriteType().mHeader.size());
    }
  }
  if (!isEOS) {
    mPrinter.print(input, true);
    getWriteType().mFree.free(input);
    if (mCompressor) {
      mCompressor->put((const uint8_t *) mPrinter.c_str(), mPrinter.size(), 
		       false);
      while(!mCompressor->run()) {
	uint8_t * output;
	std::size_t outputLen;
	mCompressor->consumeOutput(output, outputLen);
	::write(mFile, (const uint8_t *) output, outputLen);
      }
    } else {
      ::write(mFile, (const uint8_t *) mPrinter.c_str(), mPrinter.size());
    }
    mPrinter.clear();
    mRead += 1;
  } else {
    if (mCompressor) {
      // Flush data through the compressor.
      mCompressor->put(NULL, 0, true);
      while(true) {
	mCompressor->run();
	uint8_t * output;
	std::size_t outputLen;
	mCompressor->consumeOutput(output, outputLen);
	if (outputLen > 0) {
	  ::write(mFile, (const uint8_t *) output, outputLen);
	} else {
	  break;
	}
      }    
    }
    // Clean close of file and file system
    ::close(mFile);
    mFile = -1;
  }
}

void RuntimeWriteOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    if(getWriteType().mHeader.size() && 
       getWriteType().mHeaderFile.size()) {
      boost::filesystem::path p (getWriteType().mHeaderFile);
      if (boost::algorithm::iequals(".gz", p.extension().string())) {
	// TODO: Support compressed header file.
	throw std::runtime_error("Writing compressed header file not supported yet");
      }
      std::ofstream headerFile(getWriteType().mHeaderFile.c_str(),
			       std::ios_base::out);
      headerFile << getWriteType().mHeader.c_str();
      headerFile.flush();
    }
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
	bool isEOS = RecordBuffer::isEOS(input);
	//writeToHdfs(input, isEOS);
	mWriter.enqueue(input);
	if (isEOS) {
	  // Wait for writers to flush; perhaps this should be done is shutdown
	  mWriterThread->join();
	  break;
	}
      }
    }
  }
}

void RuntimeWriteOperator::shutdown()
{
  if (mFile != -1) {
    ::close(mFile);
    mFile = -1;
  }
}

RuntimeOperator * RuntimeWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeWriteOperator(services, *this);
}


/**
 * We write data to HDFS by first writing to a temporary
 * file and then renaming that temporary file to a permanent
 * file name.
 * In a program that writes multiple files, it is crucial that
 * the files be renamed in a deterministic order.  The reason for
 * this is that multiple copies of the program may be running
 * (e.g. due to Hadoop speculative execution) and we demand that
 * exactly one of the copies succeeds.  If we do not have a deterministic
 * order of file renames then it is possible for a "deadlock"-like
 * scenario to occur in which all copies fail (think of renaming
 * a file as being equivalent to taking a write lock on a resource
 * identified by the file name).
 */
class HdfsFileCommitter
{
private:
  std::vector<boost::shared_ptr<class HdfsFileRename> >mActions;
  std::string mError;
  /**
   * Number of actions that have requested commit
   */
  std::size_t mCommits;

  // TODO: Don't use a singleton here, have the dataflow
  // manage the lifetime.
  static HdfsFileCommitter * sCommitter;
  static int32_t sRefCount;
  static boost::mutex sGuard;
public:  
  static HdfsFileCommitter * get();
  static void release(HdfsFileCommitter *);
  HdfsFileCommitter();
  ~HdfsFileCommitter();
  void track (PathPtr from, PathPtr to,
	      FileSystem * fileSystem);
  bool commit();
  const std::string& getError() const 
  {
    return mError;
  }
};


/**
 * Implements a 2-phase commit like protocol for committing
 * a rename.
 */
class HdfsFileRename
{
private:
  PathPtr mFrom;
  PathPtr mTo;
  FileSystem * mFileSystem;
public:
  HdfsFileRename(PathPtr from,
		 PathPtr to,
		 FileSystem * fileSystem);
  ~HdfsFileRename();
  bool prepare(std::string& err);
  void commit();
  void rollback();
  void dispose();
  static bool renameLessThan (boost::shared_ptr<HdfsFileRename> lhs, 
			      boost::shared_ptr<HdfsFileRename> rhs);
};


HdfsFileCommitter * HdfsFileCommitter::sCommitter = NULL;
int32_t HdfsFileCommitter::sRefCount = 0;
boost::mutex HdfsFileCommitter::sGuard;

HdfsFileCommitter * HdfsFileCommitter::get()
{
  boost::unique_lock<boost::mutex> lock(sGuard);
  if (sRefCount++ == 0) {
    sCommitter = new HdfsFileCommitter();
  }
  return sCommitter;
}

void HdfsFileCommitter::release(HdfsFileCommitter *)
{
  boost::unique_lock<boost::mutex> lock(sGuard);
  if(--sRefCount == 0) {
    delete sCommitter;
    sCommitter = NULL;
  }
}

HdfsFileCommitter::HdfsFileCommitter()
  :
  mCommits(0)
{
}

HdfsFileCommitter::~HdfsFileCommitter()
{
}

void HdfsFileCommitter::track (PathPtr from, 
			       PathPtr to,
			       FileSystem * fileSystem)
{
  mActions.push_back(boost::shared_ptr<HdfsFileRename>(new HdfsFileRename(from, to, fileSystem)));
}

bool HdfsFileCommitter::commit()
{
  std::cout << "HdfsFileCommitter::commit; mCommits = " << mCommits << std::endl;
  if (++mCommits == mActions.size()) {      
    // Sort the actions to make a deterministic order.
    std::sort(mActions.begin(), mActions.end(), 
	      HdfsFileRename::renameLessThan);
    for(std::size_t i = 0; i<mActions.size(); ++i) {
      if (!mActions[i]->prepare(mError)) {
	// Failed to commit delete temp files that haven't
	// been dealt with.  Note that we don't rollback
	// since we are no longer assuming a single process
	// writes all of the files.  If we did rollback then we
	// might be undoing work that another process that has
	// succeeded is assuming is in the filesystem. That would
	// appear to the user as job success when some files
	// have not been written.
	// See CR 1459063 for more details.
	for(std::size_t j = 0; j<mActions.size(); ++j) {
	  mActions[j]->dispose();
	}
	mActions.clear();
	mCommits = 0;
	return false;
      }
    }
    // Everyone voted YES, so commit (essentially a noop).
    for(std::size_t i = 0; i<mActions.size(); ++i) {
      mActions[i]->commit();
    }
    mActions.clear();
    mCommits = 0;
  }
  return true;
}

bool HdfsFileRename::renameLessThan (boost::shared_ptr<HdfsFileRename> lhs, 
				     boost::shared_ptr<HdfsFileRename> rhs)
{
  return strcmp(lhs->mTo->toString().c_str(), 
		rhs->mTo->toString().c_str()) < 0;
}

HdfsFileRename::HdfsFileRename(PathPtr from,
			       PathPtr to,
			       FileSystem * fileSystem)
  :
  mFrom(from),
  mTo(to),
  mFileSystem(fileSystem)
{
  if (!mFrom || mFrom->toString().size() == 0 || 
      !mTo || mTo->toString().size() == 0)
    throw std::runtime_error("HdfsFileRename::HdfsFileRename "
			     "expects non-empty filenames");
}

HdfsFileRename::~HdfsFileRename()
{
}

bool HdfsFileRename::prepare(std::string& err)
{
  if (mFrom && mFrom->toString().size()) {
    BOOST_ASSERT(mTo && mTo->toString().size() != 0);
    std::cout << "HdfsFileRename::prepare renaming " << 
      (*mFrom) << " to " <<
      (*mTo) << std::endl;
    if (!mFileSystem->rename(mFrom, mTo)) {
      std::string msg = (boost::format("Failed to rename HDFS file %1% to %2%") %
			 (*mFrom) % (*mTo)).str();
      std::cout << msg.c_str() << std::endl;
      // Check whether the file already exists.  If so and it is
      // the same size as what we just wrote, then assume idempotence
      // and return success.
      boost::shared_ptr<FileStatus> toStatus, fromStatus;
      try {
	toStatus = 
	  mFileSystem->getStatus(mTo);
      } catch(std::exception & ex) {
	  err = (boost::format("Rename failed and target file %1% does "
			       "not exist") %
		 (*mTo)).str();      
	  std::cout << err.c_str() << std::endl;
	  return false;
      }
      try {
	fromStatus = 
	  mFileSystem->getStatus(mFrom);
      } catch(std::exception & ex) {
	err = (boost::format("Rename failed, target file %1% exists "
			     "but failed to "
			     "get status of temporary file %2%") %
	       (*mTo) % (*mFrom)).str();      
	std::cout << err.c_str() << std::endl;
	return false;
      }
      
      // This is an interesting check but with compression enabled
      // it isn't guaranteed to hold.  In particular, in a reducer
      // to which we have emitted with a non-unique key there is
      // non-determinism in the order of the resulting stream (depending
      // on the order in which map files are processed for example).
      // If the order winds up being sufficiently different between two
      // files, then the compression ratio may differ and the resulting
      // file sizes won't match.
      if (toStatus->size() != fromStatus->size()) {
	msg = (boost::format("Rename failed: target file %1% already "
			     "exists and has size %2%, "
			     "temporary file %3% has size %4%; "
			     "ignoring rename failure and continuing") %
	       (*mTo) % toStatus->size() % 
	       (*mFrom) % fromStatus->size()).str();      
      } else {
	msg = (boost::format("Both %1% and %2% have the same size; ignoring "
			     "rename failure and continuing") %
	       (*mFrom) % (*mTo)).str();
      }
      std::cout << msg.c_str() << std::endl;
    } 
    mFrom = PathPtr();      
    return true;
  } else {
    return false;
  }
}

void HdfsFileRename::commit()
{
  if (!mFrom && mTo) {
    std::cout << "HdfsFileRename::commit " << (*mTo) << std::endl;
    // Only commit if we prepared.
    mTo = PathPtr();
  }
}

void HdfsFileRename::rollback()
{
  if (!mFrom && mTo) {
    // Only rollback if we prepared.
    std::cout << "Rolling back permanent file: " << (*mTo) << std::endl;
    mFileSystem->remove(mTo);
    mTo = PathPtr();
  }
}

void HdfsFileRename::dispose()
{
  if (mFrom) {
    std::cout << "Removing temporary file: " << (*mFrom) << std::endl;
    mFileSystem->remove(mFrom);
    mFrom = PathPtr();
  }
}

class OutputFile
{
public:
  WritableFile * File;
  ZLibCompress Compressor;
  OutputFile(WritableFile * f) 
    :
    File(f)
  {
  }
  ~OutputFile()
  {
    close();
  }
  void flush()
  {
    // Flush data through the compressor.
    this->Compressor.put(NULL, 0, true);
    while(true) {
      this->Compressor.run();
      uint8_t * output;
      std::size_t outputLen;
      this->Compressor.consumeOutput(output, outputLen);
      if (outputLen > 0) {
	this->File->write(output, outputLen);
      } else {
	break;
      }
    }    
    // Flush data to disk
    this->File->flush();
  }
  bool close() {
    // Clean close of file 
    bool ret = true;
    if (File != NULL) {
      ret = File->close();
      delete File;
      File = NULL;
    }
    return ret;
  }
};

class FileCreation
{
public:
  virtual ~FileCreation() {}
  virtual void start(FileSystem * genericFileSystem,
		     PathPtr rootUri,
		     class RuntimeHdfsWriteOperator * fileFactory)=0;
  virtual OutputFile * onRecord(RecordBuffer input,
			class RuntimeHdfsWriteOperator * fileFactory)=0;
  virtual void close(bool flush)=0;
  virtual void commit(std::string& err)=0;
  virtual void setCompletionPort(ServiceCompletionFifo * f)
  {
  }
};

/**
 * This policy supports keeping multiple files open
 * for writes and a close policy that defers to the file
 * committer.
 */
class MultiFileCreation : public FileCreation
{
private:
  const MultiFileCreationPolicy& mPolicy;
  PathPtr mRootUri;
  FileSystem * mGenericFileSystem;
  InterpreterContext * mRuntimeContext;
  std::map<std::string, OutputFile *> mFile;
  HdfsFileCommitter * mCommitter;
  int32_t mPartition;

  OutputFile * createFile(const std::string& filePath,
			  class RuntimeHdfsWriteOperator * factory);
  void add(const std::string& filePath, OutputFile * of)
  {
    mFile[filePath] = of;
  }
public:
  MultiFileCreation(const MultiFileCreationPolicy& policy, 
		    RuntimeOperator::Services& services);
  ~MultiFileCreation();
  void start(FileSystem * genericFileSystem, PathPtr rootUri,
	     class RuntimeHdfsWriteOperator * fileFactory);
  OutputFile * onRecord(RecordBuffer input,
			class RuntimeHdfsWriteOperator * fileFactory);
  void close(bool flush);
  void commit(std::string& err);
};

class StreamingFileCreation : public FileCreation
{
private:
  StreamingFileCreationPolicy mPolicy;
  PathPtr mRootUri;
  FileSystem * mGenericFileSystem;
  std::size_t mNumRecords;
  boost::asio::deadline_timer mTimer;
  ServiceCompletionFifo * mCompletionPort;
  // Path of current file as we are writing to it
  PathPtr mTempPath;
  // Final path name of file.
  PathPtr mFinalPath;
  OutputFile * mCurrentFile;

  OutputFile * createFile(const std::string& filePath,
			  RuntimeHdfsWriteOperator * factory);

  // Timer callback
  void timeout(const boost::system::error_code& err);
public:
  StreamingFileCreation(const StreamingFileCreationPolicy& policy,
			RuntimeOperator::Services& services);
  ~StreamingFileCreation();
  void start(FileSystem * genericFileSystem, PathPtr rootUri,
	     class RuntimeHdfsWriteOperator * fileFactory);
  OutputFile * onRecord(RecordBuffer input,
			class RuntimeHdfsWriteOperator * fileFactory);
  void close(bool flush);
  void commit(std::string& err);
  virtual void setCompletionPort(ServiceCompletionFifo * f)
  {
    mCompletionPort = f;
  }
};

class RuntimeHdfsWriteOperator : public RuntimeOperatorBase<RuntimeHdfsWriteOperatorType>
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  
private:
  enum State { START, READ };
  State mState;

  PathPtr mRootUri;
  RuntimePrinter mPrinter;
  AsyncWriter<RuntimeHdfsWriteOperator> mWriter;
  boost::thread * mWriterThread;
  HdfsFileCommitter * mCommitter;
  std::string mError;
  FileCreation * mCreationPolicy;

  void renameTempFile();
  /**
   * Is this operator writing an inline header?
   */
  bool hasInlineHeader() 
  {
    return getMyOperatorType().mHeader.size() != 0 && 
      getMyOperatorType().mHeaderFile.size()==0;
  }
  /**
   * Is this operator writing a header file?
   */
  bool hasHeaderFile()
  {
    return getPartition() == 0 && 
      getMyOperatorType().mHeader.size() != 0 && 
      getMyOperatorType().mHeaderFile.size()!=0;  
  }
  
public:
  RuntimeHdfsWriteOperator(RuntimeOperator::Services& services, const RuntimeHdfsWriteOperatorType& opType);
  ~RuntimeHdfsWriteOperator();
  void start();
  void onEvent(RuntimePort * port);
  void shutdown();

  /**
   * Create a file
   */
  OutputFile * createFile(PathPtr filePath);
};

MultiFileCreationPolicy::MultiFileCreationPolicy()
  :
  mTransfer(NULL),
  mTransferFree(NULL),
  mTransferOutput(NULL)
{
}

MultiFileCreationPolicy::MultiFileCreationPolicy(const std::string& hdfsFile,
						 const RecordTypeTransfer * argTransfer)
  :
  mHdfsFile(hdfsFile),
  mTransfer(argTransfer ? argTransfer->create() : NULL),
  mTransferFree(argTransfer ? new RecordTypeFree(argTransfer->getTarget()->getFree()) : NULL),
  mTransferOutput(argTransfer ? new FieldAddress(*argTransfer->getTarget()->begin_offsets()) : NULL)
{
}

MultiFileCreationPolicy::~MultiFileCreationPolicy()
{
  delete mTransfer;
  delete mTransferFree;
  delete mTransferOutput;
}

FileCreation * MultiFileCreationPolicy::create(RuntimeOperator::Services& services) const
{
  return new MultiFileCreation(*this, services);
}

MultiFileCreation::MultiFileCreation(const MultiFileCreationPolicy& policy,
				     RuntimeOperator::Services& services)
  :
  mPolicy(policy),
  mGenericFileSystem(NULL),
  mRuntimeContext(NULL),
  mCommitter(NULL),
  mPartition(services.getPartition())
{
  if (NULL != policy.mTransfer) {
    mRuntimeContext = new InterpreterContext();
  }
}

MultiFileCreation::~MultiFileCreation()
{
  for(std::map<std::string, OutputFile*>::iterator it = mFile.begin(),
	end = mFile.end(); it != end; ++it) {
    if (it->second->File) {
      // Abnormal shutdown
      it->second->File->close();
    }
  }

  if (mCommitter) {
    HdfsFileCommitter::release(mCommitter);
    mCommitter = NULL;
  }
}

OutputFile * MultiFileCreation::createFile(const std::string& filePath,
					   RuntimeHdfsWriteOperator * factory)
{
  // We create a temporary file name and write to that.  When
  // complete we'll rename the file.  This should make things safe
  // in case multiple copies of the operator are trying to write
  // to the same file (e.g. running in Hadoop with speculative execution).
  // The temporary file name must have the pattern serial_ddddd
  // so that it uses the appropriate block placement policy.
  std::string tmpStr = FileSystem::getTempFileName();

  std::stringstream str;
  str << filePath << "/" << tmpStr << "_serial_" << 
    std::setw(5) << std::setfill('0') << mPartition <<
    ".gz";
  PathPtr tempPath = Path::get(mRootUri, str.str());
  std::stringstream permFile;
  permFile << filePath + "/serial_" << 
    std::setw(5) << std::setfill('0') << mPartition <<
    ".gz";  
  if (mCommitter == NULL) {
    mCommitter = HdfsFileCommitter::get();
  }
  mCommitter->track(tempPath, 
		    Path::get(mRootUri, permFile.str()), 
		    mGenericFileSystem);
  // Call back to the factory to actually create the file
  OutputFile * of = factory->createFile(tempPath);
  add(filePath, of);
  return of;
}

void MultiFileCreation::start(FileSystem * genericFileSystem,
			      PathPtr rootUri,
			      RuntimeHdfsWriteOperator * factory)
{
  mGenericFileSystem = genericFileSystem;
  mRootUri = rootUri;
  // If statically defined path, create and open file here.
  if (0 != mPolicy.mHdfsFile.size()) {
    createFile(mPolicy.mHdfsFile, factory);
  }
}

OutputFile * MultiFileCreation::onRecord(RecordBuffer input,
					 RuntimeHdfsWriteOperator * factory)
{
  if (NULL == mRuntimeContext) {
    return mFile.begin()->second;
  } else {
    RecordBuffer output;
    mPolicy.mTransfer->execute(input, output, mRuntimeContext, false);
    std::string fileName(mPolicy.mTransferOutput->getVarcharPtr(output)->c_str());
    mPolicy.mTransferFree->free(output);
    mRuntimeContext->clear();
    std::map<std::string, OutputFile *>::iterator it = mFile.find(fileName);
    if (mFile.end() == it) {
      std::cout << "Creating file: " << fileName.c_str() << std::endl;
      return createFile(fileName, factory);
    }
    return it->second;
  }
}

void MultiFileCreation::close(bool flush)
{
  for(std::map<std::string, OutputFile*>::iterator it = mFile.begin(),
	end = mFile.end(); it != end; ++it) {
    if (it->second->File != NULL) {
      if (flush) {
	it->second->flush();
      }
      it->second->close();
      it->second->File = NULL;
    }
  }
}

void MultiFileCreation::commit(std::string& err)
{
  // Put the file(s) in its final place.
  for(std::map<std::string, OutputFile*>::iterator it = mFile.begin(),
	end = mFile.end(); it != end; ++it) {    
    if(!mCommitter->commit()) {
      err = mCommitter->getError();
      break;
    } 
  }
}

StreamingFileCreationPolicy::StreamingFileCreationPolicy(const std::string& baseDir,
							 std::size_t fileSeconds,
							 std::size_t fileRecords)
  :
  mBaseDir(baseDir),
  mFileSeconds(fileSeconds),
  mFileRecords(fileRecords)
{
}

StreamingFileCreationPolicy::~StreamingFileCreationPolicy()
{
}

FileCreation * StreamingFileCreationPolicy::create(RuntimeOperator::Services& services) const
{
  return new StreamingFileCreation(*this, services);
}

StreamingFileCreation::StreamingFileCreation(const StreamingFileCreationPolicy& policy,
					     RuntimeOperator::Services& services)
  :
  mPolicy(policy),
  mGenericFileSystem(NULL),
  mNumRecords(0),
  mCurrentFile(NULL),
  mTimer(services.getIOService()),
  mCompletionPort(NULL)
{
}

StreamingFileCreation::~StreamingFileCreation()
{
}

void StreamingFileCreation::timeout(const boost::system::error_code& err)
{
  // Post to operator completion port.
  if(err != boost::asio::error::operation_aborted) {
    RecordBuffer buf;
    mCompletionPort->write(buf);
  }
}

OutputFile * StreamingFileCreation::createFile(const std::string& filePath,
					       RuntimeHdfsWriteOperator * factory)
{
  BOOST_ASSERT(mCurrentFile == NULL);

  // We create a temporary file name and write to that.  
  // Here we are not using a block placement naming scheme 
  // since we are likely executing on a single partition at this point.
  // This assumes we have a downstream splitter process that partitions
  // data.
  std::string tmpStr = FileSystem::getTempFileName();

  std::stringstream str;
  str << filePath << "/" << tmpStr << ".tmp";
  mTempPath = Path::get(mRootUri, str.str());
  std::stringstream finalStr;
  finalStr << filePath << "/" << tmpStr << ".gz";
  mFinalPath = Path::get(mRootUri, finalStr.str());
  // Call back to the factory to actually create the file
  mCurrentFile = factory->createFile(mTempPath);
  
  if (mPolicy.mFileSeconds > 0) {
    mTimer.expires_from_now(boost::posix_time::seconds(mPolicy.mFileSeconds));
    mTimer.async_wait(boost::bind(&StreamingFileCreation::timeout, 
				  this,
				  boost::asio::placeholders::error));
  }

  return mCurrentFile;
}

void StreamingFileCreation::start(FileSystem * genericFileSystem,
				  PathPtr rootUri,
				  RuntimeHdfsWriteOperator * factory)
{
  mGenericFileSystem = genericFileSystem;
  mRootUri = rootUri;
}

OutputFile * StreamingFileCreation::onRecord(RecordBuffer input,
					     RuntimeHdfsWriteOperator * factory)
{
  if ((0 != mPolicy.mFileRecords && 
       mPolicy.mFileRecords <= mNumRecords) ||
      (0 != mPolicy.mFileSeconds &&
       RecordBuffer() == input)) {
    close(true);
  }
  // Check if we have an input to record
  if (input != RecordBuffer()) {
    // Check if we have an open file
    if (mCurrentFile == NULL)
      createFile(mPolicy.mBaseDir, factory);
    mNumRecords += 1;
  }
  return mCurrentFile;
}

void StreamingFileCreation::close(bool flush)
{
  if (mCurrentFile != NULL) {
    if (flush) {
      mCurrentFile->flush();
    }
    mCurrentFile->close();
    delete mCurrentFile;
    mCurrentFile = NULL;
    mNumRecords = 0;
    if (flush && NULL != mTempPath.get() && 
	NULL != mFinalPath.get()) {
      // Put the file in its final place; don't wait for commit
      mGenericFileSystem->rename(mTempPath, mFinalPath);
      // TODO: Error!!!!  Queue this up for a retry.
      mTempPath = mFinalPath = PathPtr();
    }
    // Delete any file on unclean shutdown...
    if (mPolicy.mFileSeconds > 0) {
      // May not be necessary if we closing a file as a result
      // of a timeout
      mTimer.cancel();
    }
  }
}

void StreamingFileCreation::commit(std::string& err)
{
  // Streaming files don't wait for a commit signal.
}

RuntimeHdfsWriteOperator::RuntimeHdfsWriteOperator(RuntimeOperator::Services& services, 
						   const RuntimeHdfsWriteOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeHdfsWriteOperatorType>(services, opType),
  mState(START),
  mPrinter(getMyOperatorType().mPrint),
  mWriter(*this),
  mWriterThread(NULL),
  mCommitter(NULL),
  mCreationPolicy(opType.mCreationPolicy->create(services))
{
}

RuntimeHdfsWriteOperator::~RuntimeHdfsWriteOperator()
{
  delete mWriterThread;

  delete mCreationPolicy;
  
  if (mCommitter) {
    HdfsFileCommitter::release(mCommitter);
    mCommitter = NULL;
  }
}

OutputFile * RuntimeHdfsWriteOperator::createFile(PathPtr filePath)
{
  // TODO: Check if file exists
  // TODO: Make sure file is cleaned up in case of failure.
  WritableFile * f = getMyOperatorType().mFileFactory->openForWrite(filePath);
  OutputFile * of = new OutputFile(f);
  if (hasInlineHeader()) {
    // We write in-file header for every partition
    of->Compressor.put((const uint8_t *) getMyOperatorType().mHeader.c_str(), 
		       getMyOperatorType().mHeader.size(), 
		       false);
    while(!of->Compressor.run()) {
      uint8_t * output;
      std::size_t outputLen;
      of->Compressor.consumeOutput(output, outputLen);
      of->File->write(output, outputLen);
    }
  } 
  return of;
}

void RuntimeHdfsWriteOperator::start()
{
  WritableFileFactory * ff = getMyOperatorType().mFileFactory;
  mRootUri = ff->getFileSystem()->getRoot();
  if (getMyOperatorType().mCreationPolicy->requiresServiceCompletionPort()) {
    mCreationPolicy->setCompletionPort(&(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo()));
  }
  mCreationPolicy->start(ff->getFileSystem(), mRootUri, this);

  if (hasHeaderFile()) {
    PathPtr headerPath = Path::get(getMyOperatorType().mHeaderFile);
    std::string tmpHeaderStr = FileSystem::getTempFileName();
    PathPtr tmpHeaderPath = Path::get(mRootUri, 
				      "/tmp/headers/" + tmpHeaderStr);
    if (mCommitter == NULL) {
      mCommitter = HdfsFileCommitter::get();
    }
    mCommitter->track(tmpHeaderPath, headerPath, 
		      getMyOperatorType().mFileFactory->getFileSystem());
    WritableFile * headerFile = ff->openForWrite(tmpHeaderPath);
    if (headerFile == NULL) {
      throw std::runtime_error("Couldn't create header file");
    }
    headerFile->write((const uint8_t *) &getMyOperatorType().mHeader[0], 
		      getMyOperatorType().mHeader.size());
    headerFile->close();
    delete headerFile;
  }
  // Start a thread that will write
  // mWriterThread = 
  //   new boost::thread(boost::bind(&AsyncWriter<RuntimeHdfsWriteOperator>::run, 
  // 				  boost::ref(mWriter)));
  mState = START;
  onEvent(NULL);
}

void RuntimeHdfsWriteOperator::renameTempFile()
{
  mCreationPolicy->commit(mError);

  if (0 == mError.size() && hasHeaderFile()) {
    // Commit the header file as well.
    if(!mCommitter->commit()) {
      mError = mCommitter->getError();
    } 
  }
}

void RuntimeHdfsWriteOperator::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (!isEOS) {
    OutputFile * of = mCreationPolicy->onRecord(input, this);
    mPrinter.print(input);
    getMyOperatorType().mFree.free(input);
    of->Compressor.put((const uint8_t *) mPrinter.c_str(), mPrinter.size(), false);
    while(!of->Compressor.run()) {
      uint8_t * output;
      std::size_t outputLen;
      of->Compressor.consumeOutput(output, outputLen);
      of->File->write(output, outputLen);
    }
    mPrinter.clear();
  } else {
    mCreationPolicy->close(true);
  }
}

void RuntimeHdfsWriteOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      if (getCompletionPorts().size()) {
	getCompletionPorts()[0]->request_unlink();
	getCompletionPorts()[0]->request_link_after(*getInputPorts()[0]);
	requestRead(*getCompletionPorts()[0]);
      } else {
	requestRead(0);
      }
      mState = READ;
      return;
    case READ:
      RecordBuffer input;
      read(port, input);
      if (port == getInputPorts()[0]) {
	bool isEOS = RecordBuffer::isEOS(input);
	writeToHdfs(input, isEOS);
	// mWriter.enqueue(input);
	if (isEOS) {
	  // Wait for writers to flush; perhaps this should be done is shutdown
	  // mWriterThread->join();
	  // See if there was an error within the writer that we need
	  // to throw out.  Errors here are thow that would have happened
	  // after we enqueued EOS.
	  // std::string err;
	  // mWriter.getError(err);
	  // if (err.size()) {
	  //   throw std::runtime_error(err);
	  // }
	  // Do the rename of the temp file in the main dataflow
	  // thread because this makes the order in which files are renamed
	  // across the entire dataflow deterministic (at least for the map-reduce
	  // case where there is only a single dataflow thread and all collectors/reducers
	  // are sort-merge).
	  renameTempFile();
	  break;
	}
      } else {
	BOOST_ASSERT(port == getCompletionPorts()[0]);
	mCreationPolicy->onRecord(RecordBuffer(), this);
      }
    }
  }
}

void RuntimeHdfsWriteOperator::shutdown()
{
  mCreationPolicy->close(false);
  if (mError.size() != 0) {
    throw std::runtime_error(mError);
  }
}

RuntimeHdfsWriteOperatorType::RuntimeHdfsWriteOperatorType(const std::string& opName,
							   const RecordType * ty,
							   WritableFileFactory * fileFactory,
							   const std::string& header,
							   const std::string& headerFile,
							   FileCreationPolicy * creationPolicy)
  :
  RuntimeOperatorType(opName.c_str()),
  mPrint(ty->getPrint()),
  mFree(ty->getFree()),
  mFileFactory(fileFactory),
  mHeader(header),
  mHeaderFile(headerFile),
  mCreationPolicy(creationPolicy)
{
}

RuntimeHdfsWriteOperatorType::~RuntimeHdfsWriteOperatorType()
{
  delete mFileFactory;
  delete mCreationPolicy;
}

int32_t RuntimeHdfsWriteOperatorType::numServiceCompletionPorts() const
{
  return mCreationPolicy->requiresServiceCompletionPort() ? 1 : 0;
}

RuntimeOperator * RuntimeHdfsWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeHdfsWriteOperator(services, *this);
}

