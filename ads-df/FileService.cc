#include <boost/thread/mutex.hpp>

#include "FileService.hh"
#include "AsynchronousFileSystem.hh"
#include "HdfsOperator.hh"
#include "RuntimeProcess.hh"
#include "RecordParser.hh"

//typedef AsynchronousFileSystem<hdfs_file_traits> AsyncFileSystem;
typedef AsynchronousFileSystem<stdio_file_traits> AsyncFileSystem;

class FileServiceFile
{
private:
  AsyncFileSystem::file_type mHandle;
public:
  FileServiceFile(AsyncFileSystem::file_type h)
    :
    mHandle(h)
  {
  }
  AsyncFileSystem::file_type getHandle() 
  {
    return mHandle;
  }
};

class FileServiceHandler : public AsyncFileSystem::handler_type
{
  ServiceCompletionFifo& mCompletion;
public:
  FileServiceHandler(ServiceCompletionFifo & channel)
    :
    mCompletion(channel)
  {
  }
  void readComplete(int32_t bytesRead)
  {
    // std::cout << "[FileServiceHandler::readComplete] " << std::endl;
    RecordBuffer buf;
    // TODO: Elimnate egregious hack
    buf.Ptr = (uint8_t *) bytesRead;
    mCompletion.write(buf);
    delete this;
  }

  void writeComplete(int32_t bytesWritten)
  {
    throw std::runtime_error("Not implemented yet");
    delete this;
  }

  void openComplete(AsyncFileSystem::file_type f)
  {
    // std::cout << "[FileServiceHandler::openComplete] " << std::endl;
    RecordBuffer buf;
    // TODO: Elimnate egregious hack
    buf.Ptr = (uint8_t *) new FileServiceFile(f);
    mCompletion.write(buf);
    delete this;
  }
};

class FileServiceImpl
{
private:
  AsyncFileSystem * mHdfs;
public:
  FileServiceImpl();
  ~FileServiceImpl();
  void requestOpenForRead(const std::string& filename,
			  uint64_t begin,
			  uint64_t end,
			  RuntimePort * completionPort);
  void requestRead(FileServiceFile * file,
		   uint8_t * buffer,
		   int32_t size,
		   RuntimePort * completionPort);
};

FileServiceImpl::FileServiceImpl()
  :
  mHdfs(NULL)
{
}

FileServiceImpl::~FileServiceImpl()
{
  if (mHdfs) {
    AsyncFileSystem::release(mHdfs);
    mHdfs = NULL;
  }
}

void FileServiceImpl::requestOpenForRead(const std::string& filename,
						 uint64_t begin,
						 uint64_t end,
						 RuntimePort * completionPort)
{
  if (!mHdfs) {
    // std::cout << "[FileServiceImpl::requestOpenForRead] Making async request" << std::endl;
    mHdfs = AsyncFileSystem::get();
  }

  FileServiceHandler * handler = new FileServiceHandler(dynamic_cast<ServiceCompletionPort*>(completionPort)->getFifo());
  // std::cout << "[FileServiceImpl::requestOpenForRead] Making async open request" << std::endl;
  mHdfs->requestOpen(filename.c_str(), begin, end, *handler);
}

void FileServiceImpl::requestRead(FileServiceFile * file,
				  uint8_t * buffer,
				  int32_t size,
				  RuntimePort * completionPort)
{
  if (!mHdfs) {
    // std::cout << "[FileServiceImpl::requestRead] Opening File System" << std::endl;
    mHdfs = AsyncFileSystem::get();
  }
  FileServiceHandler * handler = new FileServiceHandler(dynamic_cast<ServiceCompletionPort*>(completionPort)->getFifo());
  // std::cout << "[FileServiceImpl::requestRead] Making async read request" << std::endl;
  mHdfs->requestRead(file->getHandle(),
		     buffer, size, *handler);
		     
}

FileService::FileService(int32_t numThreads)
  :
  mImpl(new FileServiceImpl())
{
}

FileService::~FileService()
{
  delete mImpl;
}

void FileService::requestOpenForRead(const std::string& filename,
				     uint64_t begin,
				     uint64_t end,
				     RuntimePort * completionPort)
{
  mImpl->requestOpenForRead(filename, begin, end, completionPort);
}

void FileService::requestRead(FileServiceFile * file,
			      uint8_t * buffer,
			      int32_t size,
			      RuntimePort * completionPort)
{
  mImpl->requestRead(file, buffer, size, completionPort);
}

static FileService * gFS=NULL;
static int32_t gRefCount=0;
static boost::mutex gLock;

FileService * FileService::get()
{
  boost::unique_lock<boost::mutex> guard(gLock);
  if (gRefCount == 0) {
    gFS = new FileService(2);
  }
  gRefCount += 1;
  return gFS;
}

void FileService::release(FileService * fs)
{
  boost::unique_lock<boost::mutex> guard(gLock);
  BOOST_ASSERT(fs == gFS);
  if (fs == gFS) {
    gRefCount -= 1;
    if (gRefCount == 0)
      delete gFS;
  }
}

