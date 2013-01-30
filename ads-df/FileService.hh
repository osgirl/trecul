#ifndef __FILESERVICE_H__
#define __FILESERVICE_H__

#include <stdint.h>
#include <stdexcept>
#include <string>
#include "RecordBuffer.hh"

class DataflowScheduler;
class FileService;
class FileServiceFile;
class RuntimePort;

/**
 * A service for file IO available to dataflow operators.
 * Dataflow operators are STRONGLY encouraged to delegate
 * all file IO to the service since this is how they can
 * avoid blocking and integrate the async file IO notifications
 * with the dataflow scheduler.
 *
 * This implementation uses a thread pool over synchronous
 * file IO apis.
 */

class FileService
{
private:
  // Hide implementation
  class FileServiceImpl * mImpl;
public:
  static FileService * get();
  static void release(FileService * fs);
  FileService(int32_t numThreads);
  ~FileService();
  void requestOpenForRead(const std::string& filename,
			  uint64_t begin,
			  uint64_t end,
			  RuntimePort * completionPort);
  void requestRead(FileServiceFile * file,
		   uint8_t * buffer,
		   int32_t size,
		   RuntimePort * completionPort);

  // Utility routines for handling completion 
  // messages on ports; these are hiding the 
  // rather ugly fact that schedulable ports
  // are assumed to carry dynamic records (RecordBuffer).
  // Improved abstractions should fix this.
  FileServiceFile * getOpenResponse(RecordBuffer buf)
  {
    return (FileServiceFile *) buf.Ptr;
  }
  int32_t getReadBytes(RecordBuffer buf) 
  {
    // TODO: Remove this cast!!!!!
    return (int32_t) (std::size_t) buf.Ptr;
  }
};

#endif
