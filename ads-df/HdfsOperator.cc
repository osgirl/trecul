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

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include "hdfs.h"
#include "Pipes.hh"
#include "HdfsOperator.hh"
#include "RecordParser.hh"
#include "RuntimeProcess.hh"

typedef boost::shared_ptr<class HdfsFileSystem> HdfsFileSystemPtr;
typedef boost::shared_ptr<class HdfsFileSystemImpl> HdfsFileSystemImplPtr;


class HdfsFileSystemImpl
{
private:
  static boost::mutex fsCacheGuard;
  static std::map<std::string, HdfsFileSystemImplPtr > fsCache;

  hdfsFS mFileSystem;
  
  boost::shared_ptr<FileStatus> createFileStatus(hdfsFileInfo& fi);
  HdfsFileSystemImpl(UriPtr uri);
public:
  static HdfsFileSystemImplPtr get(UriPtr uri);
  ~HdfsFileSystemImpl();

  PathPtr transformDefaultUri(PathPtr p);

  boost::shared_ptr<FileStatus> getStatus(PathPtr p);
  bool exists(PathPtr p);
  bool removeAll(PathPtr p);
  bool remove(PathPtr p);

  void list(PathPtr p,
	    std::vector<boost::shared_ptr<FileStatus> >& result);
  void readFile(UriPtr uri, std::string& out);

  int32_t write(hdfsFile f, const void * buf, int32_t sz)
  {
    tSize ret = ::hdfsWrite(mFileSystem, f, buf, sz);
    if (ret == -1) {
      throw std::runtime_error("hdfsWrite failed");
    }
    return (int32_t) ret;
  }
  int32_t read(hdfsFile f, void * buf, int32_t sz)
  {
    tSize ret = ::hdfsRead(mFileSystem, f, buf, sz);
    if (ret == -1) {
      throw std::runtime_error("hdfsRead failed");
    }
    return (int32_t) ret;
  }
  void flush(hdfsFile f)
  {
    int ret = ::hdfsFlush(mFileSystem, f);
    if (ret == -1) {
      throw std::runtime_error("hdfsFlush failed");
    }
  }
  void close(hdfsFile f)
  {
    int ret = ::hdfsCloseFile(mFileSystem, f);
    if (ret == -1) {
      throw std::runtime_error("hdfsCloseFile failed");
    }
  }
  void close(hdfsFile f, int& ret)
  {
    ret = ::hdfsCloseFile(mFileSystem, f);
  }
  uint64_t tell(hdfsFile f)
  {
    tOffset filePos = ::hdfsTell(mFileSystem, f);
    if (filePos == -1) {
      throw std::runtime_error("hdfsTell failed");
    }    
    return (uint64_t) filePos;
  }
  void seek(hdfsFile f, uint64_t pos)
  {
    int ret = ::hdfsSeek(mFileSystem, f, pos);
    if (ret == -1) {
      throw std::runtime_error("hdfsSeek failed");
    }    
  }
  void rename(PathPtr from, PathPtr to, int32_t & ret)
  {
    ret = ::hdfsRename(mFileSystem, 
		       from->getUri()->getPath().c_str(), 
		       to->getUri()->getPath().c_str());
  }
  hdfsFile open_for_read(PathPtr p)
  {
    hdfsFile f = ::hdfsOpenFile(mFileSystem, 
				p->getUri()->getPath().c_str(), 
				O_RDONLY,
				0, 0, 0);
    
    if (f == NULL) {
      throw std::runtime_error((boost::format("Couldn't open HDFS file %1%") %
				p->toString()).str());
    }
    // std::cout << "::hdfsOpenFile(" <<
    //   p->getUri()->getPath().c_str() << 
    //   ", O_RDONLY, 0, 0, 0)" << std::endl;

    return f;
  }
  hdfsFile open_for_write(PathPtr p,
			 int32_t bufferSize,
			 int32_t replicationFactor,
			 int32_t blockSize)
  {
    hdfsFile f = ::hdfsOpenFile(mFileSystem, 
				p->getUri()->getPath().c_str(),
				O_WRONLY|O_CREAT,
				bufferSize,
				replicationFactor,
				blockSize);
    if (f == NULL) {
      throw std::runtime_error("Couldn't create HDFS file");
    }

    // std::cout << "::hdfsOpenFile(" <<
    //   p->getUri()->getPath().c_str() << 
    //   ", O_WRONLY|O_CREAT, " << 
    //   bufferSize << ", " <<
    //   replicationFactor << ", " <<
    //   blockSize << ")" << std::endl;

    return f;
  }
  bool mkdir(PathPtr p)
  {
    int ret = ::hdfsCreateDirectory(mFileSystem, 
				    p->getUri()->getPath().c_str());
    return ret == 0;
  }
};

// TODO: Improve cache using a weak_ptr
boost::mutex HdfsFileSystemImpl::fsCacheGuard;
std::map<std::string, HdfsFileSystemImplPtr > HdfsFileSystemImpl::fsCache;
HdfsFileSystemImplPtr HdfsFileSystemImpl::get(UriPtr path)
{
  HdfsFileSystemImplPtr fs;
  // Note that if we were connecting to many different
  // hosts it might not be acceptable to hold the lock
  // during the connect call, but in this case it is 
  // probably better to be pessimistic and block other
  // threads since they are likely connecting to the
  // same HDFS instance.
  boost::unique_lock<boost::mutex> lk(fsCacheGuard);
  
  std::map<std::string, HdfsFileSystemImplPtr >::iterator it = fsCache.find(path->getHost());
  if (it == fsCache.end()) {
    // std::cout << "Opening HDFS file system at: " << 
    //   (*path) << std::endl;
    fs = HdfsFileSystemImplPtr(new HdfsFileSystemImpl(path));
    fsCache[path->getHost()] = fs;
  } else {
    fs = it->second;
  }
  return fs;
}

PathPtr HdfsFileSystemImpl::transformDefaultUri(PathPtr p)
{
  UriPtr uri = p->getUri();
  if (boost::algorithm::equals("default", uri->getHost()) &&
      0 == uri->getPort()) {
    hdfsFileInfo * info = hdfsGetPathInfo(mFileSystem, "/");
    if (info == NULL) {
      return PathPtr();
    }
    std::string baseUri(info->mName);
    if(uri->getPath().size() > 1)
      baseUri += uri->getPath().substr(1);
    uri = boost::shared_ptr<URI>(new URI(baseUri.c_str()));
    hdfsFreeFileInfo(info, 1);
  }
  return Path::get(uri);
}

HdfsFileSystemImpl::HdfsFileSystemImpl(UriPtr uri)
  :
  mFileSystem(NULL)
{
  mFileSystem = ::hdfsConnect(uri->getHost().c_str(), uri->getPort());
  if (mFileSystem == NULL) {
    throw std::runtime_error((boost::format("Failed to connect hdfs://%1%:%2%") %
			      uri->getHost() %
			      uri->getPort()).str());
  }
}

HdfsFileSystemImpl::~HdfsFileSystemImpl()
{
}

boost::shared_ptr<FileStatus> HdfsFileSystemImpl::createFileStatus(hdfsFileInfo& fi)
{
  std::string pathStr(fi.mName);
  if(fi.mKind == kObjectKindDirectory &&
     pathStr[pathStr.size()-1] != '/') 
    pathStr += std::string("/");
  return boost::make_shared<FileStatus>(Path::get(pathStr),
					fi.mKind == kObjectKindFile,
					fi.mKind == kObjectKindDirectory,
					(std::size_t) fi.mSize);
}

boost::shared_ptr<FileStatus> HdfsFileSystemImpl::getStatus(PathPtr p)
{
  p = transformDefaultUri(p);
  if (p == PathPtr()) {
    return boost::shared_ptr<FileStatus>();
  }
  hdfsFileInfo * fileInfo = ::hdfsGetPathInfo(mFileSystem, 
					      p->toString().c_str());
  if (fileInfo == NULL) {
    throw std::runtime_error((boost::format("File %1% does not exist") %
			      p->toString()).str());
  }
  boost::shared_ptr<FileStatus> tmp = createFileStatus(*fileInfo);
  hdfsFreeFileInfo(fileInfo, 1);
  return tmp;
}

bool HdfsFileSystemImpl::exists(PathPtr p)
{
  p = transformDefaultUri(p);
  if (p == PathPtr()) {
    return false;
  }
  hdfsFileInfo * fileInfo = ::hdfsGetPathInfo(mFileSystem, 
					      p->toString().c_str());
  if (fileInfo != NULL) {
    ::hdfsFreeFileInfo(fileInfo, 1);
    return true;
  } else {
    return false;
  }
}

bool HdfsFileSystemImpl::removeAll(PathPtr p)
{
  p = transformDefaultUri(p);
  if (p == PathPtr()) {
    return true;
  }
  int ret = ::hdfsDelete(mFileSystem, p->toString().c_str(), 1);	    
  return ret==0;
}

bool HdfsFileSystemImpl::remove(PathPtr p)
{
  p = transformDefaultUri(p);
  if (p == PathPtr()) {
    return true;
  }
  int ret = ::hdfsDelete(mFileSystem, p->toString().c_str(), 0);	    
  return ret==0;
}

void HdfsFileSystemImpl::list(PathPtr p,
			      std::vector<boost::shared_ptr<FileStatus> >& result)
{
  int numEntries=-1;
  p = transformDefaultUri(p);
  if (p == PathPtr()) {
    return;
  }
  hdfsFileInfo * tmp = ::hdfsListDirectory(mFileSystem, 
					   p->toString().c_str(), 
					   &numEntries);
  if (NULL == tmp && numEntries != 0) {
    throw std::runtime_error((boost::format("No such file or directory: %1%") %
  			      p->toString()).str());
  }
  for(int i=0; i<numEntries; ++i) {
    result.push_back(createFileStatus(tmp[i]));
  }
  hdfsFreeFileInfo(tmp, numEntries);
}

void HdfsFileSystemImpl::readFile(UriPtr uri, std::string& out)
{
  if (boost::algorithm::iequals("hdfs", uri->getScheme())) {
    std::stringstream sstr;
    hdfs_file_traits::file_type hdfsFile = 
      hdfs_file_traits::open_for_read(uri->toString().c_str(),
				      0,
				      std::numeric_limits<uint64_t>::max());
    while(!hdfs_file_traits::isEOF(hdfsFile)) {
      uint8_t buf[4096];
      int32_t sz = hdfs_file_traits::read(hdfsFile, &buf[0], 4096);
      sstr.write((const char *) &buf[0], sz);
    }
    out = sstr.str();
  } else {
    throw std::runtime_error((boost::format("Invalid HDFS URL %1%") %
			      uri->toString()).str());
  }
}

class HdfsFileSystemRegistrar
{
public:
  HdfsFileSystemRegistrar();
  static FileSystem * create(UriPtr uri);
};

HdfsFileSystemRegistrar::HdfsFileSystemRegistrar()
{
  FileSystemFactory & factory(FileSystemFactory::get());
  factory.registerCreator("hdfs", &create);
}

FileSystem * HdfsFileSystemRegistrar::create(UriPtr uri)
{
  return new HdfsFileSystem(uri);
}

// Static to register file system
static HdfsFileSystemRegistrar registrar;

HdfsFileSystem::HdfsFileSystem(const std::string& uri)
  :
  mImpl(HdfsFileSystemImpl::get(URI::get(uri.c_str())))
{
  mUri = mImpl->transformDefaultUri(Path::get(uri));
}

HdfsFileSystem::HdfsFileSystem(UriPtr uri)
  :
  mImpl(HdfsFileSystemImpl::get(uri))
{
  mUri = mImpl->transformDefaultUri(Path::get(uri));
}

HdfsFileSystem::~HdfsFileSystem()
{
}
  
void HdfsFileSystem::expand(std::string pattern,
			      int32_t numPartitions,
			      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
  hdfs_file_traits::expand(pattern, numPartitions, files);
}

PathPtr HdfsFileSystem::getRoot()
{
  return mUri;
}

boost::shared_ptr<FileStatus> HdfsFileSystem::getStatus(PathPtr p)
{
  return mImpl->getStatus(p);
}

bool HdfsFileSystem::exists(PathPtr p)
{
  return mImpl->exists(p);
}

bool HdfsFileSystem::removeAll(PathPtr p)
{
  return mImpl->removeAll(p);
}

bool HdfsFileSystem::remove(PathPtr p)
{
  return mImpl->remove(p);
}

void HdfsFileSystem::list(PathPtr p,
			    std::vector<boost::shared_ptr<FileStatus> >& result)
{
  mImpl->list(p, result);
}

void HdfsFileSystem::readFile(UriPtr uri, std::string& out)
{
  mImpl->readFile(uri, out);
}

bool HdfsFileSystem::rename(PathPtr from, PathPtr to)
{
  int32_t ret;
  mImpl->rename(from, to, ret);
  return ret == 0;
}

HdfsDelete::HdfsDelete(const std::string& path)
{
  URI uri(path.c_str());
  if (!boost::algorithm::iequals(uri.getScheme(), "hdfs")) {
    throw std::runtime_error((boost::format("HdfsDelete::HdfsDelete path "
					    "argument %1% must be a valid HDFS"
					    " URL") % path).str());
  }
  mPath = path;
}

HdfsDelete::~HdfsDelete()
{
  if (mPath.size()) {
    PathPtr p = Path::get(mPath);
    AutoFileSystem fs (p->getUri());
    fs->removeAll(p);
  }
}

class HdfsWritableFile : public WritableFile
{
private:
  HdfsFileSystemImplPtr mFileSystem;
  hdfsFile mFile;
public:
  HdfsWritableFile(HdfsFileSystemImplPtr fileSystem,
		   hdfsFile f);
  ~HdfsWritableFile();
  bool close();
  bool flush();
  int32_t write(const uint8_t * buf, std::size_t len);
};

HdfsWritableFile::HdfsWritableFile(HdfsFileSystemImplPtr fileSystem,
				   hdfsFile f)
  :
  mFileSystem(fileSystem),
  mFile(f)
{
}

HdfsWritableFile::~HdfsWritableFile() 
{
  close();
}

bool HdfsWritableFile::close()
{
  int32_t ret = 0;
  if (mFile != NULL) {
    mFileSystem->close(mFile, ret);
    mFile = NULL;
  }
  return ret == 0;
}

bool HdfsWritableFile::flush()
{
  mFileSystem->flush(mFile);
  return true;
}

int32_t HdfsWritableFile::write(const uint8_t * buf, std::size_t len)
{
  return mFileSystem->write(mFile, buf, (int32_t) len);
}

HdfsWritableFileFactory::HdfsWritableFileFactory(UriPtr baseUri, 
						 int32_t bufferSize,
						 int32_t replicationFactor,
						 int32_t blockSize)
  :
  mFileSystem(NULL),
  mBufferSize(bufferSize),
  mReplicationFactor(replicationFactor),
  mBlockSize(blockSize)
{
  mFileSystem = (HdfsFileSystem *) HdfsFileSystemRegistrar::create(baseUri);
}

HdfsWritableFileFactory::~HdfsWritableFileFactory()
{
  delete mFileSystem;
}

HdfsFileSystem * HdfsWritableFileFactory::create(PathPtr p)
{
  return (HdfsFileSystem *) HdfsFileSystemRegistrar::create(p->getUri());
}

FileSystem * HdfsWritableFileFactory::getFileSystem()
{
  return mFileSystem;
}

WritableFile * HdfsWritableFileFactory::openForWrite(PathPtr p)
{
  hdfsFile f = mFileSystem->mImpl->open_for_write(p, mBufferSize, 
						  mReplicationFactor, 
						  mBlockSize);
  return new HdfsWritableFile(mFileSystem->mImpl, f);
}

bool HdfsWritableFileFactory::mkdir(PathPtr p)
{
  return mFileSystem->mImpl->mkdir(p);
}

static bool canBeSplit(boost::shared_ptr<FileStatus> file)
{
  // Gzip compressed files cannot be split.  Others can.
  return  !boost::algorithm::ends_with(file->getPath()->toString(), 
				       ".gz");
}

class hdfs_file_handle
{
public:
  HdfsFileSystemImplPtr FileSystem;
  hdfsFile File;
  uint64_t End;
  hdfs_file_handle()
    :
    File(NULL),
    End(std::numeric_limits<uint64_t>::max())
  {
  }

  static int32_t write(HdfsFileSystemImplPtr fs, hdfsFile f, const void * buf, int32_t sz)
  {
    fs->write(f, buf, sz);
  }
  static void flush(HdfsFileSystemImplPtr fs, hdfsFile f)
  {
    fs->flush(f);
  }
  static void close(HdfsFileSystemImplPtr fs, hdfsFile f)
  {
    fs->close(f);
  }
  static void close(HdfsFileSystemImplPtr fs, hdfsFile f, int& ret)
  {
    fs->close(f, ret);
  }
};

class PartitionCapacity {
public:
  std::size_t mPartition;
  std::size_t mCapacity;
  PartitionCapacity(std::size_t partition,
		    std::size_t capacity)
    :
    mPartition(partition),
    mCapacity(capacity)
  {
  }

  bool operator < (const PartitionCapacity& pc) const
  {
    return 
      mCapacity < pc.mCapacity || 
      (mCapacity==pc.mCapacity && mPartition < pc.mPartition);
  }
};

void hdfs_file_traits::expand(std::string pattern, 
			      int32_t numPartitions,
			      std::vector<std::vector<boost::shared_ptr<FileChunk> > >& files)
{
  files.resize(numPartitions);
  // TODO: Handle globbing 
  PathPtr path = Path::get(pattern.c_str());
  HdfsFileSystemImplPtr fs = HdfsFileSystemImpl::get(path->getUri());

  std::vector<boost::shared_ptr<FileStatus> > fileInfo;
  boost::shared_ptr<FileStatus> tmp = fs->getStatus(path);
  if (tmp->isDirectory()) {
    fs->list(path, fileInfo);
  } else {
    fileInfo.push_back(tmp);
  }

  // TODO: Should we sort the files on size????
  // Get size of all files.
  // TODO: Fix FileStatus to contain block size or else
  // make an HDFS specific method here...
  std::vector<uint64_t> cumulativeSizes;
  uint64_t totalBlocks=0;  
  std::size_t blockSize = 64*1024*1024;
  for(std::size_t i=0; i<fileInfo.size(); i++) {
    cumulativeSizes.push_back(fileInfo[i]->size() + (i==0 ? 0 : cumulativeSizes.back()));
    totalBlocks += (fileInfo[i]->size() + blockSize - 1)/blockSize;
  }
  uint64_t totalFileSize=cumulativeSizes.back();

  // Check that numPartitions agrees with JobServers
  // if ((std::size_t) numPartitions != PlanRunner::JobServers.size()) {
  //   throw std::runtime_error((boost::format("Mismatch between size of job server list=%1% and requested num partitions = %2%") % PlanRunner::JobServers.size() % numPartitions).str());
  // }

  // // This code is for a negative test: what happens if we
  // // use 0% local reads from HDFS.
  // std::vector<PartitionCapacity> q;
  // for(int32_t p = 0; p < numPartitions; ++p) {
  //   q.push_back(PartitionCapacity(p,0));
  // }
  // // Assign blocks to servers.
  // // Use greedy algorithm in which we figure out which servers have
  // // the fewest local blocks and assign those first.
  // std::size_t numAssigned = 0;
  // std::map<int, std::map<tOffset, bool> > blockAssignment;
  // for(int i=0; i<numEntries; i++) {
  //   char *** hosts = hdfsGetHosts(fs, 
  // 				  fileInfo[i].mName, 
  // 				  0, 
  // 				  fileInfo[i]->size());
  //   int blockIdx=0;
  //   tOffset offset=0;
  //   for(; offset<fileInfo[i]->size(); offset += fileInfo[i].mBlockSize, blockIdx++) {
  //     // Mark block as unassigned
  //     blockAssignment[i][offset] = false;

  //     // Pick the local server with smallest number
  //     // of assigned blocks.
  //     std::set<std::string> local;
  //     for(char ** blockHosts = hosts[blockIdx]; *blockHosts != NULL; ++blockHosts) {
  // 	local.insert(*blockHosts);
  //     }
  //     for(std::vector<PartitionCapacity>::iterator pcit = q.begin();
  // 	  pcit != q.end();
  // 	  ++pcit) {
  // 	if (local.end() ==
  // 	    local.find(PlanRunner::JobServers[pcit->mPartition])) {
  // 	  boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[i].mName,
  // 							   offset,
  // 							   offset+fileInfo[i].mBlockSize));
  // 	  // TODO: Choose the partition with fewest chunks.
  // 	  files[pcit->mPartition].push_back(chunk);
  // 	  pcit->mCapacity += 1;
  // 	  // Suboptimal.  This is test code!
  // 	  std::sort(q.begin(), q.end());
  // 	  numAssigned += 1;
  // 	  break;
  // 	} 
  //     }
  //   }
  // }  

  ///////////////////////////////////////////////////////////
  // The following code was used during benchmarking to
  // to implement load balancing across an HDFS file
  //////////////////////////////////////////////////////////
  // // Assign blocks to servers.
  // // Use greedy algorithm in which we figure out which servers have
  // // the fewest local blocks and assign those first.
  // std::map<int, std::map<tOffset, bool> > blockAssignment;
  // for(int i=0; i<numEntries; i++) {
  //   char *** hosts = hdfsGetHosts(fs, 
  // 				  fileInfo[i].mName, 
  // 				  0, 
  // 				  fileInfo[i]->size());
  //   int blockIdx=0;
  //   tOffset offset=0;
  //   for(; offset<fileInfo[i]->size(); offset += fileInfo[i].mBlockSize, blockIdx++) {
  //     // Mark block as unassigned
  //     blockAssignment[i][offset] = false;

  //     // Pick the local server with smallest number
  //     // of assigned blocks.
  //     std::size_t minHost = 0;
  //     std::size_t minHostLoad = std::numeric_limits<std::size_t>::max();

  //     for(char ** blockHosts = hosts[blockIdx]; *blockHosts != NULL; ++blockHosts) {
  // 	std::map<std::string, std::vector<std::size_t> >::const_iterator it = 
  // 	  PlanRunner::JobServerIndex.find(*blockHosts);
  // 	if(PlanRunner::JobServerIndex.end() != it) {
  // 	  for(std::vector<std::size_t>::const_iterator pit = it->second.begin();
  // 	      pit != it->second.end();
  // 	      ++pit) {
  // 	    if (files[*pit].size() < minHostLoad) {
  // 	      minHostLoad = files[*pit].size();
  // 	      minHost = *pit;
  // 	    }
  // 	  }
  // 	}  	
  //     }

  //     if (minHostLoad != std::numeric_limits<std::size_t>::max()) {
  // 	// Attach allocation to partition
  // 	boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[i].mName,
  // 							 offset,
  // 							 offset+fileInfo[i].mBlockSize));
  // 	// TODO: Choose the partition with fewest chunks.
  // 	files[minHost].push_back(chunk);
  // 	blockAssignment[i][offset] = true;
  //     }
  //   }
  //   hdfsFreeHosts(hosts);
  // }

  // // Take a pass through and assign all blocks that didn't have a local server
  // // TODO: It would be good to load balance here.
  // uint64_t numLocalBlocks=0;
  // uint64_t numRemoteBlocks=0;
  // for(std::map<int, std::map<tOffset, bool> >::const_iterator it = blockAssignment.begin();
  //     it != blockAssignment.end();
  //     ++it) {
  //   for(std::map<tOffset, bool>::const_iterator iit = it->second.begin();
  // 	iit != it->second.end();
  // 	++iit) {
  //     if (iit->second) {
  // 	numLocalBlocks +=1 ;
  // 	continue; 
  //     }
  //     numRemoteBlocks += 1;
  //     int i = it->first;
  //     tOffset offset = iit->first;
  //     boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[i].mName,
  // 						       offset,
  // 						       offset+fileInfo[i].mBlockSize));
  //     files[rand() % numPartitions].push_back(chunk);    
  //   }
  // }
  // std::cout << "HdfsRead with numLocalBlocks=" << numLocalBlocks << ", numRemoteBlocks=" << numRemoteBlocks << std::endl;
  // for(std::size_t i=0; i<files.size(); i++) {
  //   std::cout << PlanRunner::JobServers[i] << " assigned " << files[i].size() << " blocks\n";
  // }
  ///////////////////////////////////////////////////////////
  // End of benchmarking code
  //////////////////////////////////////////////////////////

  // Our goal is to assign even amounts to partitions.
  uint64_t partitionSize = (totalFileSize+numPartitions-1)/numPartitions;
  // Partition we are assigning files to.
  int32_t currentPartition=0;
  // Amount to try to allocate to the current partition.
  // Make sure that the last partition can hold whatever remains.
  uint64_t  currentPartitionRemaining = currentPartition+1 == numPartitions ? 
    std::numeric_limits<uint64_t>::max() :
    partitionSize;
  for(std::size_t currentFile=0; 
      currentFile<fileInfo.size(); 
      ++currentFile)  {
    // We don't want to assign a small part of a file to a partition however
    // This is an arbitrary number at this point; don't know if it is sensible.
    // It should probably be determined by the block size and the size of the file
    // itself.
    const uint64_t minimumFileAllocationSize = 1024*1024;
    uint64_t currentFileRemaining = fileInfo[currentFile]->size();
    while(currentFileRemaining) {
      uint64_t currentFilePosition = fileInfo[currentFile]->size() - currentFileRemaining;
      uint64_t fileAllocation = 0;
      bool splittable = canBeSplit(fileInfo[currentFile]);
      if (!splittable ||
  	  currentFileRemaining < currentPartitionRemaining ||
  	  currentFileRemaining-currentPartitionRemaining < minimumFileAllocationSize) {
  	// This file cannot be broken up (e.g. gzip compression).  Have no
  	// choice but to assign completely to 1 partition.
  	// or
  	// If what remains of the file is small before or after our target allocation,
  	// just go ahead and
  	// put the remainder of the file in this partition even if it overflows a bit.
  	fileAllocation = currentFileRemaining;
      } else {
  	// Fill up this partition.  
  	fileAllocation = currentPartitionRemaining;
      }
      // Attach allocation to partition
      boost::shared_ptr<FileChunk> chunk(new FileChunk(fileInfo[currentFile]->getPath()->getUri()->toString(),
  						       currentFilePosition,
						       splittable ?
  						       currentFilePosition+fileAllocation :
						       std::numeric_limits<uint64_t>::max()));
      files[currentPartition].push_back(chunk);
      // Update state of the partition and move on to next if necessary.
      // If what remains in the partition is "small" then close out the partition
      // even if it is not technically full.
      if(minimumFileAllocationSize <= currentPartitionRemaining &&
	 fileAllocation <= currentPartitionRemaining-minimumFileAllocationSize) {
  	currentPartitionRemaining -= fileAllocation;
      } else {
  	currentPartition += 1;
  	// Make sure that the last partition can hold whatever remains.
  	currentPartitionRemaining = currentPartition+1 == numPartitions ? 
  	  std::numeric_limits<uint64_t>::max() :
  	  partitionSize;
      }
      // Anymore file to be allocated?
      currentFileRemaining -= fileAllocation;	
    }
  }
}

hdfs_file_traits::file_type hdfs_file_traits::open_for_read(const char * filename, 
							    uint64_t beginOffset,
							    uint64_t endOffset)
{
  PathPtr path = Path::get(filename);

  file_type f = new hdfs_file_handle();

  // Connect to the file system
  f->FileSystem = HdfsFileSystemImpl::get(path->getUri());

  // Check and save the size of the file.
  f->End = f->FileSystem->getStatus(path)->size();

  // Open the file
  f->File = f->FileSystem->open_for_read(path);
    
  // Seek to appropriate offset.
  f->FileSystem->seek(f->File, beginOffset);
    
  return f;
} 

void hdfs_file_traits::close(hdfs_file_traits::file_type f)
{
  f->FileSystem->close(f->File);
  // Don't close since we have the caching hack.
  //hdfsDisconnect(f->FileSystem);
}

int32_t hdfs_file_traits::read(hdfs_file_traits::file_type f, uint8_t * buf, int32_t bufSize)
{  
  return f->FileSystem->read(f->File, buf, bufSize);
}

bool hdfs_file_traits::isEOF(hdfs_file_traits::file_type f)
{
  uint64_t filePos = f->FileSystem->tell(f->File);
  if (filePos >= f->End) 
    return true;
  else
    return false;
}

class HdfsDataBlockRegistrar
{
public:
  HdfsDataBlockRegistrar();
  static DataBlock * create(const char * filename,
			    int32_t targetBlockSize,
			    uint64_t begin,
			    uint64_t end);
};

HdfsDataBlockRegistrar::HdfsDataBlockRegistrar()
{
  DataBlockFactory & factory(DataBlockFactory::get());
  factory.registerCreator("hdfs", &create);
}

DataBlock * HdfsDataBlockRegistrar::create(const char * filename,
					    int32_t targetBlockSize,
					    uint64_t begin,
					    uint64_t end)
{
  typedef BlockBufferStream<hdfs_file_traits> hdfs_block;
  typedef BlockBufferStream<zlib_file_traits<hdfs_block> > zlib_hdfs_block;
  URI uri(filename);
  bool compressed = uri.getPath().size() > 3 &&
    boost::algorithm::iequals(".gz", 
			      uri.getPath().substr(uri.getPath().size()-3));
  if (compressed) {
    return new zlib_hdfs_block(filename, targetBlockSize, begin, end);
  } else {
    return new hdfs_block(filename, targetBlockSize, begin, end);
  }
}

// Static to register file system
static HdfsDataBlockRegistrar dataBlockRegistrar;

LogicalEmit::LogicalEmit()
  :
  mPartitioner(NULL)
{
}

LogicalEmit::~LogicalEmit()
{
  delete mPartitioner;
}

void LogicalEmit::check(PlanCheckContext& log)
{
  // Validate the parameters
  std::string partitioner;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (it->equals("key")) {
      mKey = getStringValue(log, *it);
    } else if (it->equals("partition")) {
      partitioner = getStringValue(log, *it);
    } else {
      checkDefaultParam(*it);
    }
  }

  if (!getInput(0)->getRecordType()->hasMember(mKey)) {
    log.logError(*this, std::string("Missing field: ") + mKey);
  }

  // if partitioner set, validate/compile
  if (partitioner.size()) {
    std::vector<RecordMember> emptyMembers;
    RecordType emptyTy(emptyMembers);
    std::vector<const RecordType *> tableOnly;
    tableOnly.push_back(getInput(0)->getRecordType());
    tableOnly.push_back(&emptyTy);
    mPartitioner = new RecordTypeFunction(log, 
					  "partitioner",
					  tableOnly, 
					  partitioner);
    std::cout << "Setting partitioner " << partitioner.c_str() << std::endl;
  }
}

void LogicalEmit::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeHadoopEmitOperatorType("RuntimeHadoopEmitOperatorType",
				      getInput(0)->getRecordType(),
				      mKey, mPartitioner);
  
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
}

RuntimeHadoopEmitOperator::RuntimeHadoopEmitOperator(RuntimeOperator::Services& services, const RuntimeOperatorType& opType)
  :
  RuntimeOperator(services, opType),
  mState(START),
  mContext(NULL),
  mKeyPrinter(getHadoopEmitType().mKey),
  mValuePrinter(getHadoopEmitType().mPrint),
  mRuntimeContext(new InterpreterContext())
{
}

RuntimeHadoopEmitOperator::~RuntimeHadoopEmitOperator()
{
  delete mRuntimeContext;
}

void RuntimeHadoopEmitOperator::start()
{
  // Somebody needs to set the context!
  if (mContext == NULL) {
    throw std::runtime_error("Must set RuntimeHadoopEmitOperator::mContext");
  }
  mState = START;
  onEvent(NULL);
}

void RuntimeHadoopEmitOperator::writeToHdfs(RecordBuffer input, bool isEOS)
{
  if (!isEOS) {
    mValuePrinter.print(input, false);
    mKeyPrinter.print(input, false);
    mContext->emit(mKeyPrinter.str(), mValuePrinter.str());
    mKeyPrinter.clear();
    mValuePrinter.clear();
    getHadoopEmitType().mFree.free(input);
  } 
}

void RuntimeHadoopEmitOperator::onEvent(RuntimePort * port)
{
  switch(mState) {
  case START:
    while(true) {
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	read(port, mInput);
	bool isEOS = RecordBuffer::isEOS(mInput);
	writeToHdfs(mInput, isEOS);
	if (isEOS) {
	  break;
	}
      }
    }
  }
}

void RuntimeHadoopEmitOperator::shutdown()
{
}

bool RuntimeHadoopEmitOperator::hasPartitioner()
{
  return getHadoopEmitType().mPartitioner != NULL;
}

uint32_t RuntimeHadoopEmitOperator::partition(const std::string& key, 
					     uint32_t numPartitions)
{
  // Based on code inspection we should be called as a callback
  // from within the emit.  Therefore we can ignore the key
  // and just calculate the partition from the input record.  This saves
  // us from having to reparse the key.
  return (uint32_t)getHadoopEmitType().mPartitioner->execute(mInput, RecordBuffer(),
							     mRuntimeContext) % numPartitions;
}

RuntimeHadoopEmitOperatorType::~RuntimeHadoopEmitOperatorType()
{
  delete mPartitioner;
}

RuntimeOperator * RuntimeHadoopEmitOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeHadoopEmitOperator(services, *this);
}

