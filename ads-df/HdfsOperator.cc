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
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include "hdfs.h"
#include "Pipes.hh"
#include "HdfsOperator.hh"
#include "RecordParser.hh"

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
    p = transformDefaultUri(p);    
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
    p = transformDefaultUri(p);
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
  int ret = ::hdfsDelete(mFileSystem, p->toString().c_str(), 1);	    
  return ret==0;
}

bool HdfsFileSystemImpl::remove(PathPtr p)
{
  p = transformDefaultUri(p);
  int ret = ::hdfsDelete(mFileSystem, p->toString().c_str(), 0);	    
  return ret==0;
}

void HdfsFileSystemImpl::list(PathPtr p,
			      std::vector<boost::shared_ptr<FileStatus> >& result)
{
  int numEntries=-1;
  p = transformDefaultUri(p);
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

void HdfsFileSystem::list(PathPtr p,
			    std::vector<boost::shared_ptr<FileStatus> >& result)
{
  mImpl->list(p, result);
}

void HdfsFileSystem::readFile(UriPtr uri, std::string& out)
{
  mImpl->readFile(uri, out);
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
    HdfsFileSystem fs (mPath);
    fs.removeAll(Path::get(mPath));
  }
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
	      HdfsFileSystemImplPtr fileSystem);
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
  HdfsFileSystemImplPtr mFileSystem;
public:
  HdfsFileRename(PathPtr from,
		 PathPtr to,
		 HdfsFileSystemImplPtr fileSystem);
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
			       HdfsFileSystemImplPtr fileSystem)
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
			       HdfsFileSystemImplPtr fileSystem)
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
    int32_t ret;
    mFileSystem->rename(mFrom, mTo, ret);
    if (ret != 0) {
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
  hdfsFile File;
  ZLibCompress Compressor;
  OutputFile(hdfsFile f) 
    :
    File(f)
  {
  }
  void flush(HdfsFileSystemImplPtr fileSystem)
  {
    // Flush data through the compressor.
    this->Compressor.put(NULL, 0, true);
    while(true) {
      this->Compressor.run();
      uint8_t * output;
      std::size_t outputLen;
      this->Compressor.consumeOutput(output, outputLen);
      if (outputLen > 0) {
	fileSystem->write(this->File, output, outputLen);
      } else {
	break;
      }
    }    
    // Flush data to disk
    fileSystem->flush(this->File);
  }
  void close(HdfsFileSystemImplPtr fileSystem) {
    // Clean close of file and file system
    fileSystem->close(this->File);
  }
};

/**
 * This policy supports keeping multiple files open
 * for writes and a close policy that defers to the file
 * committer.
 */
class MultiFileCreation
{
private:
  const MultiFileCreationPolicy& mPolicy;
  PathPtr mRootUri;
  HdfsFileSystemImplPtr mFileSystem;
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
		    int32_t partition);
  ~MultiFileCreation();
  void start(HdfsFileSystemImplPtr fileSystem, PathPtr rootUri,
	     class RuntimeHdfsWriteOperator * fileFactory);
  OutputFile * onRecord(RecordBuffer input,
			class RuntimeHdfsWriteOperator * fileFactory);
  void close(bool flush);
  void commit(std::string& err);
};

class RuntimeHdfsWriteOperator : public RuntimeOperatorBase<RuntimeHdfsWriteOperatorType>
{
public:
  void writeToHdfs(RecordBuffer input, bool isEOS);  
private:
  enum State { START, READ };
  State mState;

  PathPtr mRootUri;
  HdfsFileSystemImplPtr mFileSystem;
  RuntimePrinter mPrinter;
  AsyncWriter<RuntimeHdfsWriteOperator> mWriter;
  boost::thread * mWriterThread;
  HdfsFileCommitter * mCommitter;
  std::string mError;
  MultiFileCreation * mCreationPolicy;

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

MultiFileCreation * MultiFileCreationPolicy::create(int32_t partition) const
{
  return new MultiFileCreation(*this, partition);
}

MultiFileCreation::MultiFileCreation(const MultiFileCreationPolicy& policy,
				     int32_t partition)
  :
  mPolicy(policy),
  mRuntimeContext(NULL),
  mCommitter(NULL),
  mPartition(partition)
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
      int ret;
      mFileSystem->close(it->second->File, ret);
      it->second->File = NULL;
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
		    mFileSystem);
  // Call back to the factory to actually create the file
  OutputFile * of = factory->createFile(tempPath);
  add(filePath, of);
  return of;
}

void MultiFileCreation::start(HdfsFileSystemImplPtr fileSystem, 
			      PathPtr rootUri,
			      RuntimeHdfsWriteOperator * factory)
{
  mFileSystem = fileSystem;
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
	it->second->flush(mFileSystem);
      }
      it->second->close(mFileSystem);
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

RuntimeHdfsWriteOperator::RuntimeHdfsWriteOperator(RuntimeOperator::Services& services, 
						   const RuntimeHdfsWriteOperatorType& opType)
  :
  RuntimeOperatorBase<RuntimeHdfsWriteOperatorType>(services, opType),
  mState(START),
  mPrinter(getMyOperatorType().mPrint),
  mWriter(*this),
  mWriterThread(NULL),
  mCommitter(NULL),
  mCreationPolicy(opType.mCreationPolicy->create(getPartition()))
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
  if (mFileSystem) {
    // Since we are not calling hdfsConnectNewInstance
    // we are sharing references to the same underlying
    // Java class instance.  Explicitly closing will
    // mess up other instances floating around.
    // hdfsDisconnect(mFileSystem);
    mFileSystem = HdfsFileSystemImplPtr();
  }
}

OutputFile * RuntimeHdfsWriteOperator::createFile(PathPtr filePath)
{
  // TODO: Check if file exists
  // TODO: Make sure file is cleaned up in case of failure.
  hdfsFile f = mFileSystem->open_for_write(filePath,
					   getMyOperatorType().mBufferSize,
					   getMyOperatorType().mReplicationFactor,
					   /*getMyOperatorType().mBlockSize*/2000000000);
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
      mFileSystem->write(of->File, output, outputLen);
    }
  } 
  return of;
}

void RuntimeHdfsWriteOperator::start()
{
  // Connect to the file system
  // TODO: Convert write operator to use a URI
  std::string uriStr = (boost::format("hdfs://%1%:%2%/") %
			getMyOperatorType().mHdfsHost %
			getMyOperatorType().mHdfsPort).str();
  mRootUri = Path::get(uriStr);
  mFileSystem = HdfsFileSystemImpl::get(mRootUri->getUri());
  
  mCreationPolicy->start(mFileSystem, mRootUri, this);

  if (hasHeaderFile()) {
    PathPtr headerPath = Path::get(getMyOperatorType().mHeaderFile);
    std::string tmpHeaderStr = FileSystem::getTempFileName();
    PathPtr tmpHeaderPath = Path::get(mRootUri, 
				      "/tmp/headers/" + tmpHeaderStr);
    if (mCommitter == NULL) {
      mCommitter = HdfsFileCommitter::get();
    }
    mCommitter->track(tmpHeaderPath, headerPath, mFileSystem);
    hdfsFile headerFile = 
      mFileSystem->open_for_write(tmpHeaderPath,
				  getMyOperatorType().mBufferSize,
				  getMyOperatorType().mReplicationFactor,
				  getMyOperatorType().mBlockSize);
    if (headerFile == NULL) {
      throw std::runtime_error("Couldn't create header file");
    }
    mFileSystem->write(headerFile, 
		       &getMyOperatorType().mHeader[0], 
		       (tSize) getMyOperatorType().mHeader.size());
    mFileSystem->close(headerFile);
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
  // Since we are not calling hdfsConnectNewInstance
  // we are sharing references to the same underlying
  // Java class instance.  Explicitly closing will
  // mess up other instances floating around.
  // hdfsDisconnect(mFileSystem);
  mFileSystem = HdfsFileSystemImplPtr();
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
      mFileSystem->write(of->File, output, outputLen);
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
      requestRead(0);
      mState = READ;
      return;
    case READ:
      {
	RecordBuffer input;
	read(port, input);
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
      }
    }
  }
}

void RuntimeHdfsWriteOperator::shutdown()
{
  mCreationPolicy->close(false);
  if (mFileSystem) {
    // Since we are not calling hdfsConnectNewInstance
    // we are sharing references to the same underlying
    // Java class instance.  Explicitly closing will
    // mess up other instances floating around.
    // hdfsDisconnect(mFileSystem);
    mFileSystem = HdfsFileSystemImplPtr();
  }
  if (mError.size() != 0) {
    throw std::runtime_error(mError);
  }
}

RuntimeHdfsWriteOperatorType::RuntimeHdfsWriteOperatorType(const std::string& opName,
							   const RecordType * ty, 
							   const std::string& hdfsHost, 
							   int32_t port, 
							   const std::string& hdfsFile,
							   const std::string& header,
							   const std::string& headerFile,
							   const RecordTypeTransfer * argTransfer,
							   int32_t bufferSize, 
							   int32_t replicationFactor, 
							   int32_t blockSize)
  :
  RuntimeOperatorType(opName.c_str()),
  mPrint(ty->getPrint()),
  mFree(ty->getFree()),
  mHdfsHost(hdfsHost),
  mHdfsPort(port),
  mBufferSize(bufferSize),
  mReplicationFactor(replicationFactor),
  mBlockSize(blockSize),
  mHeader(header),
  mHeaderFile(headerFile),
  mCreationPolicy(new MultiFileCreationPolicy(hdfsFile, argTransfer))
{
}

RuntimeHdfsWriteOperatorType::~RuntimeHdfsWriteOperatorType()
{
  delete mCreationPolicy;
}

RuntimeOperator * RuntimeHdfsWriteOperatorType::create(RuntimeOperator::Services& services) const
{
  return new RuntimeHdfsWriteOperator(services, *this);
}

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

