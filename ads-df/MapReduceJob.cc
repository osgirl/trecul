/**
 * Copyright (c) 2015, Akamai Technologies
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

#include <unistd.h>
#include <fcntl.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>
#include <boost/make_shared.hpp>

#include "md5.h"
#include "FileSystem.hh"
#include "GraphBuilder.hh"
#include "HdfsOperator.hh"
#include "MapReduceJob.hh"
#include "RecordParser.hh"
#include "RuntimeProcess.hh"
#include "QueueImport.hh"

class TempFile
{
private:
  boost::filesystem::path mFileName;
public:
  TempFile(const std::string& contents);
  ~TempFile();
  std::string getName() const;
};

TempFile::TempFile(const std::string& contents)
{
  boost::filesystem::path tmpDir((boost::format("/ghostcache/hadoop/temp/%1%")
				  % ::getenv("USER")).str());
  // For backward compatibility try this if we don't have ghostcache set up
  if (!boost::filesystem::exists(tmpDir))
    tmpDir = boost::filesystem::path("/usr/local/akamai/tmp");

  // Retry in case we get a file name that collides
  for(int32_t i=0;i<2;i++) {
    boost::filesystem::path tmpStr(FileSystem::getTempFileName());
    boost::filesystem::path tmpPath(tmpDir / tmpStr);
    int fd = ::open(tmpPath.string().c_str(), 
		    O_CREAT | O_EXCL | O_WRONLY,
		    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (-1 == fd) {
      if (errno == EEXIST) {
	continue;
      } else {
	break;
      }
    }
    ssize_t sz = ::write(fd, contents.c_str(), contents.size());
    ::close(fd);
    if (sz != (ssize_t) contents.size()) {
      continue;
    }
    mFileName = tmpPath;
    return;
  }
  throw std::runtime_error((boost::format("Failed to create temporary file in "
					  "directory %1%") % 
			    tmpDir.string()).str());
}

TempFile::~TempFile()
{
  if (!mFileName.empty()) {
    boost::filesystem::remove(mFileName);
  }
}

std::string TempFile::getName() const
{
  return mFileName.string();
}

std::string HadoopSetup::hadoopHome()
{
  std::string hadoopHome("/usr/local/akamai/third-party/hadoop");
  if (getenv("HADOOP_HOME")) {
    hadoopHome = getenv("HADOOP_HOME");
  } 
  return hadoopHome;
}

void HadoopSetup::setEnvironment()
{
  std::string myHome(hadoopHome());  
  std::string myHadoopEnv(myHome + "/conf/hadoop-env.sh");
  std::string myHadoopConfig(myHome + "/bin/hadoop-config.sh");

  // Don't do anything if we don't have a valid Hadoop
  // install
  if (!boost::filesystem::exists(myHome) ||
      !boost::filesystem::is_directory(myHome) ||
      !boost::filesystem::exists(myHadoopEnv) ||
      !boost::filesystem::is_regular_file(myHadoopEnv) ||
      !boost::filesystem::exists(myHadoopConfig) ||
      !boost::filesystem::is_regular_file(myHadoopConfig))
    return;

  // If Hadoop home exists then grab environment variables set
  // up by conf scripts.
  PosixProcessFactory p;
  std::vector<boost::shared_ptr<PosixProcessInitializer> > v;
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixPath("/bin/bash")));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixArgument("-c")));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixArgument("source " + myHadoopEnv + ";\n"
									   "source " + myHadoopConfig + ";\n"
									   "echo \"CLASSPATH=$CLASSPATH\"")));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixParentEnvironment()));

  ProcessPipe stdOutPipe;
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new StandardOutTo(stdOutPipe)));
  p.create(v);
  boost::iostreams::stream<ProcessPipe::source_type> fromChild(stdOutPipe.mSource);

  // Grab the environment from stdout.
  typedef std::map<std::string,std::string> string_map;
  string_map envVars;
  std::string buf;
  while(std::getline(fromChild, buf)) {
    std::vector<std::string> s;
    boost::algorithm::split(s, buf, boost::algorithm::is_any_of("="), boost::algorithm::token_compress_on);
    envVars[s[0]] = s[1];
  }
  p.waitForCompletion();
  for(string_map::iterator it = envVars.begin();
      it != envVars.end();
      ++it) {
    ::setenv(it->first.c_str(), it->second.c_str(), 1);
  }
  
  ::setenv("LIBHDFS_OPTS", "-Xmx100m", 0);
}

AdsDfSpeculativeExecution::AdsDfSpeculativeExecution()
  :
  mType(BOTH)
{
}

AdsDfSpeculativeExecution::AdsDfSpeculativeExecution(const std::string& str)
  :
  mType(BOTH)
{
  std::string s = boost::algorithm::trim_copy(str);
  boost::algorithm::to_upper(s);
  if(boost::algorithm::equals(s, "BOTH")) {
    mType = BOTH;
  } else if(boost::algorithm::equals(s, "NONE")) {
    mType = NONE;
  } else if(boost::algorithm::equals(s, "MAP")) {
    mType = MAP;
  } else if(boost::algorithm::equals(s, "REDUCE")) {
    mType = REDUCE;
  } else {
    throw std::runtime_error("speculative execution must one of both|none|map|reduce");
  }
}
  

const char * AdsDfSpeculativeExecution::isMapEnabledString() const
{
  return mType == BOTH || mType == MAP ? "true" : "false";
}

const char * AdsDfSpeculativeExecution::isReduceEnabledString() const
{
  return mType == BOTH || mType == REDUCE ? "true" : "false";
}

/**
 * Run an ads-df dataflow using Hadoop pipes.
 *
 * An ads-df job is run in Hadoop using pipes.  This is
 * accomplished by having ads-df compile the dataflow plans
 * and pass them to a slave executable ads-df-pipes that 
 * runs the plans as a map reduce task.
 * 
 * To make upgrades of ads-df easy we have every version
 * of ads-df upload an appropriate version of ads-df-pipes into
 * Hadoop.  This makes it possible for multiple versions of
 * ads-df to coexist on a cluster.  At a minimum this is 
 * crucial while a cluster is being upgraded; without this
 * it would be possible to have a version mismatch between the
 * ads-df that serialized a dataflow plan and an ads-df-pipes
 * slave that deserializes and executes it.  Much easier than
 * demanding backward/forward compatibility is to just keep
 * these versions separate from one another.
 *
 * We identify versions of ads-df by taking a checksum of
 * the ads-df-pipes executable.  This has the advantage that
 * one doesn't have to manage arbitrary versioning schemes 
 * and that one is protected from incompatible changes during
 * a development cycle that don't correspond to a change in any
 * official release version number.  The big disadvantage of this
 * upload policy is that developer machines will accumulate a large
 * quantity of ads-df-pipes executable versions in HDFS and it will
 * be necessary to purge these on occasion.  An alternative that was
 * tried was just having ads-df always upload a private "one use
 * only" copy of ads-df-pipes to HDFS.  It appears that the massively
 * scalable Hadoop core is incapable of handling this much traffic
 * in its distributed cache as our entire cluster was crippled after
 * a couple of days with this upload policy.
 */
const int32_t AdsPipesJobConf::DEFAULT_TASK_TIMEOUT(600000);

AdsPipesJobConf::AdsPipesJobConf(const std::string& jobDir)
  :
  mJobDir(jobDir),
  mNumReducers(0),
  mJvmReuse(true),
  mTaskTimeout(DEFAULT_TASK_TIMEOUT),
  mNeedHdfsJobDir(false)
{
  // We assume that ads-df-pipes is in the same directory
  // as this exe
  mLocalPipesPath =
    Executable::getPath().parent_path()/boost::filesystem::path("ads-df-pipes");
  if (!boost::filesystem::exists(mLocalPipesPath))
    throw std::runtime_error((boost::format("Couldn't find ads-df-pipes "
					    "executable: %1%.  "
					    "Check installation") % mLocalPipesPath.string()).str());
  mLocalPipesChecksum = getPipesExecutableChecksum(mLocalPipesPath);
}

void AdsPipesJobConf::setMapper(const std::string& m)
{
  mMapper = m;
  mNeedHdfsJobDir = true;
}

void AdsPipesJobConf::setReducer(const std::string& r, int32_t numReducers)
{
  mReducer = r;
  mNumReducers = numReducers;
  mNeedHdfsJobDir = true;
}

void AdsPipesJobConf::setName(const std::string& name)
{
  mName = name;
}

void AdsPipesJobConf::setNumReducers(int32_t numReducers)
{
  mNumReducers = numReducers;
}

void AdsPipesJobConf::setJvmReuse(bool jvmReuse)
{
  mJvmReuse = jvmReuse;
}
 
void AdsPipesJobConf::setJobQueue(const std::string& jobQueue)
{
  mJobQueue = jobQueue;
}

void AdsPipesJobConf::setSpeculativeExecution(AdsDfSpeculativeExecution s)
{
  mSpeculative = s;
}

void AdsPipesJobConf::setTaskTimeout(int32_t timeout)
{
  mTaskTimeout = timeout;
}

std::string AdsPipesJobConf::get() const
{
  boost::format openBoilerPlateFormat(
			      "<?xml version=\"1.0\"?>\n"
			      "<configuration>\n"
			      "%2%"
			      "  <property>\n"
			      "    <name>hadoop.pipes.executable</name>\n"
			      "    <value>%1%</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>hadoop.pipes.java.recordreader</name>\n"
			      "    <value>true</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>hadoop.pipes.java.recordwriter</name>\n"
			      "    <value>true</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>mapred.job.shuffle.input.buffer.percent</name>\n"
			      "    <value>0.5</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>mapred.map.tasks.speculative.execution</name>\n"
			      "    <value>%3%</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>mapred.reduce.tasks.speculative.execution</name>\n"
			      "    <value>%4%</value>\n"
			      "  </property>\n"
			      "  <property>\n"
			      "    <name>mapred.task.timeout</name>\n"
			      "    <value>%5%</value>\n"
			      "  </property>\n"
			      );

  boost::format ldLibPath(
			  "  <property>\n"
			  "    <name>mapred.child.env</name>\n"
			  "    <value>LD_LIBRARY_PATH=%1%</value>\n"
			  "  </property>\n"
			  );
  boost::format mapperFormat(
			     "  <property>\n"
			     "    <name>com.akamai.ads.dataflow.mapper.plan</name>\n"
			     "    <value>%1%</value>\n"
			     "  </property>\n"
			     );
  boost::format numReducersFormat(
			      "  <property>\n"
			      "    <name>mapred.reduce.tasks</name>\n"
			      "    <value>%1%</value>\n"
			      "  </property>\n"
				  );
  boost::format reducerFormat(
			      "  <property>\n"
			      "    <name>com.akamai.ads.dataflow.reducer.plan</name>\n"
			      "    <value>%1%</value>\n"
			      "  </property>\n"
			      );
  boost::format jobName(
			"  <property>\n"
			"     <name>mapred.job.name</name>\n"
			"     <value>%1%</value>\n"
			"  </property>\n"
			);
  boost::format jobQueue(
			 "  <property>\n"
			 "     <name>mapred.job.queue.name</name>\n"
			 "     <value>%1%</value>\n"
			 "  </property>\n"
			 );
  // We used to put plans in the job conf and let them get pushed
  // out through the distributed cache by virtue of that. When we 
  // had "large" plans such as those generated from scoring runs
  // this caused memory pressure on various JVMs (despite the 
  // fact that we were only talking about 10MB or so).  To avoid
  // any such problems in the future we put plans into the 
  // distributed cache ourselves so that Java doesn't have to touch them
  // during JobConf processing.
  // Note that we set the symlink option so that Hadoop will create
  // links to these files in the working directory of each map
  // reduce task.  This is the only way to make the location of
  // these file predictable when one doesn't have access to the
  // distributed cache API (which pipes doesn't provide).
  boost::format distCacheFormat(
			     "  <property>\n"
			     "    <name>mapred.cache.files</name>\n"
			     "    <value>%1%</value>\n"
			     "  </property>\n"
			     "  <property>\n"
			     "    <name>mapred.create.symlink</name>\n"
			     "    <value>yes</value>\n"
			     "  </property>\n"
			     );

  std::string closeBoilerPlate(
			       "</configuration>\n"
			       );


  std::string jvmReuseProperty(mJvmReuse ?
			       "  <property>\n"
			       "    <name>mapred.job.reuse.jvm.num.tasks</name>\n"
			       "    <value>-1</value>\n"
			       "  </property>\n" :			       
			       "  <property>\n"
			       "    <name>mapred.job.reuse.jvm.num.tasks</name>\n"
			       "    <value>1</value>\n"
			       "  </property>\n" 
			       );
  std::string ret = (openBoilerPlateFormat % getPipesExecutableName() % jvmReuseProperty %
		     mSpeculative.isMapEnabledString() % mSpeculative.isReduceEnabledString() %
		     mTaskTimeout ).str();

  if (getenv("LD_LIBRARY_PATH")) {
    ret += (ldLibPath % getenv("LD_LIBRARY_PATH")).str();
  } 

  std::string distCacheFiles;
  ret += (jobName % mName).str();
  if (mJobQueue.size()) {
    ret += (jobQueue % mJobQueue).str();
  }
  // The URI fragment on dist cache names is used
  // to determine the name of the symlink that will 
  // created.  Pass the fragment in the job conf so
  // the map reduce tasks know how to find their plans.
  if (mMapper.size()) {
    const char * planFileLocal = "map.plan.txt";
    ret += (mapperFormat % planFileLocal).str();
    distCacheFiles += getMapPlanFileName();
    distCacheFiles += "#";
    distCacheFiles += planFileLocal;
  }
  ret += (numReducersFormat % mNumReducers).str();
  if (mReducer.size()) {
    const char * planFileLocal = "reduce.plan.txt";
    ret += (reducerFormat % planFileLocal).str();
    if (distCacheFiles.size()) {
      distCacheFiles += ",";
    }
    distCacheFiles += getReducePlanFileName();
    distCacheFiles += "#";
    distCacheFiles += planFileLocal;
  }
  if (distCacheFiles.size()) {
    ret += (distCacheFormat % distCacheFiles).str();
  }
  ret += closeBoilerPlate;

  return ret;
}

std::string AdsPipesJobConf::getPipesExecutableName() const
{
  return "/a/bin/ads-df-pipes-" + mLocalPipesChecksum;
}

std::string AdsPipesJobConf::getMapPlanFileName() const
{
  return mJobDir + "/plans/map.plan.txt";
}

std::string AdsPipesJobConf::getReducePlanFileName() const
{
  return mJobDir + "/plans/reduce.plan.txt";
}

int32_t AdsPipesJobConf::copyFromLocal(const std::string& localPath,
				       const std::string& remotePath)
{
  // Just shell out to hadoop to upload file
  typedef boost::shared_ptr<PosixProcessInitializer> ppiptr;
  PosixProcessFactory hadoopJob;
  std::vector<ppiptr> v;
  v.push_back(ppiptr(new PosixPath(HadoopSetup::hadoopHome() + "/bin/hadoop")));     
  v.push_back(ppiptr(new PosixArgument("fs")));
  v.push_back(ppiptr(new PosixArgument("-copyFromLocal")));
  v.push_back(ppiptr(new PosixArgument(localPath)));
  v.push_back(ppiptr(new PosixArgument(remotePath)));
  hadoopJob.create(v);
  return hadoopJob.waitForCompletion();
}

void AdsPipesJobConf::copyPlanFileToHDFS(const std::string& plan,
					 const std::string& planFileName)
{
  TempFile tmp(plan);
  int32_t ret = copyFromLocal(tmp.getName(), planFileName);
  if (ret != 0) {
    throw std::runtime_error((boost::format("Failed to upload plan "
					    "file from %1% to %2%") % 
			      tmp.getName() % planFileName).str());
  }
}

void AdsPipesJobConf::copyPipesExecutableToHDFS()
{
  // If file doesn't yet exist in HDFS, copy it up.
  HdfsFileSystem fs ("hdfs://default:0");
  std::string pipesPath=getPipesExecutableName();
  PathPtr pipesUri (Path::get(fs.getRoot(), pipesPath));
  if (fs.exists(pipesUri)) {
    return;
  }

  int32_t ret = copyFromLocal(mLocalPipesPath.string(),
			      pipesPath);
  // We might fail because another process has uploaded the file.
  // Only fail if the file doesn't exist.
  // We could retry some number of times (or not).
  if (ret != 0 && !fs.exists(pipesUri)) {
    throw std::runtime_error("Failed to upload pipes executable ads-df-pipes to HDFS");
  }
}

void AdsPipesJobConf::copyFilesToHDFS()
{
  HdfsFileSystem fs ("hdfs://default:0");
  PathPtr jobsDirUri (Path::get(fs.getRoot(), mJobDir + "/plans/"));

  copyPipesExecutableToHDFS();
  
  if( mNeedHdfsJobDir && ! fs.exists(jobsDirUri) ) {
    HdfsWritableFileFactory *factory = new HdfsWritableFileFactory(jobsDirUri->getUri());
    PathPtr createDir (Path::get(mJobDir + "/plans/"));
    if( ! factory->mkdir(createDir) ) {
      throw std::runtime_error("Failed to create job dir in HDFS");
    }
    delete factory;
  }

  if (mMapper.size()) {
    copyPlanFileToHDFS(mMapper, getMapPlanFileName());
  }
  if (mReducer.size()) {
    copyPlanFileToHDFS(mReducer, getReducePlanFileName());
  }
}

std::string AdsPipesJobConf::getPipesExecutableChecksum(const boost::filesystem::path & p)
{
  static const int32_t bufSz(4096);
  static const int32_t digestSz(16);
  uint8_t buf[bufSz];
  stdio_file_traits::file_type f = 
    stdio_file_traits::open_for_read(p.string().c_str(), 0, 
				     std::numeric_limits<uint64_t>::max());
  md5_byte_t md5digest[digestSz];
  md5_state_t md5sum;
  ::md5_init(&md5sum);
  int32_t numRead = 0;
  do {
    numRead = stdio_file_traits::read(f, &buf[0], bufSz);
    ::md5_append(&md5sum, (const md5_byte_t *) &buf[0], numRead);
  } while(numRead == bufSz);
  ::md5_finish(&md5sum, md5digest);
  stdio_file_traits::close(f);

  static const char hexDigits [] = {'0','1','2','3','4','5','6','7',
				    '8','9','a','b','c','d','e','f'};
  char output[2*digestSz];
  for(int32_t i=0; i<digestSz; i++) {
    output[2*i] = hexDigits[(md5digest[i]&0xf0) >> 4];
    output[2*i+1] = hexDigits[md5digest[i]&0x0f];
  }
  
  return std::string(&output[0], &output[32]);
}

void MapReducePlanRunner::createSerialized64MapPlan(const std::string& f,
						    int32_t partitions,
						    std::string& emitFormat,
						    std::string& p)
{
  PlanCheckContext ctxt;
  DataflowGraphBuilder gb(ctxt);
  gb.buildGraph(f);  
  boost::shared_ptr<RuntimeOperatorPlan> plan = gb.create(partitions);
  p = PlanGenerator::serialize64(plan);
  // Now that plan is checked we can scrape up the output
  // format of any emit operator so that it can be fed to
  // a reducer.
  std::vector<LogicalEmit *> emitOp;
  gb.getPlan().getOperatorOfType(emitOp);
  if (emitOp.size() > 1) {
    throw std::runtime_error("Must be at most one emit operator in a map plan.");
  }
  if (emitOp.size() == 1) {
    emitFormat = emitOp[0]->getStringFormat();
  }
}

void MapReducePlanRunner::createSerialized64ReducePlan(const std::string& f,
						       int32_t partitions,
						       const std::string& defaultReduceFormat,
						       std::string& p)
{
  PlanCheckContext ctxt;
  DataflowGraphBuilder gb(ctxt);
  gb.buildGraph(f);  
  if (defaultReduceFormat.size()) {
    std::vector<LogicalInputQueue *> reduceOp;
    gb.getPlan().getOperatorOfType(reduceOp);
    if (reduceOp.size() != 1) {
      throw std::runtime_error("Must be exactly one reduce operator in a reduce plan.");
    }
    if (reduceOp[0]->getStringFormat().size() == 0) {
      reduceOp[0]->setStringFormat(defaultReduceFormat);
    }
  }
  boost::shared_ptr<RuntimeOperatorPlan> plan = gb.create(partitions);
  p = PlanGenerator::serialize64(plan);
}

int MapReducePlanRunner::runMapReduceJob(const std::string& mapProgram,
					 const std::string& reduceProgram,
					 const std::string& inputDirArg,
					 const std::string& outputDirArg,
					 bool useHp)
{
  AdsDfSpeculativeExecution speculative;
  return runMapReduceJob(mapProgram, reduceProgram, "", "",
			 inputDirArg, outputDirArg,
			 reduceProgram.size() ? 1 : 0, true, useHp, speculative,
			 AdsPipesJobConf::DEFAULT_TASK_TIMEOUT);
}

int MapReducePlanRunner::runMapReduceJob(const std::string& mapProgram,
					 const std::string& reduceProgram,
					 const std::string& inputDirArg,
					 const std::string& outputDirArg,
					 int32_t numReduces,
					 bool jvmReuse,
					 bool useHp)
{
  AdsDfSpeculativeExecution speculative;
  return runMapReduceJob(mapProgram, reduceProgram, "", "", inputDirArg, 
			 outputDirArg, numReduces, jvmReuse, useHp, speculative, 
			 AdsPipesJobConf::DEFAULT_TASK_TIMEOUT);
}

int MapReducePlanRunner::runMapReduceJob(const std::string& mapProgram,
					 const std::string& reduceProgram,
					 const std::string& name,
					 const std::string& jobQueue,
					 const std::string& inputDirArg,
					 const std::string& outputDirArg,
					 int32_t numReduces,
					 bool jvmReuse,
					 bool useHp,
					 AdsDfSpeculativeExecution speculative,
					 int32_t timeout)
{
  // Create a temporary work space for this job
  boost::shared_ptr<HdfsDelete> cleanup;
  std::string jobDir = FileSystem::getTempFileName();
  jobDir = "/tmp/ads_df/" + jobDir;
  std::string uri("hdfs://default:0");
  uri += jobDir;
  cleanup = boost::make_shared<HdfsDelete>(uri);
  
  std::string inputDir;
  if (inputDirArg.size() == 0) {
    inputDir = "/1_2048/serials";
  } else {
    inputDir = inputDirArg;
  }
  std::string outputDir;
  if (outputDirArg.size() == 0) {
    outputDir = jobDir + "/output";
  } else {
    outputDir = outputDirArg;
  }

  // Set up job conf and fork/exec hadoop to run
  AdsPipesJobConf jobConf(jobDir);
  jobConf.setName(name);
  jobConf.setJobQueue(jobQueue);
  jobConf.setSpeculativeExecution(speculative);
  jobConf.setTaskTimeout(timeout);

  std::string mapBuf;
  std::string emitFormat;
  // TODO: Infer the number of serials from a conf file or from
  // the input directory?
  static const int32_t serials(2048);
  createSerialized64MapPlan(mapProgram, serials, emitFormat, mapBuf);
  jobConf.setMapper(mapBuf);
  if(reduceProgram.size()) {    
    std::string reduceBuf;
    createSerialized64ReducePlan(reduceProgram, numReduces, emitFormat, reduceBuf);
    jobConf.setReducer(reduceBuf,numReduces);
  }
  jobConf.setJvmReuse(jvmReuse);

  // Make sure we have all necessary files uploaded to HDFS
  jobConf.copyFilesToHDFS();

  // Create the job configuration temp file and run Hadoop against it.
  TempFile jobConfFile(jobConf.get());
  typedef boost::shared_ptr<PosixProcessInitializer> ppiptr;
  PosixProcessFactory hadoopJob;
  std::vector<ppiptr> v;
  if (!useHp) {
    v.push_back(ppiptr(new PosixPath(HadoopSetup::hadoopHome() + "/bin/hadoop")));
  } else {
    v.push_back(ppiptr(new PosixPath("/usr/local/akamai/bin/ads-hp-client")));       
    v.push_back(ppiptr(new PosixArgument("job")));
    v.push_back(ppiptr(new PosixArgument("hadoop")));
  }
  v.push_back(ppiptr(new PosixArgument("pipes")));
  v.push_back(ppiptr(new PosixArgument("-conf")));
  v.push_back(ppiptr(new PosixArgument(jobConfFile.getName())));
  v.push_back(ppiptr(new PosixArgument("-input")));
  v.push_back(ppiptr(new PosixArgument(inputDir)));
  v.push_back(ppiptr(new PosixArgument("-output")));
  v.push_back(ppiptr(new PosixArgument(outputDir)));
  v.push_back(ppiptr(new PosixParentEnvironment()));
  //TODO: Support some command line config of what to do with stdin/out/err.
  hadoopJob.create(v);
  return hadoopJob.waitForCompletion();
}

