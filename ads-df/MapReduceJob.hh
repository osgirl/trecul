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

#ifndef __MAPREDUCEJOB_H
#define __MAPREDUCEJOB_H

#include <string>
#include <boost/filesystem/path.hpp>

/**
 * Interface into the Hadoop installation.
 */
class HadoopSetup
{
public:
  /**
   * Get the Hadoop installation directory.
   */
  static std::string hadoopHome();
  /**
   * Set appropriate environment variables required for Hadoop
   * and HDFS functionality (e.g. CLASSPATH).
   */
  static void setEnvironment();
};

class AdsDfSpeculativeExecution
{
public:
  enum Type { BOTH, NONE, MAP, REDUCE };
private:
  Type mType;
public:
  AdsDfSpeculativeExecution();
  AdsDfSpeculativeExecution(const std::string& str);
  const char * isMapEnabledString() const;
  const char * isReduceEnabledString() const;
};

class AdsPipesJobConf
{
public:
  static const int32_t DEFAULT_TASK_TIMEOUT;
private:
  std::string mJobDir;
  std::string mMapper;
  std::string mReducer;
  std::string mName;
  std::string mJobQueue;
  int32_t mNumReducers;
  bool mJvmReuse;
  boost::filesystem::path mLocalPipesPath;
  std::string mLocalPipesChecksum;
  AdsDfSpeculativeExecution mSpeculative;
  int32_t mTaskTimeout;
  bool mNeedHdfsJobDir;


  /**
   * Name of pipes executable in HDFS.  It goes into the
   * distributed cache from here.
   */
  std::string getPipesExecutableName() const;
  /**
   * Name of map plan in HDFS.  It goes into the
   * distributed cache from here.
   */
  std::string getMapPlanFileName() const;
  /**
   * Name of reduce plan in HDFS.  It goes into the
   * distributed cache from here.
   */
  std::string getReducePlanFileName() const;
  /**
   * Copy plan from local file system to HDFS.
   */
  void copyPlanFileToHDFS(const std::string& plan,
			  const std::string& planFileName);
  /**
   * Copy ads-df-pipes from local file system to HDFS.
   */
  void copyPipesExecutableToHDFS();

  /**
   * MD5 checksum of ads-df-pipes executable on local disk.
   */
  static std::string getPipesExecutableChecksum(const boost::filesystem::path & p);

public:
  AdsPipesJobConf(const std::string& jobDir);
  void setMapper(const std::string& m);
  void setReducer(const std::string& r, int32_t numReducers);
  void setName(const std::string& name);
  void setNumReducers(int32_t numReducers);
  void setJvmReuse(bool jvmReuse);
  void setJobQueue(const std::string& jobQueue);
  void setSpeculativeExecution(AdsDfSpeculativeExecution s);
  void setTaskTimeout(int32_t timeout);
  std::string get() const;
  void copyFilesToHDFS();
  static int32_t copyFromLocal(const std::string& localPath,
			       const std::string& remotePath);
};

class MapReducePlanRunner
{
private:
  static void createSerialized64MapPlan(const std::string& f,
					int32_t partitions,
					std::string& emitFormat,
					std::string& plan);
  static void createSerialized64ReducePlan(const std::string& f,
					   int32_t partitions,
					   const std::string& defaultReduceFormat,
					   std::string& plan);
public:
  static int runMapReduceJob(const std::string& mapProgram,
			     const std::string& reduceProgram,
			     const std::string& inputDir,
			     const std::string& outputDir,
			     bool useHp);
  static int runMapReduceJob(const std::string& mapProgram,
			     const std::string& reduceProgram,
			     const std::string& inputDir,
			     const std::string& outputDir,
			     int32_t numReduces,
			     bool jvmReuse,
			     bool useHp);
  static int runMapReduceJob(const std::string& mapProgram,
			     const std::string& reduceProgram,
			     const std::string& name,
			     const std::string& jobQueue,
			     const std::string& inputDir,
			     const std::string& outputDir,
			     int32_t numReduces,
			     bool jvmReuse,
			     bool useHp,
			     AdsDfSpeculativeExecution speculative,
			     int32_t timeout);
};

#endif
