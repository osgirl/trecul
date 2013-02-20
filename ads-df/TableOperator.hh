/**
 * Copyright (c) 2013, Akamai Technologies
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

#ifndef __TABLEOPERATOR_HH__
#define __TABLEOPERATOR_HH__

#include "LogicalOperator.hh"

class SerialOrganizedTable;

class LogicalTableParser : public LogicalOperator
{  
private:
  // The table I am reading.
  std::string mTable;
  // If configured, this is a single file input
  std::string mFile;
  // TODO: Support full transfer semantics and
  // infer referenced columns from the transfer spec.
  // Right now this is a column list.
  std::vector<std::string> mReferenced;
  // If reading the "table" format, this is the "database"
  // we are reading from.  It should be a URL that points
  // to the base path for tables.  It can be either a file
  // or hdfs URL.
  std::string mFileSystem;
  // Common Version from the database (serial count).
  int32_t mCommonVersion;
  // Major Version of the table.
  int32_t mMajorVersion;
  // When reading from file system this is an optional predicate
  // that will prune the directories we read from.
  std::string mPredicate;

  boost::shared_ptr<const class TableMetadata> mTableMetadata;
  class TableOutput * mTableOutput;

  // The format of the underlying table.
  // May not be the same as the output format
  // if we are applying a select/output list.
  const RecordType * mTableFormat;
  // Paths that we'll be reading.
  SerialOrganizedTable * mSOT;

  // Every path that we read for a table has to be associated
  // with a file metadata object that describes the contents of
  // the file.  In a database that stuff is managed explicitly in
  // the system catalog.  In our case the file system itself is the
  // only persistent storage so we calculate the mapping between the
  // path and the metadata (assuming that the information is encoded
  // in the path somehow.

public:
  LogicalTableParser(const std::string& table);
  LogicalTableParser(const std::string& table,
		     boost::shared_ptr<const class TableMetadata> tableMetadata);
  ~LogicalTableParser();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

#endif
