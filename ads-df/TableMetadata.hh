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

#ifndef __TABLEMETATDATA_HH__
#define __TABLEMETATDATA_HH__

#include <map>
#include <set>
#include <string>
#include <vector>

#include <boost/shared_ptr.hpp>

class DynamicRecordContext;
class RecordType;

typedef boost::shared_ptr<class SerialOrganizedTableFile> SerialOrganizedTableFilePtr;

/**
 * Tables in Trecul 
 */
/**
 * A description of the on disk format of a table file together with
 * computed column expressions.  As we update minor version, it may become
 * necessary to add computed column expressions to allow integration of prior
 * versions with the updated schema.
 */
class TableFileMetadata
{
public:
  typedef std::map<std::string, std::string> computed_columns_type;
  typedef std::map<std::string, std::string>::const_iterator computed_columns_iterator;
private:
  /**
   * Stored columns.
   */
  std::string mRecordType;
  /**
   * Computed columns.
   */
  std::map<std::string, std::string> mComputedColumns;
public:
  TableFileMetadata(const std::string& recordType,
		    const std::map<std::string, std::string>& computedColumns);
  const RecordType * getRecordType(DynamicRecordContext & ctxt) const;
  const std::string& getRecordType() const 
  {
    return mRecordType;
  }
  const std::map<std::string, std::string>& getComputedColumns() const;
};

/**
 * A version of a table comprises one or more files.  Each file contains a 
 * group of columns for the table; some columns are stored and others
 * are computed..  If there is a primary key and version then
 * those columns must appear in every file.
 */
class TableColumnGroup
{
public:
  typedef std::map<int32_t, TableFileMetadata*>::iterator file_iterator;
  typedef std::map<int32_t, TableFileMetadata*>::const_iterator const_file_iterator;
private:
  // Table I belong to
  const class TableMetadata * mTable;
  // RecordType of this column group
  std::string mRecordType;
  // Map from table metadata to a minor version.
  std::map<int32_t, TableFileMetadata*> mMinorVersions;
  // Name of this column group
  std::string mName;
public:
  TableColumnGroup(const class TableMetadata * table,
		   const std::string& columnGroupType,
		   const std::string& columnGroupName);
  ~TableColumnGroup();

  bool canPrune() const 
  {
    // TODO: Support logic that makes it safe to prune
    // column groups from selects.
    return false;
  }

  const class TableMetadata * getTable() const
  {
    BOOST_ASSERT(mTable != NULL);
    return mTable;
  }

  const RecordType * getRecordType(DynamicRecordContext & ctxt) const;

  const std::vector<std::string>& getSortKeys() const;
  const std::string& getName() const
  {
    return mName;
  }

  std::size_t getNumFiles() const 
  {
    return mMinorVersions.size();
  }
  void add(int32_t minorVersion, TableFileMetadata * m) 
  {
    mMinorVersions[minorVersion] = m;
  }
  const_file_iterator findFiles(int32_t minorVersion) const
  {
    return mMinorVersions.find(minorVersion);
  }
  const_file_iterator endFiles() const
  {
    return mMinorVersions.end();
  }
  file_iterator beginFiles()
  {
    return mMinorVersions.begin();
  }
  file_iterator endFiles()
  {
    return mMinorVersions.end();
  }
};

class TableMetadata
{
public:
  typedef std::vector<std::string>::const_iterator sort_key_const_iterator;
  typedef std::vector<TableColumnGroup*>::const_iterator column_group_const_iterator;
private:
  std::string mTableName;
  std::string mRecordType;
  std::vector<std::string> mSortKeys;
  // Optional primary key.  A table with multiple 
  // column groups must have a primary key and must be sorted
  // its primary key.  There is an implicit assumption (which we 
  // cannot validate) that the table is partitioned on a prefix
  // of its primary key.
  std::vector<std::string> mPrimaryKey;
  // Column containing the version of the record.
  // If the version column is not empty then there must be a 
  // primary key.
  std::string mVersion;
  // Column groups for the table.  Every column in the table
  // is associated with exactly one column group.
  // We do NOT support moving columns between column groups.
  std::vector<TableColumnGroup*> mColumnGroups;
  // If we have multiple column groups then they must be
  // named.  This is the map from name to column group.
  std::map<std::string, TableColumnGroup*> mColumnGroupNames;
public:
  TableMetadata(const std::string& tableName,
		const std::string& recordType,
		const std::vector<std::string>& sortKeys);

  TableMetadata(const std::string& tableName,
		const std::string& recordType,
		const std::vector<std::string>& sortKeys,
		const std::vector<std::string>& primaryKey,
		const std::string& version);

  ~TableMetadata();

  /**
   * Given a path into a serial organized table, identify
   * the coresponding metadata.
   */
  void resolveMetadata(SerialOrganizedTableFilePtr file,
		       TableColumnGroup* & tableColumnGroup,
		       TableFileMetadata* & fileMetadata) const;

  /**
   * Add a single column group with all of the table columns.
   */
  TableColumnGroup * addDefaultColumnGroup();

  /**
   * Add a named column group.   The name of the column group
   * corresponds to the second component in the file system
   * path (e.g. MinorVersion/CgName/Batch).
   */
  TableColumnGroup * addColumnGroup(const std::string& cgName,
				    const std::string& cgRecordType);

  column_group_const_iterator beginColumnGroups() const
  {
    return mColumnGroups.begin();
  }

  column_group_const_iterator endColumnGroups() const
  {
    return mColumnGroups.end();
  }

  /**
   * The offical table format of the table.
   */
  const RecordType * getRecordType(DynamicRecordContext& ctxt) const;
  /**
   * Is the data stored in a sorted order?
   */
  const std::vector<std::string>& getSortKeys() const;

  /**
   * Optional primary key for the table?
   */
  const std::vector<std::string>& getPrimaryKey() const;

  /**
   * Version column for the table.
   */
  const std::string& getVersion() const;

  /**
   * Add columns required for column group processing.
   */
  void addColumnGroupRequired(std::set<std::string>& s) const;
};

class MetadataCatalog
{
public:
  typedef boost::shared_ptr<const TableMetadata> ptr_type;
  typedef std::map<std::string, ptr_type> map_type;
private:
  map_type mCatalog;
public:
  MetadataCatalog();
  ~MetadataCatalog();
  ptr_type find(const std::string& tableName) const;
};

#endif
