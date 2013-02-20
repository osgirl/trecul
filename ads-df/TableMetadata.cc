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

#include <stdexcept>
#include "FileSystem.hh"
#include "RecordType.hh"
#include "TableMetadata.hh"

TableFileMetadata::TableFileMetadata(const std::string& recordType,
				     const std::map<std::string, std::string>& computedColumns)
  :
  mRecordType(recordType),
  mComputedColumns(computedColumns)
{
}

const RecordType * TableFileMetadata::getRecordType(DynamicRecordContext & ctxt) const
{
  IQLRecordTypeBuilder bld(ctxt, mRecordType, false);
  return bld.getProduct();
}

const std::map<std::string, std::string>& TableFileMetadata::getComputedColumns() const
{
  return mComputedColumns;
}

TableMetadata::TableMetadata(const std::string& tableName,
			     const std::string& recordType,
			     const std::vector<std::string>& sortKeys)
  :
  mTableName(tableName),
  mRecordType(recordType),
  mSortKeys(sortKeys)
{
}

TableMetadata::TableMetadata(const std::string& tableName,
			     const std::string& recordType,
			     const std::vector<std::string>& sortKeys,
			     const std::vector<std::string>& primaryKey,
			     const std::string& version)
  :
  mTableName(tableName),
  mRecordType(recordType),
  mSortKeys(sortKeys),
  mPrimaryKey(primaryKey),
  mVersion(version)
{
  if (sortKeys.size() < primaryKey.size()) { 
    throw std::runtime_error("Primary key must be a prefix of sort keys");
  }
  for(std::size_t i = 0, e = sortKeys.size(); i<e; ++i) {
    if (primaryKey[i] != sortKeys[i]) {
      throw std::runtime_error("Primary key must be a prefix of sort keys");
    }
  }
  if (version.size() && 0 == primaryKey.size()) {
    throw std::runtime_error("Versioned table requires a primary key");
  }
}

TableMetadata::~TableMetadata()
{
  for(std::vector<TableColumnGroup*>::iterator it = mColumnGroups.begin();
      it != mColumnGroups.end();
      ++it) {
    delete *it;
  }
}

void TableMetadata::resolveMetadata(SerialOrganizedTableFilePtr file,
				    TableColumnGroup* & tableColumnGroup,
				    TableFileMetadata* & fileMetadata) const
{
  // HACK: The code is calling the second component
  // of the path the "Date" because that is what it
  // has been historically.  We are piggybacking on that
  // path component to name the column group.  This 
  // means that we cannot support date partitioned tables
  // with multiple column groups without some additional
  // work on metadata.
  const std::string& cgName = file->getDate();
  if (mColumnGroupNames.size() == 0) {
    tableColumnGroup = mColumnGroups[0];
  } else {
    std::map<std::string, TableColumnGroup*>::const_iterator it = 
      mColumnGroupNames.find(cgName);
    if (it == mColumnGroupNames.end()) {
      throw std::runtime_error((boost::format("Invalid path in file system: '%1%'; "
					      "contains unknown column group name: '%2%'")
				% file->getPath()->toString() % cgName).str());
    }
    tableColumnGroup = it->second;
  }

  int32_t minorVersion = file->getMinorVersion();
  TableColumnGroup::const_file_iterator f = tableColumnGroup->findFiles(minorVersion);
  if (f == tableColumnGroup->endFiles()) {
    throw std::runtime_error("Invalid serial table path");
  }
  fileMetadata = f->second;
}

TableColumnGroup * TableMetadata::addDefaultColumnGroup()
{
  BOOST_ASSERT(mColumnGroups.size() == 0);
  mColumnGroups.push_back(new TableColumnGroup(this, mRecordType, ""));
  return mColumnGroups.back();
}

TableColumnGroup * TableMetadata::addColumnGroup(const std::string& cgName,
						 const std::string& cgRecordType)
{
  if (mColumnGroupNames.find(cgName) != mColumnGroupNames.end()) {
    throw std::runtime_error("INTERNAL ERROR: duplicate column group name");
  }
  if (mColumnGroupNames.size() != mColumnGroups.size()) {
    throw std::runtime_error("INTERNAL ERROR: cannot use default column group "
			     "and named column groups in same table");
  }
  mColumnGroups.push_back(new TableColumnGroup(this, cgRecordType, cgName));
  mColumnGroupNames[cgName] = mColumnGroups.back();
  return mColumnGroups.back();
}

const RecordType * TableMetadata::getRecordType(DynamicRecordContext& ctxt) const
{
  IQLRecordTypeBuilder bld(ctxt, mRecordType, false);
  return bld.getProduct();
}

const std::vector<std::string>& TableMetadata::getSortKeys() const
{
  return mSortKeys;
}

const std::vector<std::string>& TableMetadata::getPrimaryKey() const
{
  return mPrimaryKey;
}

const std::string& TableMetadata::getVersion() const
{
  return mVersion;
}

void TableMetadata::addColumnGroupRequired(std::set<std::string>& s) const
{
  for(std::vector<std::string>::const_iterator pk = mPrimaryKey.begin(),
	e = mPrimaryKey.end(); pk != e; ++pk) {
    if (s.find(*pk) == s.end()) {
      s.insert(*pk);
    }
  }
  if (mVersion.size() && s.find(mVersion) == s.end()) {
    s.insert(mVersion);
  }    
}

TableColumnGroup::TableColumnGroup(const class TableMetadata * table,
				   const std::string& recordType,
				   const std::string& columnGroupName)
  :
  mTable(table),
  mRecordType(recordType),
  mName(columnGroupName)
{
  BOOST_ASSERT(mTable != NULL);
}

TableColumnGroup::~TableColumnGroup()
{
  for(file_iterator f = beginFiles(), e = endFiles(); 
      f != e; ++f) {
    delete f->second;
  }
}

const std::vector<std::string>& TableColumnGroup::getSortKeys() const
{
  return mTable->getSortKeys();
}

const RecordType * TableColumnGroup::getRecordType(DynamicRecordContext & ctxt) const
{
  IQLRecordTypeBuilder bld(ctxt, mRecordType, false);
  return bld.getProduct();
}

MetadataCatalog::MetadataCatalog()
{
}

MetadataCatalog::~MetadataCatalog()
{
}

MetadataCatalog::ptr_type
MetadataCatalog::find(const std::string& tableName) const
{
  map_type::const_iterator it = mCatalog.find(tableName);
  if (it == mCatalog.end()) return MetadataCatalog::ptr_type();
  return it->second;
}

