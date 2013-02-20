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

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/format.hpp>
#include <boost/make_shared.hpp>
#include <boost/tokenizer.hpp>

#include "Merger.hh"
#include "RecordParser.hh"
#include "RuntimeOperator.hh"
#include "TableMetadata.hh"
#include "TableOperator.hh"

/**
 * The LogicalTable operator is a rather complicated macro
 * that expands into a collection of file readers, transformers,
 * filters and sort merges.  Building the subplan based on table
 * metadata is facilitated by a collection of auxilliary classes
 * that manage subtask of the overall plan generation.
 */

/**
 * PathOutput
 * This is a particular path in the file system with serials underneath.
 * The path is associated with a particular TableFileMetadata object that 
 * describes its contents.
 */
// class PathOutput
// {
// public:
//   // The actual path in the file system
//   SerialOrganizedTableFilePtr File;
// };

/**
 * Represents the processing on a particular
 * minor version of a column group.
 * There will likely be multiple paths to be processed
 * that have this format, but we only want to compile
 * the transfer a single time.
 */
class TableColumnGroupVersionOutput
{
public:
  TableColumnGroupVersionOutput(DynamicRecordContext & ctxt,
	     const class TableFileMetadata * pathMetadata,
	     const RecordType * opOutput);
  ~TableColumnGroupVersionOutput();
  // The format of the underlying file
  const RecordType * FileType;
  // The format of the file output operator
  // (e.g. take into consideration omitted columns).
  const RecordType * FileOutput;
  // Transfer from file output to column group output
  // (e.g. reordering of columns or computed columns).
  RecordTypeTransfer * Transfer;

  // The paths to process
  std::vector<SerialOrganizedTableFilePtr> Paths;
  typedef std::vector<SerialOrganizedTableFilePtr>::iterator path_iterator;
  path_iterator beginPaths()
  {
    return Paths.begin();
  }
  path_iterator endPaths()
  {
    return Paths.end();
  }
  void addPath(SerialOrganizedTableFilePtr file) 
  {
    Paths.push_back(file);
  }
};

/**
 * The underlying table is broken into one or more
 * groups of columns to support column store-like 
 * vertical partitioning of attributes.
 * This represents the output of a single column group
 * within the operator.  In the case that there are
 * multiple column groups they will be joined together
 * to form the final result.
 */
class ColumnGroupOutput
{
private:
  // The type of the column group (on disk; with all columns)
  const RecordType * mColumnGroupFormat;

  RecordTypeFunction * mKeyPrefixFun;
  RecordTypeFunction * mKeyLessThanFun;
  RecordTypeFunction * mPresortedEqFun;
  RecordTypeAggregate * mAggregate;
  RecordTypeFunction * mFilterFun;
  RecordTypeTransfer * mFinalTransfer;

  std::vector<SortKey> mPrimaryKeyWithSuffix;

public:
  // The output format of the column group in
  // the dataflow (e.g. representing projecting out
  // columns that are not needed).
  const RecordType * ColumnGroupType;
  // The Metadata describing the column group
  TableColumnGroup * Metadata;
  // If we are reading multiple files within the column group,
  // they must be sort merged.
  // Here are predicates for this.
  RecordTypeFunction * mKeyPrefix;
  RecordTypeFunction * mLessThan;
  RuntimeOperatorType * OutputOperator;

  // Mapping from minor versions we'll be reading to
  // their output specs (which may very well be identities).
  std::map<int32_t, TableColumnGroupVersionOutput *> mOutputs;

  ColumnGroupOutput(DynamicRecordContext& ctxt,
		    TableColumnGroup * metadata,
		    const std::set<std::string>& referenced);
  ~ColumnGroupOutput();

  void addPath(DynamicRecordContext& ctxt,
	       TableFileMetadata * fileMetadata,
	       SerialOrganizedTableFilePtr file);

  typedef std::map<int32_t, TableColumnGroupVersionOutput *>::iterator version_iterator;
  version_iterator beginVersions()
  {
    return mOutputs.begin();
  }
  version_iterator endVersions()
  {
    return mOutputs.end();
  }

  /**
   * Return comma delimited primary key string 
   * with column group name suffix.
   */
  const std::vector<SortKey>& getPrimaryKeyWithSuffix() const;

  /**
   * For a key value, add the column group name as a suffix.
   */
  std::string appendSuffix(const std::string& k) 
  {
    if (Metadata->getName().size()) {
      return (boost::format("%1%_%2%") % k % Metadata->getName()).str();
    } else {
      return k;
    }
  }

  // The output of the final column group operator.
  // This is the same as ColumnGroupType except for
  // necessary renaming to make primary key and version
  // column unique
  const RecordType * getColumnGroupTypeWithSuffix() const 
  {
    return mFinalTransfer->getTarget();
  }

  void check(PlanCheckContext & ctxt);
  RuntimeOperatorType * create(class RuntimePlanBuilder& plan,
			       RuntimeOperatorType * inputOp);
};

class TableOutput
{
private:
  // The column groups that will be read by the operator.
  // Note that with the current implementation of column groups
  // we must always read all column groups because we don't explicitly
  // store entries for every value of the primary key in every column group.
  // Obviously this breaks one of the basic benefits of column organization
  // but right now we are using column store methods to make updates easier
  // not so much for read performance.  If we knew that there is a column group
  // that does contain all values for primary keys then we could drop other
  // column groups safely in the read.
public:
  std::vector<ColumnGroupOutput*> mColumnGroups;
  std::map<TableColumnGroup*, ColumnGroupOutput*> mColumnGroupIndex;
  std::vector<RuntimeOperatorType *> mJoins;
  const TableMetadata * Metadata;
  // The actual format of the operator (e.g. including 
  // column selection).
  const RecordType * TableType;

  TableOutput(const TableMetadata * metadata,
	      const RecordType * tableType);
  ~TableOutput();
  ColumnGroupOutput * create(DynamicRecordContext& ctxt, 
			     TableColumnGroup* cg,
			     const std::set<std::string>& referenced);
  ColumnGroupOutput * find(TableColumnGroup* cg);
  void check(PlanCheckContext & ctxt);
};

TableColumnGroupVersionOutput::TableColumnGroupVersionOutput(DynamicRecordContext & ctxt,
		       const TableFileMetadata * fileMetadata,
		       const RecordType * ty)
  :
  FileType(NULL),
  FileOutput(NULL),
  Transfer(NULL)
{
  FileType = fileMetadata->getRecordType(ctxt);
  std::vector<std::string> referenced;
  for(RecordType::const_member_iterator outputMember = ty->begin_members();
      outputMember != ty->end_members();
      ++outputMember) {
    referenced.push_back(outputMember->GetName());
  }  
  FileOutput = RecordType::get(ctxt, FileType, referenced.begin(), referenced.end());
      
  // Create a transfer spec from minor version file output to operator output.
  std::string xfer;
  for(RecordType::const_member_iterator outputMember = ty->begin_members();
      outputMember != ty->end_members();
      ++outputMember) {
    if (xfer.size()) {
      xfer += ", ";
    }
    if (FileOutput->hasMember(outputMember->GetName())) {
      xfer += outputMember->GetName();
    } else {
      TableFileMetadata::computed_columns_iterator cc = 
	fileMetadata->getComputedColumns().find(outputMember->GetName());
      if (cc == fileMetadata->getComputedColumns().end()) {
	// For now let's make sure that metadata is explicit about forward compatibility
	// For NULL output columns we could always add a sensible default rule to
	// output a NULL even if it is not in metadata for the minor version.
	throw std::runtime_error((boost::format("Minor version of table(%2%) "
						"does not have column %1% defined.") %
				  outputMember->GetName() % 
				  fileMetadata->getRecordType()).str());
      }
      xfer += cc->second;
      xfer += " AS ";
      xfer += cc->first;
    }
  }
  Transfer = new RecordTypeTransfer(ctxt, 
				    "ImportMinorVersionXfer", 
				    FileOutput,
				    xfer);
}

TableColumnGroupVersionOutput::~TableColumnGroupVersionOutput()
{
  delete Transfer;
}

ColumnGroupOutput::ColumnGroupOutput(DynamicRecordContext& ctxt, 
				     TableColumnGroup * metadata,
				     const std::set<std::string>& referenced)
  :
  mKeyPrefixFun(NULL),
  mKeyLessThanFun(NULL),
  mPresortedEqFun(NULL),
  mAggregate(NULL),
  mFilterFun(NULL),
  mFinalTransfer(NULL),
  ColumnGroupType(NULL),
  Metadata(metadata),
  mKeyPrefix(NULL),
  mLessThan(NULL),
  OutputOperator(NULL)
{
  const std::vector<std::string> & pk(Metadata->getTable()->getPrimaryKey());
  for (std::vector<std::string>::const_iterator k = pk.begin(), 
	 kEnd = pk.end(); k != kEnd; ++k) {
    // TODO: allow primary key to have desc order.
    mPrimaryKeyWithSuffix.push_back(SortKey(appendSuffix(*k), SortKey::ASC));
  }
  mColumnGroupFormat = Metadata->getRecordType(ctxt);
  ColumnGroupType = RecordType::get(ctxt, mColumnGroupFormat,
				    referenced.begin(), referenced.end());
}

ColumnGroupOutput::~ColumnGroupOutput()
{
  for(std::map<int32_t, TableColumnGroupVersionOutput *>::iterator it=mOutputs.begin();
      it != mOutputs.end();
      ++it) {
    delete it->second;
  }

  delete mKeyPrefix;
  delete mLessThan;

  delete mKeyPrefixFun;
  delete mKeyLessThanFun;
  delete mPresortedEqFun;
  delete mAggregate;
  delete mFilterFun;
  delete mFinalTransfer;
}

void ColumnGroupOutput::addPath(DynamicRecordContext& ctxt,
				TableFileMetadata * fileMetadata,
				SerialOrganizedTableFilePtr file)
{
  int32_t minorVersion = file->getMinorVersion();
  if (mOutputs.find(minorVersion) == mOutputs.end()) {
    // Compile the transfer from file output to op output.
    // This will tell us whether it is an
    // identity or not (i.e. do we actually need to insert a 
    // copy operator to transform the parse output).
    mOutputs[minorVersion] = new TableColumnGroupVersionOutput(ctxt, fileMetadata, 
							       ColumnGroupType);
  }
  mOutputs[minorVersion]->addPath(file);
}

const std::vector<SortKey>& ColumnGroupOutput::getPrimaryKeyWithSuffix() const
{
  return mPrimaryKeyWithSuffix;
}

void ColumnGroupOutput::check(PlanCheckContext & ctxt)
{
  std::string version = Metadata->getTable()->getVersion();
  if (version.size()) {
    const RecordType * input = ColumnGroupType;
    std::vector<SortKey> sortKeys;
    sortKeys.push_back(version + " DESC");
    const std::vector<std::string>& primaryKey = Metadata->getTable()->getPrimaryKey();
    mKeyPrefixFun = SortKeyPrefixFunction::get(ctxt, input, sortKeys);
    mKeyLessThanFun = LessThanFunction::get(ctxt, input, input, sortKeys, true, "sort_less");
    mPresortedEqFun = EqualsFunction::get(ctxt, input, input, primaryKey, 
					  "presort_eq", true);

    // Output all fields plus the version index that
    // identifies the Nth highest version.
    std::string agg;
    for(RecordType::const_member_iterator m = input->begin_members(),
	  mEnd = input->end_members(); m != mEnd; ++m) {
      if (agg.size()) {
	agg += ",";
      }
      agg += m->GetName();
    }
    // Output index (TODO: Make sure this is unique)
    agg += ", SUM(1) AS versionIdx";
    mAggregate = new RecordTypeAggregate(ctxt, "columnGroupAgg", input, 
					 agg, primaryKey, true);

    // Filter largest version
    std::vector<RecordMember> emptyMembers;
    RecordType emptyTy(emptyMembers);
    std::vector<const RecordType *> inputs;
    inputs.push_back(mAggregate->getTarget());
    inputs.push_back(&emptyTy);
    mFilterFun = new RecordTypeFunction(ctxt, "columnGroupVersionFilter", 
					inputs, "versionIdx <= 1");

    // Transfer to final format.
    std::string xfers;
    std::set<std::string> pks(Metadata->getTable()->getPrimaryKey().begin(),
			      Metadata->getTable()->getPrimaryKey().end());
    pks.insert(version);
    for(RecordType::const_member_iterator m = input->begin_members(),
	  mEnd = input->end_members(); m != mEnd; ++m) {
      if (xfers.size()) {
	xfers += ",";
      }
      if (pks.find(m->GetName()) == pks.end()) {
	xfers += m->GetName();
      } else {
	// PKs and version must be made unique across
	// multiple columns groups
	xfers += m->GetName();
	xfers += " AS ";
	xfers += appendSuffix(m->GetName());
      }
    }
    mFinalTransfer = new RecordTypeTransfer(ctxt, "columnGroupTransfer", 
					    mAggregate->getTarget(), xfers);
  }
}

RuntimeOperatorType * ColumnGroupOutput::create(class RuntimePlanBuilder& plan,
						RuntimeOperatorType * inputOp)
{
  // s = sort[presorted="akid", key="version DESC"];
  // sm_muid -> s;
  // gb = sort_group_by[key="akid", output="akid, muid, SUM(1) AS idx", runningtotal="true"];
  // s -> gb;
  // f = filter[where="idx <= 1"];
  // gb -> f;
  // muid = copy[output="akid AS akid_muid, muid"];
  // f -> muid;
  plan.addOperatorType(inputOp);

  RuntimeOperatorType * sortTy = 
    new RuntimeSortOperatorType(ColumnGroupType,
				mKeyPrefixFun,
				mKeyLessThanFun,
				mPresortedEqFun,
				"",
				1000000);
  plan.addOperatorType(sortTy);
  plan.connect(inputOp, 0, sortTy, 0, false);

  RuntimeOperatorType * runningTotalTy = 
    new RuntimeSortRunningTotalOperatorType(ColumnGroupType->getFree(),
					    NULL,
					    mPresortedEqFun,
					    mAggregate);
  plan.addOperatorType(runningTotalTy);
  plan.connect(sortTy, 0, runningTotalTy, 0, false);

  RuntimeOperatorType * printTy = 
    new RuntimePrintOperatorType(mAggregate->getTarget(), 0);
  plan.addOperatorType(printTy);

  RuntimeOperatorType * filterTy = 
    new RuntimeFilterOperatorType(mAggregate->getTarget(),
				  mFilterFun,
				  std::numeric_limits<int64_t>::max());
  plan.addOperatorType(filterTy);
  plan.connect(runningTotalTy, 0, printTy, 0, false);
  plan.connect(printTy, 0, filterTy, 0, false);

  std::vector<const RecordTypeTransfer *> xfers;
  std::vector<bool> pics; 
  xfers.push_back(mFinalTransfer);
  pics.push_back(false);
  RuntimeOperatorType * copyTy = 
    new RuntimeCopyOperatorType(mAggregate->getTarget()->getFree(),
				xfers, pics);
  plan.connect(filterTy, 0, copyTy, 0, false);
				
  return copyTy;
}

TableOutput::TableOutput(const TableMetadata * metadata,
			 const RecordType * tableFormat)
  :
  Metadata(metadata),
  TableType(tableFormat)
{
}

TableOutput::~TableOutput()
{
}

ColumnGroupOutput * TableOutput::create(DynamicRecordContext& ctxt,
					TableColumnGroup* cg,
					const std::set<std::string>& referenced)
{
  mColumnGroups.push_back(new ColumnGroupOutput(ctxt, cg, referenced));
  mColumnGroupIndex[cg] = mColumnGroups.back();
  return mColumnGroups.back();
}

ColumnGroupOutput * TableOutput::find(TableColumnGroup* cg)
{
  return mColumnGroupIndex.find(cg)->second;
}

void TableOutput::check(PlanCheckContext & ctxt)
{
  std::vector<SortMergeJoin *> smjs;

  for(std::vector<ColumnGroupOutput*>::iterator cg = mColumnGroups.begin(),
	cgEnd = mColumnGroups.end(); cg != cgEnd; ++cg) {
    (*cg)->check(ctxt);
  }

  if (mColumnGroups.size() > 1) {
    const std::vector<std::string>& pk(Metadata->getPrimaryKey());
    std::set<std::string> pks(pk.begin(), pk.end());
    if (Metadata->getVersion().size()) {
      pks.insert(Metadata->getVersion());
    }
    // There are primary key columns in each of the column group
    // inputs.  In the final output, use a big CASE to pick
    // a single non NULL value for each primary key that exists
    // in the output.
    // TODO: Coerce the primary key to non nullable with an ISNULL.
    // TODO: Handle non nullable non-key columns.
    // TODO: If we have a column group that we know contains all valid
    // primary key values, then we can make these full outer joins into
    // right/left outer.  If we know other column groups contain all valid
    // primary key values then we can make inner joins.
    
    // Map each non-key member of the output to the column group that contains it.
    // We need this information in order to create the output of each
    // full outer join.
    std::map<std::string, std::size_t> outputToColumnGroup;
    for(std::size_t i = 0; i<mColumnGroups.size(); ++i) {
      const RecordType * cgType = mColumnGroups[i]->ColumnGroupType;
      for(RecordType::const_member_iterator m = cgType->begin_members(),
	    mEnd = cgType->end_members(); m != mEnd; ++m) {
	if (pks.find(m->GetName()) != pks.end()) 
	  continue;

	if(outputToColumnGroup.find(m->GetName()) !=
	   outputToColumnGroup.end()) {
	  throw std::runtime_error((boost::format("INTERNAL ERROR : "
						  "unexpected duplicate column %1%") %
				    m->GetName()).str());
	}
	outputToColumnGroup[m->GetName()] = i;
      }
    }

    std::string xfer;
    boost::format pkFmt("CASE WHEN %1% IS NULL THEN %1% ELSE %2% END AS %3%");
    for(RecordType::const_member_iterator m = TableType->begin_members(),
	  mEnd = TableType->end_members(); m != mEnd; ++m) {
      // Only include non-key columns from the inputs.
      if (pks.find(m->GetName()) == pks.end() &&
	  outputToColumnGroup.find(m->GetName())->second >= 2)
	continue;
      if (xfer.size()) {
	xfer += ",";
      }
      std::set<std::string>::iterator k = pks.find(m->GetName());
      if (k != pks.end()) {
	xfer += "CASE ";
	for(std::size_t j=0; j<2; ++j) {
	  xfer += (boost::format("WHEN %1% IS NOT NULL THEN %1% ") % 
			mColumnGroups[j]->appendSuffix(*k)).str();
	}
	xfer += "END AS ";
	xfer += *k;
      } else {
	xfer += m->GetName();
      }
    }
    // create full outer join of column group inputs
    smjs.push_back(new SortMergeJoin(ctxt, SortMergeJoin::FULL_OUTER, 
				     mColumnGroups[0]->getColumnGroupTypeWithSuffix(),
				     mColumnGroups[1]->getColumnGroupTypeWithSuffix(), 
				     mColumnGroups[0]->getPrimaryKeyWithSuffix(), 
				     mColumnGroups[1]->getPrimaryKeyWithSuffix(), 
				     "", xfer));
    mJoins.push_back(smjs.back()->create());
    boost::format pkFmt2("CASE WHEN %2% IS NOT NULL THEN %2% "
			 "WHEN %1% IS NOT NULL THEN %1% END AS %2%");
    for(std::size_t i=2; i<mColumnGroups.size(); ++i) {
      xfer = "";
      for(RecordType::const_member_iterator m = TableType->begin_members(),
	    mEnd = TableType->end_members(); m != mEnd; ++m) {

	// Only include non-key columns from the inputs.
	if (pks.find(m->GetName()) == pks.end() &&
	    outputToColumnGroup.find(m->GetName())->second >= (i+1))
	  continue;

	if (xfer.size()) {
	  xfer += ",";
	}
	std::set<std::string>::iterator k = pks.find(m->GetName());
	if (k != pks.end()) {
	  xfer += (pkFmt2 % mColumnGroups[i]->appendSuffix(*k) % (*k)).str();
	} else {
	  xfer += m->GetName();
	}
      }

      // TODO: Convert all metadata to use SortKey rather than string.
      std::vector<SortKey> pkMerge;
      for(std::vector<std::string>::const_iterator k = pk.begin();
	  k != pk.end(); ++k) {
	pkMerge.push_back(SortKey(*k, SortKey::ASC));
      }
      smjs.push_back(new SortMergeJoin(ctxt, SortMergeJoin::FULL_OUTER, 
				       smjs.back()->getOutputType(),
				       mColumnGroups[i]->getColumnGroupTypeWithSuffix(), 
				       pkMerge, 
				       mColumnGroups[i]->getPrimaryKeyWithSuffix(), 
				       "", xfer));
      mJoins.push_back(smjs.back()->create());
    }	
  }
  for(std::vector<SortMergeJoin *>::iterator smj= smjs.begin(),
	smjEnd = smjs.end(); smj != smjEnd; ++smj) {
    delete *smj;
  }
}

LogicalTableParser::LogicalTableParser(const std::string& table)
  :
  mTable(table),
  mCommonVersion(1),
  mMajorVersion(1),
  mTableFormat(NULL),
  mTableOutput(NULL),
  mSOT(NULL)
{
}

LogicalTableParser::LogicalTableParser(const std::string& table,
				       boost::shared_ptr<const TableMetadata> tableMetadata)
  :
  mTable(table),
  mCommonVersion(1),
  mMajorVersion(1),
  mTableFormat(NULL),
  mTableMetadata(tableMetadata),
  mTableOutput(NULL),
  mSOT(NULL)
{
}

LogicalTableParser::~LogicalTableParser()
{
  delete mSOT;
  delete mTableOutput;
}

void LogicalTableParser::check(PlanCheckContext& ctxt)
{
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (boost::algorithm::iequals(it->Name, "file")) {
      mFile = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "connect")) {
      mFileSystem = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "where")) {
      mPredicate = boost::get<std::string>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "commonversion")) {
      mCommonVersion = boost::get<int32_t>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "majorversion")) {
      mMajorVersion = boost::get<int32_t>(it->Value);
    } else if (boost::algorithm::iequals(it->Name, "output")) {
      std::string str = boost::get<std::string>(it->Value);
      typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
      boost::char_separator<char> sep(",");
      tokenizer tok(str, sep);
      for(tokenizer::iterator tokIt = tok.begin();
	  tokIt != tok.end();
	  ++tokIt) {
	// TODO: Validate that these are valid field names.
	mReferenced.push_back(boost::trim_copy(*tokIt));
      }
    } else if (boost::algorithm::iequals(it->Name, "table")) {
      mTable = boost::get<std::string>(it->Value);
    } else {
      checkDefaultParam(*it);
    }
  }
  
  // Attach to metadata catalog to get record type,
  // sort keys etc.
  if (mTableMetadata.get() == NULL) {
    MetadataCatalog catalog;
    mTableMetadata = catalog.find(mTable);
    if (mTableMetadata.get() == NULL) {
      ctxt.logError(*this, (boost::format("Table %1% does not exist") %
			    mTable).str());
      // Attach a dummy empty record.
      std::vector<RecordMember> members;
      getOutput(0)->setRecordType(RecordType::get(ctxt, members));
      return;
    }
  }
  mTableFormat = mTableMetadata->getRecordType(ctxt);

  // Default referenced is the entire table.
  if (0 == mReferenced.size()) {
    for(RecordType::const_member_iterator m = mTableFormat->begin_members(),
	  e = mTableFormat->end_members(); m != e; ++m) {
      mReferenced.push_back(m->GetName());
    }
  }
    
  // The output format of the operator.
  getOutput(0)->setRecordType(RecordType::get(ctxt, mTableFormat, 
					      mReferenced.begin(), mReferenced.end()));


  // In addition to the columns that the operators output,
  // there may be other keys for processing column groups (e.g.
  // primary keys used to glue together multiple column groups
  // and to resolve versions.
  std::set<std::string> referencedAndRequired(mReferenced.begin(), mReferenced.end());
  mTableMetadata->addColumnGroupRequired(referencedAndRequired);
  mTableOutput = new TableOutput(mTableMetadata.get(), mTableFormat);
  if (0 == mFile.size()) {
    if (mTableMetadata->getSortKeys().size() != 0) {
      for(TableMetadata::column_group_const_iterator cg = mTableMetadata->beginColumnGroups(),
	    endCg = mTableMetadata->endColumnGroups(); cg != endCg; ++cg) {
	ColumnGroupOutput * cgo = mTableOutput->create(ctxt, *cg, referencedAndRequired);
	const RecordType * ty = cgo->ColumnGroupType;

	// If we are in "table" mode then we're going
	// to need a sort merge.  Build the required
	// predicates now.
	// If the user is not outputting sort keys
	// then we skip the sort merging starting at that
	// point.  
	std::vector<std::string> sortKeys;
	std::set<std::string> lookup;
	for(RecordType::const_member_iterator m = ty->begin_members(),
	      e = ty->end_members(); m != e; ++m) {
	  lookup.insert(m->GetName());
	}
	for(TableMetadata::sort_key_const_iterator k = mTableMetadata->getSortKeys().begin(),
	      e = mTableMetadata->getSortKeys().end();
	    k != e; ++k) {
	  std::set<std::string>::const_iterator it = lookup.find(*k);
	  if (lookup.end() == it) {
	    break;
	  }
	  sortKeys.push_back(*it);
	}
	if (sortKeys.size()) {
	  cgo->mKeyPrefix = 
	    KeyPrefixFunction::get(ctxt, ty, sortKeys, "keyPrefix");
	  cgo->mLessThan = 
	    LessThanFunction::get(ctxt, ty, sortKeys, "lessThan");
	}
      }
    }

    // We are reading from a table/serial structured store.
    // Expand out the serial structure and create an
    // op for each path then merge them with a sort merge
    // on akid.
    mSOT = new SerialOrganizedTable(mCommonVersion, mMajorVersion, 
				    mTable, 
				    mPredicate.size() ? mPredicate.c_str() : NULL);
    {
      // Scope to hide fs.
      FileSystem * fs = FileSystem::get(boost::make_shared<URI>(mFileSystem.c_str()));
      mSOT->bind(fs);
      FileSystem::release(fs);
    }

    // TODO: Handle case in which there is nothing to read!  Perhaps we just
    // return an empty data set
    if (mSOT->getSerialPaths().size() == 0)
      throw std::runtime_error((boost::format("No available paths for %1% table") %
				mTable).str());
    
    // Check all of the input serial paths.  For each input examine the 
    // minor version and figure out whether there are any columns that
    // we need to compute.  Note also that we allow for columns to be
    // reordered between minor versions.
    for(std::vector<SerialOrganizedTableFilePtr>::const_iterator it = mSOT->getSerialPaths().begin();
	it != mSOT->getSerialPaths().end();
	++it) {
      TableFileMetadata * fileMetadata = NULL;
      TableColumnGroup * cg = NULL;
      mTableMetadata->resolveMetadata(*it, cg, fileMetadata);
      ColumnGroupOutput * cgo = mTableOutput->find(cg);
      cgo->addPath(ctxt, fileMetadata, *it);
    } 
    // Check all table and column group outputs
    mTableOutput->check(ctxt);
  }

}

void LogicalTableParser::create(class RuntimePlanBuilder& plan)
{
  typedef GenericParserOperatorType<> file_op;
  typedef GenericParserOperatorType<SerialChunkStrategy> table_op;
  if (mFile.size()) {
    RuntimeOperatorType * opType = NULL;
    opType = new file_op(mFile,
			 '\t',
			 '\n',
			 getOutput(0)->getRecordType(),
			 mTableFormat);
    plan.addOperatorType(opType);
    plan.mapOutputPort(this, 0, opType, 0);
  } else {
    for(std::vector<ColumnGroupOutput*>::iterator cg = mTableOutput->mColumnGroups.begin(),
	  cgEnd = mTableOutput->mColumnGroups.end(); cg != cgEnd; ++cg) {
      RuntimeOperatorType * mergeType = NULL;
      std::size_t inputNum = 0;
      // If sorted data then we sort merge otherwise union all.
      if ((*cg)->mKeyPrefix != NULL && (*cg)->mLessThan != NULL) {
	mergeType = new RuntimeSortMergeOperatorType((*cg)->mKeyPrefix, (*cg)->mLessThan);
      } else {
	mergeType = new RuntimeUnionAllOperatorType();
      }

      for(ColumnGroupOutput::version_iterator v = (*cg)->beginVersions(),
	    vEnd = (*cg)->endVersions(); v != vEnd; ++v) {
	TableColumnGroupVersionOutput * po = v->second;
	for(TableColumnGroupVersionOutput::path_iterator p = po->beginPaths(),
	      pEnd = po->endPaths(); p != pEnd; ++p) {
	  RuntimeOperatorType * opType = new table_op((*p)->getPath(),
						      '\t',
						      '\n',
						      po->FileOutput,
						      po->FileType);
	  plan.addOperatorType(opType);
	  if (!po->Transfer->isIdentity()) {
	    // Resolve version discrepancies 
	    std::vector<const RecordTypeTransfer *> xfers;
	    xfers.push_back(po->Transfer);
	    RuntimeOperatorType * copyOp = new RuntimeCopyOperatorType(po->FileOutput->getFree(),
								       xfers);
	    plan.addOperatorType(copyOp);
	    // Disable buffering here since not needed and chews up memory.
	    plan.connect(opType, 0, copyOp, 0, false);
	    opType = copyOp;
	  }
	  // When connecting reads to the sort merge there is no need for buffering
	  // so disabled it (otherwise we can chew up a lot of memory in the buffers
	  // of so many channels).
	  plan.connect(opType, 0, mergeType, inputNum++, false);	
	}

	// Is this a "versioned" column group?  If so collapse the 
	// each stream of record versions into the last version.
	BOOST_ASSERT(NULL != (*cg)->Metadata);
	BOOST_ASSERT(NULL != (*cg)->Metadata->getTable());
	if ((*cg)->Metadata->getTable()->getVersion().size()) {
	  mergeType = (*cg)->create(plan, mergeType);
	}
      }
      // All done with the ColumnGroup; this is the operator that outputs it.
      (*cg)->OutputOperator = mergeType;
    }

    if (mTableOutput->mColumnGroups.size() > 1) {
      BOOST_ASSERT(mTableOutput->mJoins.size()+1 == 
		   mTableOutput->mColumnGroups.size());
      plan.addOperatorType(mTableOutput->mColumnGroups[0]->OutputOperator);
      plan.addOperatorType(mTableOutput->mColumnGroups[1]->OutputOperator);
      plan.addOperatorType(mTableOutput->mJoins[0]);
      plan.connect(mTableOutput->mColumnGroups[0]->OutputOperator, 0, 
		   mTableOutput->mJoins[0], 0, false);
      plan.connect(mTableOutput->mColumnGroups[1]->OutputOperator, 0, 
		   mTableOutput->mJoins[0], 1, false);
      for(std::size_t i=1; i<mTableOutput->mJoins.size(); ++i) {
	plan.addOperatorType(mTableOutput->mColumnGroups[i+1]->OutputOperator);
	plan.addOperatorType(mTableOutput->mJoins[i]);
	plan.connect(mTableOutput->mColumnGroups[i+1]->OutputOperator, 0, 
		     mTableOutput->mJoins[i], 1, false);
	plan.connect(mTableOutput->mJoins[i-1], 0, 
		     mTableOutput->mJoins[i], 0, false);
      }
      plan.mapOutputPort(this, 0, mTableOutput->mJoins.back(), 0);
    } else {
      plan.addOperatorType(mTableOutput->mColumnGroups[0]->OutputOperator);
      plan.mapOutputPort(this, 0, mTableOutput->mColumnGroups[0]->OutputOperator, 0);
    }
  }
}

