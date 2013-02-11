#include <boost/algorithm/string/trim.hpp>
#include <boost/tokenizer.hpp>
#include "AsyncRecordParser.hh"
#include "FileSystem.hh"

// Based on uts/common/sys/sysmacros.h of Open Solaris
static uintptr_t p2align(uintptr_t x, uintptr_t align)
{
  return x & -align;
}
static uintptr_t p2roundup(uintptr_t x, uintptr_t align)
{
  return -(-x & -align);
}

// Recall x86_64 calling convention:
// begin = %%rdi
// end = %%rsi
// c = %%edx
// toMatch = %%rcx
// This implementation reads in 16 byte blocks and thus reads
// outside the pointers begin/end if they are not 16 byte aligned.
// No harm comes from this since the memory will be readable and
// we check to make sure that we don't detect characters outside
// the interval [begin, end).
// This implementation is heavily influenced by __strchr_sse2
// from GLIBC.
// %rdi = pointer to input
// %rcx = temporary for begin, difference between 16 aligned, bitmask
// %rdx = bitmask result
// %r8 = toMatch pointer
// %r9 = toMatch value
const int32_t __attribute__((used)) popcntlut[256] = {0, 
				1, 
				1, 2, 
				1, 2, 2, 3, 
				1, 2, 2, 3, 2, 3, 3, 4,
				1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
				1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
				1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
				1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 4, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

const char * mymemchr(const char * begin,
		      const char * end,
		      int32_t c,
		      std::size_t * toMatch)
{
  const char * ret=NULL;
  __asm__ ("movd %%edx, %%xmm1;\n\t"
	   "movq %%rcx, %%r8;\n\t"
	   "movq %%rdi, %%rcx;\n\t"
	   "punpcklbw %%xmm1, %%xmm1;\n\t"
	   "andq $~15, %%rdi;\n\t"
	   "punpcklbw %%xmm1, %%xmm1;\n\t"
	   "orl $0xffffffff, %%edx;\n\t"
	   "movdqa (%%rdi), %%xmm0;\n\t"
	   "pshufd $0, %%xmm1, %%xmm1;\n\t"
	   "movq (%%r8), %%r9;\n\t"
	   "subq %%rdi, %%rcx;\n\t"
	   "leaq 16(%%rdi), %%rdi;\n\t"
	   "pcmpeqb %%xmm1, %%xmm0;\n\t"
	   "shl %%cl, %%edx;\n\t"
	   "pmovmskb %%xmm0, %%ecx;\n\t"
	   "andl %%edx, %%ecx;\n\t"
	   "mov %%cl, %%r10b;\n\t"
	   "movl popcntlut(,%%r10,4), %%edx;\n\t"
	   "movl %%ecx, %%r10d;\n\t"
	   "shr $8, %%r10d;\n\t"
	   "movl popcntlut(,%%r10,4), %%edx;\n\t"
	   "bsfl %%ecx, %%ecx;\n\t"
	   "jnz 1f;\n\t"
	   "2: cmpq %%rsi, %%rdi;\n\t"
	   "jae 3f;\n\t"
	   "movdqa (%%rdi), %%xmm0;\n\t"
	   "leaq 16(%%rdi), %%rdi;\n\t"
	   "pcmpeqb %%xmm1, %%xmm0;\n\t"
	   "pmovmskb %%xmm0, %%ecx;\n\t"
	   "bsfl %%ecx, %%ecx;\n\t"
	   "jz 2b;\n\t"
	   "1: leaq -16(%%rdi,%%rcx), %%rdi;\n\t"
	   "cmpq %%rdi, %%rsi;\n\t"
	   "ja 4f;\n\t"
	   "3: xorl %%edi, %%edi;\n\t"	   
	   "4: movq %%rdi, %0;\n\t"
	   : "=r"(ret)
	   : 
	   : "%rdi", "%rsi", "%rcx", "%rdx", "%xmm0", "%xmm1");
  
  // TODO: Put return in ret.
  return ret;
}

AsyncDataBlock::AsyncDataBlock(uint8_t * start, uint8_t * end)
  :
  mCurrentBlockMark(start),
  mCurrentBlockStart(start),
  mCurrentBlockEnd(end),
  mCurrentBlockPtr(start)
{
}

AsyncDataBlock::AsyncDataBlock()
  :
  mCurrentBlockMark(NULL),
  mCurrentBlockStart(NULL),
  mCurrentBlockEnd(NULL),
  mCurrentBlockPtr(NULL)
{
}

void AsyncDataBlock::rebind(uint8_t * start, uint8_t * end)
{
  mCurrentBlockMark = mCurrentBlockStart = mCurrentBlockPtr = start;
  mCurrentBlockEnd = end;
}

void ImporterSpec::createDefaultImport(const RecordType * recordType,
				       const RecordType * baseRecordType,
				       char fieldDelim,
				       char recordDelim,
				       std::vector<ImporterSpec*>& importers)
{
  typedef FieldImporter field_importer_type;
  // Apply default rules for inferring physical layout from
  // logical record definition.
  for(RecordType::const_member_iterator mit = baseRecordType->begin_members();
      mit != baseRecordType->end_members();
      ++mit) {
    // TODO: Last field allow either record or field terminator so that
    // we can support ignoring trailing fields.
    char delim = mit+1 == baseRecordType->end_members() ? recordDelim : fieldDelim;
    if (recordType->hasMember(mit->GetName())) {
      const RecordMember& member(recordType->getMember(mit->GetName()));
      FieldAddress offset = recordType->getMemberOffset(mit->GetName());
      // Get the importer to set up.
      ImporterSpec * spec;
      // Dispatch on in-memory type.
      switch(member.GetType()->GetEnum()) {
      // case FieldType::VARCHAR:
      // 	fit.InitVariableLengthTerminatedString(offset, delim, 
      // 					       member.GetType()->isNullable());
      // 	break;
      case FieldType::CHAR:
	spec = new ImportFixedLengthStringSpec(offset, 
					       member.GetType()->GetSize(), 
					       member.GetType()->isNullable());
	break;
      // case FieldType::BIGDECIMAL:
      // 	fit.InitDelimitedDecimal(offset, delim, member.GetType()->isNullable());
      // 	break;
      case FieldType::INT32:
	spec = new ImportDecimalInt32Spec(offset, 
					  member.GetType()->isNullable());
	break;
      // case FieldType::INT64:
      // 	fit.InitDecimalInt64(offset, 
      // 			     member.GetType()->isNullable());
      // 	break;
      case FieldType::DOUBLE:
	spec = new ImportDoubleSpec(offset, 
				    member.GetType()->isNullable());
      	break;
      // case FieldType::DATETIME:
      // 	fit.InitDelimitedDatetime(offset, 
      // 				  member.GetType()->isNullable());
      // 	break;
      // case FieldType::DATE:
      // 	fit.InitDelimitedDate(offset, 
      // 			      member.GetType()->isNullable());
      // 	break;
      case FieldType::FUNCTION:
	throw std::runtime_error("Importing function types not supported");
	break;
      default:
	throw std::runtime_error("Importing unknown field type");
      }
      importers.push_back(spec);
    }
    importers.push_back(new ConsumeTerminatedStringSpec(delim));
  }
  // Optimize this a bit.  Iterate backward to find the
  // last importer that generates a value (not just consume).
  // Make one more importer consuming till the end of the line.
  std::size_t sz = importers.size();
  for(std::size_t i=1; i<=sz; ++i) {
    if (!importers[sz-i]->isConsumeOnly()) {
      if (i > 2) {
	// Last generator is at sz - i.  So make the last
	// element a consumer at sz - i + 1.  Thus new size
	// is sz-i+2.
	importers.resize(sz - i + 2);
	delete importers[sz - i + 1];
	importers[sz - i + 1] = new ConsumeTerminatedStringSpec(recordDelim);
      }
      break;
    }
  }
}

ConsumeTerminatedString::ConsumeTerminatedString(uint8_t term)
  :
  mState(START),
  mTerm(term)
{
}

ParserState ConsumeTerminatedString::import(AsyncDataBlock& source, RecordBuffer target) 
{
  switch(mState) {
    while(true) {      
      if (source.isEmpty()) {
	mState = READ;
	return ParserState::exhausted();
      case READ:
	if (source.isEmpty()) {
	  return ParserState::error(-1);
	}
      }
      if (importInternal(source, target)) {
	mState = START;
	return ParserState(ParserState::success());
      case START:
	;
      }
    }
  }
}

ImportDecimalInt32::ImportDecimalInt32(const FieldAddress& targetOffset,
				       uint8_t term)
  :
  mTargetOffset(targetOffset),
  mState(START),
  mValue(0),
  mTerm(term),
  mNeg(false)
{
}

ParserState ImportDecimalInt32::import(AsyncDataBlock& source, RecordBuffer target) 
{
  switch(mState) {
    while(true) {      
      if (source.isEmpty()) {
	mState = READ_FIRST;
	return ParserState::exhausted();
      case READ_FIRST:
	if (source.isEmpty()) {
	  return ParserState::error(-1);
	}
      }
      if (*source.begin() == '-') {
	mNeg = true;
	source.consume(1);
      } else if (*source.begin() == '+') {
	source.consume(1);
      }
      while(true) {
	if (source.isEmpty()) {
	  mState = READ_DIGITS;
	  return ParserState::exhausted();
	case READ_DIGITS:
	  if (source.isEmpty()) {
	    return ParserState::error(-1);
	  }
	}
	if (importInternal(source, target)) {
	  mState = START;
	  return ParserState(ParserState::success());
	case START:
	  break;
	}
      }
    }
  }
}

ImportDouble::ImportDouble(const FieldAddress& targetOffset,
			   uint8_t term)
  :
  mState(START),
  mLocal(NULL),
  mTerm(term)
{
}

ParserState ImportDouble::import(AsyncDataBlock& source, RecordBuffer target)
{
  switch(mState) {
    while(true) {      
    case START:
      // Fast path; we find a terminator in the input buffer and
      // just call atof.
      {
	uint8_t term = mTerm;
	for(uint8_t * s = source.begin(), * e = source.end();
	    s != e; ++s) {
	  if (*s == term) {
	    mTargetOffset.setDouble(atof((char *) source.begin()), target);
	    source.consume(s - source.begin());
	    return ParserState::success();	    
	  }
	}
      }  

      // Slow path; the field crosses the block boundary.  Copy into
      // private memory and call atof from there.
      mLocal = new std::vector<uint8_t>();
      do {
	mLocal->insert(mLocal->end(), source.begin(), source.end());
	source.consumeAll();
	mState = READ;
	return ParserState::exhausted();
      case READ:
      {
	uint8_t term = mTerm;
	for(uint8_t * s = source.begin(), * e = source.end();
	    s != e; ++s) {
	  if (*s == term) {
	    mLocal->insert(mLocal->end(), source.begin(), s);
	    mTargetOffset.setDouble(atof((char *) &mLocal->front()), target);
	    source.consume(s - source.begin());
	    delete mLocal;
	    mLocal = NULL;
	    mState = START;
	    return ParserState::success();	    
	  } 
	}
      }
      } while(true);
    }
  }
}

ImportFixedLengthString::ImportFixedLengthString(const FieldAddress& targetOffset,
						 int32_t sz)
  :
  mTargetOffset(targetOffset),
  mState(START),
  mSize(sz)
{
}

ParserState ImportFixedLengthString::import(AsyncDataBlock& source, RecordBuffer target) 
{
  switch(mState) {
    while(true) { 
    case START:
      // Fast path
      if (source.begin() + mSize <= source.end()) {
	mTargetOffset.SetFixedLengthString(target, 
					   (const char *) source.begin(), 
					   mSize);
	source.consume(mSize);
	mState = START;
	return ParserState::success();
      }

      // Slow path : not enough buffer to copy in one
      // shot.  Read the remainder of the buffer and try
      // again.
      mRead = 0;
      while(true) {
	if (source.isEmpty()) {
	  mState = READ;
	  return ParserState::exhausted();
	case READ:
	  if (source.isEmpty()) {
	    return ParserState::error(-1);
	  }
	}
	if (mSize - mRead + source.begin() <= source.end()) {	  
	  // We're done
	  int32_t toRead = mSize - mRead;
	  memcpy(mTargetOffset.getCharPtr(target) + mRead,
		 source.begin(),
		 toRead);
	  mTargetOffset.clearNull(target);
	  source.consume(toRead);
	  mState = START;
	  mRead = 0;
	  return ParserState::success();
	} else {
	  // Partial read
	  int32_t toRead = (int32_t) (source.end() - source.begin());
	  memcpy(mTargetOffset.getCharPtr(target) + mRead,
		 source.begin(),
		 source.end() - source.begin());
	  mRead += toRead;
	  source.consumeAll();
	}	       
      }      
    }
  }
}

LogicalAsyncParser::LogicalAsyncParser()
  :
  LogicalOperator(1,1,1,1),
  mMode("text"),
  mSkipHeader(false),
  mFieldSeparator('\t'),
  mRecordSeparator('\n'),
  mFormat(NULL)
{
}

LogicalAsyncParser::~LogicalAsyncParser()
{
}

void LogicalAsyncParser::check(PlanCheckContext& ctxt)
{
  const LogicalOperatorParam * formatParam=NULL;
  std::vector<std::string> referenced;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("comment")) {
	mCommentLine = getStringValue(ctxt, *it);
      } else if (it->equals("fieldseparator")) {
	std::string tmp = getStringValue(ctxt, *it);
	if (tmp.size() == 1) {
	  mFieldSeparator = tmp[0];
	} else if (boost::algorithm::equals(tmp, "\\t")) {
	  mFieldSeparator = '\t';
	} else if (boost::algorithm::equals(tmp, "\\n")) {
	  mFieldSeparator = '\n';
	} else {
	  ctxt.logError(*this, "unsupported field separator");
	}
      } else if (it->equals("recordseparator")) {
	std::string tmp = getStringValue(ctxt, *it);
	if (tmp.size() == 1) {
	  mRecordSeparator = tmp[0];
	} else if (boost::algorithm::equals(tmp, "\\t")) {
	  mRecordSeparator = '\t';
	} else if (boost::algorithm::equals(tmp, "\\n")) {
	  mRecordSeparator = '\n';
	} else {
	  ctxt.logError(*this, "unsupported record separator");
	}
      } else if (it->equals("format")) {
	mStringFormat = getStringValue(ctxt, *it);
	formatParam = &*it;
      } else if (it->equals("formatfile")) {
	std::string filename(getStringValue(ctxt, *it));
	mStringFormat = FileSystem::readFile(filename);
	formatParam = &*it;
      } else if (it->equals("mode")) {
	mMode = getStringValue(ctxt, *it);
      } else if (it->equals("output")) {
	std::string str = boost::get<std::string>(it->Value);
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(",");
	tokenizer tok(str, sep);
	for(tokenizer::iterator tokIt = tok.begin();
	    tokIt != tok.end();
	    ++tokIt) {
	  // TODO: Validate that these are valid field names.
	 referenced.push_back(boost::trim_copy(*tokIt));
	}
      } else if (it->equals("skipheader")) {
	mSkipHeader = getBooleanValue(ctxt, *it);
      } else {
	checkDefaultParam(*it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }

  if (!boost::algorithm::iequals("binary", mMode) &&
      !boost::algorithm::iequals("text", mMode)) {
    ctxt.logError(*this, "mode parameter must be \"text\" or \"binary\"");
  }

  if (boost::algorithm::iequals("binary", mMode) &&
      mCommentLine.size()) {
    ctxt.logError(*this, "comment only supported when mode is \"text\"");
  }

  if (boost::algorithm::iequals("binary", mMode) &&
      referenced.size()) {
    ctxt.logError(*this, "output only supported when mode is \"text\"");
  }

  // We must have format parameter.
  if (NULL == formatParam || 0==mStringFormat.size()) {
    ctxt.logError(*this, "Must specify format argument");
  } else {
    try {
      IQLRecordTypeBuilder bld(ctxt, mStringFormat, false);
      mFormat = bld.getProduct();
      // Default referenced is all columns.
      if (0 == referenced.size()) {
	for(RecordType::const_member_iterator m = mFormat->begin_members(),
	      e = mFormat->end_members(); m != e; ++m) {
	  referenced.push_back(m->GetName());
	}
      }
      
      getOutput(0)->setRecordType(RecordType::get(ctxt, mFormat, 
						  referenced.begin(), 
						  referenced.end()));
    } catch(std::exception& ex) {
      ctxt.logError(*this, *formatParam, ex.what());
    }
  }
}

void LogicalAsyncParser::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = NULL;
  if(boost::algorithm::iequals("binary", mMode)) {
    throw std::runtime_error("Binary mode not supported yet");
  } else {
    GenericAsyncParserOperatorType * tot = 
      new GenericAsyncParserOperatorType(mFieldSeparator,
					 mRecordSeparator,
					 getInput(0)->getRecordType(),
					 getOutput(0)->getRecordType(),
					 mFormat,
					 mCommentLine.c_str());
    tot->setSkipHeader(mSkipHeader);
    opType = tot;
  }
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

GenericRecordImporter::GenericRecordImporter(std::vector<ImporterSpec*>::const_iterator begin,
					     std::vector<ImporterSpec*>::const_iterator end)
  :
  mImporterObjects(NULL)
{
  // Convert the import specs into objects and delegates
  // for fast invocation (that avoids virtual functions/vtable
  // lookup).
    
  // We want all importer state to be packed into a small
  // region of memory.
  std::size_t sz = 0;
  for(std::vector<ImporterSpec*>::const_iterator it = begin;
      it != end; ++it) {
    sz += sz % (*it)->objectAlignment();
    sz += (*it)->objectSize();      
  }
  mImporterObjects = new uint8_t [sz];
  sz = 0;
  for(std::vector<ImporterSpec*>::const_iterator it = begin;
      it != end; ++it) {
    sz += sz % (*it)->objectAlignment();
    mImporters.push_back((*it)->makeObject(mImporterObjects + sz));
    sz += (*it)->objectSize();      
  }
}

GenericRecordImporter::~GenericRecordImporter()
{
  delete [] mImporterObjects;
}

class GenericAsyncParserOperator : public RuntimeOperator
{
private:
  typedef GenericAsyncParserOperatorType operator_type;

  enum State { START, READ, READ_NEW_RECORD, WRITE, WRITE_EOF };
  State mState;

  // // The importer objects themselves
  // uint8_t * mImporterObjects;
  // // Importer delegates
  // std::vector<ImporterDelegate> mImporters;
  // // The field am I currently importing
  // std::vector<ImporterDelegate>::iterator mIt;
  // Importer objects and delegates
  GenericRecordImporter mImporters;
  // The field I am currently importing
  GenericRecordImporter::iterator mIt;
  // Input buffer for the file.
  AsyncDataBlock mInputBuffer;
  // Status of last call to parser
  ParserState mParserState;
  // Stream block buffer containing data
  RecordBuffer mInput;
  // Record buffer I am importing into
  RecordBuffer mOutput;
  // Records imported
  uint64_t mRecordsImported;

  const operator_type & getLogParserType()
  {
    return *static_cast<const operator_type *>(&getOperatorType());
  }

public:
  GenericAsyncParserOperator(RuntimeOperator::Services& services, 
			     const RuntimeOperatorType& opType)
    :
    RuntimeOperator(services, opType),
    mImporters(getLogParserType().mImporters.begin(),
	       getLogParserType().mImporters.end()),
    mRecordsImported(0)
  {
    // std::size_t sz = getLogParserType().mCommentLine.size();
    // if (sz > std::numeric_limits<int32_t>::max()) {
    //   throw std::runtime_error("Comment line too large");
    // }
    // mCommentLineSz = (int32_t) sz;

    // // Convert the import specs into objects and delegates
    // // for fast invocation (that avoids virtual functions/vtable
    // // lookup).
    
    // // We want all importer state to be packed into a small
    // // region of memory.
    // std::size_t sz = 0;
    // for(operator_type::field_importer_const_iterator it = 
    // 	  getLogParserType().mImporters.begin();
    // 	it != getLogParserType().mImporters.end(); ++it) {
    //   sz += sz % (*it)->objectAlignment();
    //   sz += (*it)->objectSize();      
    // }
    // mImporterObjects = new uint8_t [sz];
    // sz = 0;
    // for(operator_type::field_importer_const_iterator it = 
    // 	  getLogParserType().mImporters.begin();
    // 	it != getLogParserType().mImporters.end(); ++it) {
    //   sz += sz % (*it)->objectAlignment();
    //   mImporters.push_back((*it)->makeObject(mImporterObjects + sz));
    //   sz += (*it)->objectSize();      
    // }
  }

  ~GenericAsyncParserOperator()
  {
    // delete [] mImporterObjects;
  }

  /**
   * intialize.
   */
  void start()
  {
    mState = START;
    mRecordsImported = 0;    
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      // TODO: Handle cases in which we have to skip the first line
      // e.g. a header or a partial line in a map reduce style split.
      // // Allocate a new input buffer for the file in question.
      // if ((*mFileIt)->getBegin() > 0) {
      //   throw std::runtime_error("Not implemented yet");
      //   // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
      //   // 				64*1024,
      //   // 				(*mFileIt)->getBegin()-1,
      //   // 				(*mFileIt)->getEnd());
      //   // RecordBuffer nullRecord;
      //   // getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
      // } else {
      //   // mInputBuffer = DataBlock::get((*mFileIt)->getFilename().c_str(), 
      //   // 				64*1024,
      //   // 				(*mFileIt)->getBegin(),
      //   // 				(*mFileIt)->getEnd());
      //   mFileService->requestOpenForRead((*mFileIt)->getFilename().c_str(), 
      // 				   (*mFileIt)->getBegin(),
      // 				   (*mFileIt)->getEnd(),
      // 				   getCompletionPorts()[0]);
      //   requestCompletion(0);
      //   mState = OPEN_FILE;
      //   return;
      // case OPEN_FILE:
      //   {
      //     RecordBuffer buf;
      //     read(port, buf);
      //     mFileHandle = mFileService->getOpenResponse(buf);
      //   }
      //   if (getLogParserType().mSkipHeader) {
      //     throw std::runtime_error("Async skip header not implemented yet");
      //     //   RecordBuffer nullRecord;
      //     //   getLogParserType().mSkipImporter.Import(*mInputBuffer, nullRecord);
      //   }
      // }
	
      // Read all of the record in the file.
      while(true) {
	// If empty read a block; it is OK to exhaust a file
	// here but not while in the middle of a record, so 
	// we make a separate read attempt here.
	if (mInputBuffer.isEmpty()) {
	  requestRead(0);
	  mState = READ_NEW_RECORD;
	  return;
	case READ_NEW_RECORD:
	  {
	    if (mInput != RecordBuffer()) {
	      getLogParserType().mFree.free(mInput);
	    }
	    read(port, mInput);
	    if (mInput == RecordBuffer()) {
	      // Done
	      break;
	    }
	    int32_t bytesRead = getLogParserType().mStreamBlock.getSize(mInput);
	    if (0 == bytesRead) {
	      // ASSERT here instead of trying again?
	      getLogParserType().mFree.free(mInput);
	      mInput = RecordBuffer();
	      continue;
	    }
	    uint8_t * imp = 
	      (uint8_t *) getLogParserType().mStreamBlock.begin(mInput);
	    mInputBuffer.rebind(imp, imp + bytesRead);
	  }
	}
	// This is our actual record.
	mOutput = getLogParserType().mMalloc.malloc();
	for(mIt = mImporters.begin();
	    mIt != mImporters.end();
	    ++mIt) {
	  while(true) {
	    mParserState = (*mIt)(mInputBuffer, mOutput);
	    if (mParserState.isSuccess()) {
	      // Successful parse
	      break;
	    } else if (mParserState.isExhausted()) {
	      BOOST_ASSERT(mInputBuffer.isEmpty());
	      do {
		requestRead(0);
		mState = READ;
		return;
	      case READ:
		{
		  if (mInput != RecordBuffer()) {
		    getLogParserType().mFree.free(mInput);
		  }
		  read(port, mInput);
		  if (mInput == RecordBuffer()) {
		    throw std::runtime_error("Parse Error in record: "
					     "end of file reached");
		  }
		  int32_t bytesRead = getLogParserType().mStreamBlock.getSize(mInput);
		  if (0 == bytesRead) {
		    // ASSERT here instead of trying again?
		    getLogParserType().mFree.free(mInput);
		    mInput = RecordBuffer();
		    continue;
		  }
		  uint8_t * imp = 
		    (uint8_t *) getLogParserType().mStreamBlock.begin(mInput);
		  mInputBuffer.rebind(imp, imp + bytesRead);
		}
	      } while(false);	      
	    } else {
	      // Bad record
	      throw std::runtime_error("Parse Error in record");
	    }
	  }
	}
	// Done cause we had good record
	mRecordsImported += 1;
	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mOutput, false);
      }
      // Done with the last file so output EOS.
      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer::create(), true);
      return;
    }
  }

  void shutdown()
  {
  }
};

GenericAsyncParserOperatorType::GenericAsyncParserOperatorType(char fieldSeparator,
							       char recordSeparator,
							       const RecordType * inputStreamType,
							       const RecordType * recordType,
							       const RecordType * baseRecordType,
							       const char * commentLine)
  :
  RuntimeOperatorType("GenericAsyncParserOperatorType"),
  mSkipImporter(NULL),
  mStreamBlock(inputStreamType),
  mStreamMalloc(inputStreamType->getMalloc()),
  mRecordType(recordType),
  mSkipHeader(false),
  mCommentLine(commentLine)
{
  mMalloc = mRecordType->getMalloc();
  mFree = inputStreamType->getFree();

  // Records have tab delimited fields and newline delimited records
  ImporterSpec::createDefaultImport(recordType, 
				    baseRecordType ? baseRecordType : recordType,
				    fieldSeparator, 
				    recordSeparator, mImporters);
    
  // To skip a line we just parse to newline and discard.
  // We need this when syncing to the middle of a file.
  mSkipImporter = new ConsumeTerminatedStringSpec(recordSeparator);
}

GenericAsyncParserOperatorType::~GenericAsyncParserOperatorType()
{
  for(field_importer_const_iterator it = mImporters.begin(); 
      it != mImporters.end(); ++it) {
    delete *it;
  }
  delete mSkipImporter;
}

RuntimeOperator * GenericAsyncParserOperatorType::create(RuntimeOperator::Services & services) const
{
  return new GenericAsyncParserOperator(services, *this);
}

LogicalBlockRead::LogicalBlockRead()
  :
  LogicalOperator(0,0,1,1),
  mStreamBlock(NULL),
  mBufferCapacity(64*1024),
  mBucketed(false)
{
}

LogicalBlockRead::~LogicalBlockRead()
{
}

void LogicalBlockRead::check(PlanCheckContext& ctxt)
{
  const LogicalOperatorParam * formatParam=NULL;
  std::vector<std::string> referenced;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("file")) {
	mFile = getStringValue(ctxt, *it);
      } else if (it->equals("bucketed")) {
	mBucketed = getBooleanValue(ctxt, *it);
      } else if (it->equals("blocksize")) {
	mBufferCapacity = getInt32Value(ctxt, *it);
      } else {
	checkDefaultParam(*it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }

  std::vector<RecordMember> members;
  members.push_back(RecordMember("size", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("buffer", CharType::Get(ctxt, mBufferCapacity)));  
  mStreamBlock = RecordType::get(ctxt, members);
  getOutput(0)->setRecordType(mStreamBlock);
}

void LogicalBlockRead::create(class RuntimePlanBuilder& plan)
{
  typedef GenericAsyncReadOperatorType<ExplicitChunkStrategy> text_op_type;
  typedef GenericAsyncReadOperatorType<SerialChunkStrategy> serial_op_type;

  RuntimeOperatorType * opType = NULL;
  if (mBucketed) {
    PathPtr p = Path::get(mFile);
    // Default to a local file URI.
    if (p->getUri()->getScheme().size() == 0)
      p = Path::get("file://" + mFile);
    // serial_op_type * sot = new serial_op_type(p,
    // 					      mFieldSeparator,
    // 					      mRecordSeparator,
    // 					      getOutput(0)->getRecordType(),
    // 					      mFormat,
    // 					      mCommentLine.c_str());
    // sot->setSkipHeader(mSkipHeader);
    serial_op_type * sot = new serial_op_type(p,
    					      getOutput(0)->getRecordType());
    opType = sot;
  } else {
    text_op_type * tot = new text_op_type(mFile,
					  getOutput(0)->getRecordType());
    opType = tot;
  }
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);  
}
