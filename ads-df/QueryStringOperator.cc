#include <boost/algorithm/string/trim.hpp>
#include <boost/tokenizer.hpp>
#include "QueryStringOperator.hh"

class QueryStringOperator : public RuntimeOperatorBase<QueryStringOperatorType>
{
private:
  enum State { START, READ, WRITE, WRITE_EOF };
  State mState;  
  RecordBuffer mBuffer;
  RecordBuffer mOutput;
  RecordBuffer mTemporary;
  InterpreterContext * mRuntimeContext;
  // For processing name-value pairs (headers or query string
  // parameters).
  enum DecodeState { PERCENT_DECODE_START, PERCENT_DECODE_x, PERCENT_DECODE_xx };
  DecodeState mDecodeState;
  char mDecodeChar;
  std::string mField;
  std::string mValue;
  QueryStringParser<QueryStringOperator> mParser;

  void parseQueryString();

public:
  QueryStringOperator(RuntimeOperator::Services& services, 
		      const QueryStringOperatorType& opType)
    :
    RuntimeOperatorBase<QueryStringOperatorType>(services, opType),
    mState(START),
    mRuntimeContext(new InterpreterContext()),
    mDecodeState(PERCENT_DECODE_START),
    mParser(this)
  {
  }

  ~QueryStringOperator()
  {
    delete mRuntimeContext;
  }

  /**
   * intialize.
   */
  void start()
  {
    mState = START;
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      while(true) {
	requestRead(0);
	mState = READ;
	return;
      case READ:
	read(port, mBuffer);
	if (RecordBuffer::isEOS(mBuffer)) {
	  break;
	}

	// Parse the field...
	parseQueryString();
	getMyOperatorType().mFree.free(mBuffer);
	mBuffer = RecordBuffer();

	requestWrite(0);
	mState = WRITE;
	return;
      case WRITE:
	write(port, mOutput, false);
	mOutput = RecordBuffer();
      }

      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(), true);
    }
  }

  void shutdown()
  {
  }  

  int32_t onQueryStringField(const char * c, size_t length, bool done);
  int32_t onQueryStringValue(const char * c, size_t length, bool done);
  int32_t onQueryStringComplete()
  {
    // TODO: Should we handle parsing multiple query strings?
    return 0;
  }
};

int32_t QueryStringOperator::onQueryStringField(const char * c, size_t length,
						bool done)
{
  // Like onHeaderField except we urldecode 
  mField.reserve(mField.size() + length);
  const char * end = c + length;
  for(; c != end; ++c) {
    switch(mDecodeState) {
    case PERCENT_DECODE_START:
      switch(*c) {
      case '+':
	mField.push_back(' ');
	break;
      case '%':
	mDecodeState = PERCENT_DECODE_x;
	break;
      default:
	mField.push_back(*c);
	break;
      }	
      break;
    case PERCENT_DECODE_x:
      if (*c >= '0' && *c <= '9') {
	mDecodeChar = (char) (*c - '0');
      } else {
	char lower = *c | 0x20;
	if (lower >= 'a' && lower <= 'f') {
	  mDecodeChar = (char) (lower - 'a' + 10);
	} else {
	  // TODO: bogus: try to recover
	  throw std::runtime_error("Invalid post body");
	}
      }
      mDecodeState = PERCENT_DECODE_xx;
      break;
    case PERCENT_DECODE_xx:
      if (*c >= '0' && *c <= '9') {
	mField.push_back((char) ((mDecodeChar<<4) + *c - '0'));
      } else {
	char lower = *c | 0x20;
	if (lower >= 'a' && lower <= 'f') {
	  mField.push_back((char) ((mDecodeChar<<4) + lower - 'a' + 10));
	} else {
	  // TODO: bogus: try to recover
	  throw std::runtime_error("Invalid post body");
	}
      }
      mDecodeState = PERCENT_DECODE_START;
      break;
    }
  }
  // Check if we are processing this field.  If not clear the
  // field so we don't bother processing the value.
  if (done &&
      getMyOperatorType().mQueryStringFields.end() ==
      getMyOperatorType().mQueryStringFields.find(mField)) {
    mField.clear();
  }
  return 0;
}

int32_t QueryStringOperator::onQueryStringValue(const char * c, size_t length,
						bool done)
{
  if (mField.size() == 0)
    return 0;
  // Like onHeaderField except we urldecode 
  mValue.reserve(mValue.size() + length);
  const char * end = c + length;
  for(; c != end; ++c) {
    switch(mDecodeState) {
    case PERCENT_DECODE_START:
      switch(*c) {
      case '+':
	mValue.push_back(' ');
	break;
      case '%':
	mDecodeState = PERCENT_DECODE_x;
	break;
      default:
	mValue.push_back(*c);
	break;
      }	
      break;
    case PERCENT_DECODE_x:
      if (*c >= '0' && *c <= '9') {
	mDecodeChar = (char) (*c - '0');
      } else {
	char lower = *c | 0x20;
	if (lower >= 'a' && lower <= 'f') {
	  mDecodeChar = (char) (lower - 'a' + 10);
	} else {
	  // TODO: bogus: try to recover
	  throw std::runtime_error("Invalid post body");
	}
      }
      mDecodeState = PERCENT_DECODE_xx;
      break;
    case PERCENT_DECODE_xx:
      if (*c >= '0' && *c <= '9') {
	mValue.push_back((char) ((mDecodeChar<<4) + *c - '0'));
      } else {
	char lower = *c | 0x20;
	if (lower >= 'a' && lower <= 'f') {
	  mValue.push_back((char) ((mDecodeChar<<4) + lower - 'a' + 10));
	} else {
	  // TODO: bogus: try to recover
	  throw std::runtime_error("Invalid post body");
	}
      }
      mDecodeState = PERCENT_DECODE_START;
      break;
    }
  }
  if (done) {
    // TODO: Handle empty value
    QueryStringTempRecord::const_iterator it = 
      getMyOperatorType().mQueryStringFields.find(mField);
    BOOST_ASSERT(mField.size() > 0);
    BOOST_ASSERT(it != getMyOperatorType().mQueryStringFields.end());
    it->second.SetVariableLengthString(mTemporary, mValue.c_str(), 
				       mValue.size());
    mField.clear();
    mValue.clear();
  }
  return 0;
}

void QueryStringOperator::parseQueryString()
{
  const QueryStringOperatorType& opType(getMyOperatorType());
  mTemporary = opType.mQueryStringFields.malloc();
  if (!opType.mQueryString.isNull(mBuffer)) {
    Varchar * input = opType.mQueryString.getVarcharPtr(mBuffer);
    mParser.parse(input->c_str(), input->size());
    mParser.parse(NULL, 0);
  }
  getMyOperatorType().mTransfer->execute(mBuffer, mTemporary, mOutput, 
					 mRuntimeContext, false, false);
  opType.mQueryStringFields.free(mTemporary);
}

QueryStringTempRecord::QueryStringTempRecord()
{
}

QueryStringTempRecord::QueryStringTempRecord(const RecordType * ty)
  :
  mMalloc(ty->getMalloc()),
  mFree(ty->getFree())
{
  for(RecordType::const_member_iterator m = ty->begin_members(),
	e = ty->end_members(); m != e; ++m) {
    const std::string & nm(m->GetName());
    mFields[nm] = ty->getFieldAddress(nm);
  }  
}

QueryStringTempRecord::~QueryStringTempRecord()
{
}

LogicalQueryString::LogicalQueryString()
  :
  LogicalOperator(1,1,1,1),
  mFieldsType(NULL),
  mTransfer(NULL)
{
}

LogicalQueryString::~LogicalQueryString()
{
  delete mTransfer;
}

void LogicalQueryString::check(PlanCheckContext& log)
{
  // All fields are parse out as VARCHAR into a temp buffer.
  std::vector<RecordMember> fields;
  std::string output;
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    if (it->equals("fields")) {
      std::string str = getStringValue(log, *it);
      typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
      boost::char_separator<char> sep(",");
      tokenizer tok(str, sep);
      for(tokenizer::iterator tokIt = tok.begin();
	  tokIt != tok.end();
	  ++tokIt) {
	fields.push_back(RecordMember(boost::trim_copy(*tokIt),
				      VarcharType::Get(log, true)));
      }
    } else if (it->equals("input")) {
      mInputField = getStringValue(log, *it);
    } else if (it->equals("output")) {
      output = getStringValue(log, *it);
    } else {
      checkDefaultParam(*it);
    }
  }

  // Make sure input field exists
  std::vector<std::string> inputFields(1, mInputField);
  checkFieldsExist(log, inputFields, 0);

  // TODO: Make sure input fields is a VARCHAR

  // By default, identity transfer all
  if (0 == output.size()) {
    output = "input.*, query.*";
  }

  // parse into this record.
  mFieldsType = RecordType::get(log, fields);
  
  std::vector<AliasedRecordType> types;
  types.push_back(AliasedRecordType("input", getInput(0)->getRecordType()));
  types.push_back(AliasedRecordType("query", mFieldsType));
  mTransfer = new RecordTypeTransfer2(log, "queryStringXfer", types, output);
  getOutput(0)->setRecordType(mTransfer->getTarget());
}

void LogicalQueryString::create(class RuntimePlanBuilder& plan)
{
  
  RuntimeOperatorType * opType = 
    new QueryStringOperatorType(mTransfer,
				getInput(0)->getRecordType(),
				mInputField,
				mFieldsType);
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

QueryStringOperatorType::QueryStringOperatorType(const RecordTypeTransfer2 * transfer,
						 const RecordType * input,
						 const std::string& queryStringField,
						 const RecordType * fields)
  :
  RuntimeOperatorType("QueryStringOperatorType"),
  mMalloc(transfer->getTarget()->getMalloc()),
  mFree(input->getFree()),
  mQueryStringFields(fields),
  mTransfer(transfer->create()),
  mQueryString(input->getFieldAddress(queryStringField))
{
}

QueryStringOperatorType::~QueryStringOperatorType()
{
  delete mTransfer;
}

RuntimeOperator * QueryStringOperatorType::create(RuntimeOperator::Services & services) const
{
  return new QueryStringOperator(services, *this);
}
