#ifndef __QUERYSTRINGOPERATOR_H__
#define __QUERYSTRINGOPERATOR_H__

#include "RuntimeOperator.hh"

template <typename _Processor>
class QueryStringParser
{
private:
  enum State { START, FIELD_START, FIELD, VALUE_START, VALUE };
  State mState;
  char mFieldValueSeparator;
  char mPairSeparator;
  _Processor * mProcessor;
public:
  QueryStringParser(_Processor * processor)
    :
    mState(FIELD_START),
    mFieldValueSeparator('='),
    mPairSeparator('&'),
    mProcessor(processor)
  {
  }

  std::size_t parse(const char * data, std::size_t sz)
  {
    const char * fieldMark = NULL;
    const char * valueMark = NULL;

    // Handle empty
    if (0 == sz) {
      switch(mState) {
      case VALUE_START:
      case VALUE:
	mProcessor->onQueryStringValue(NULL, 0, true);
	mState = FIELD_START;
	return 0;
      default:
	// TODO: Error instead of throw
	throw std::runtime_error("Invalid query string");
      }
    }

    // Initialize Marks
    switch(mState) {
    case FIELD:
      fieldMark = data;
      break;
    case VALUE_START:
    case VALUE:
      valueMark = data;
    default:
      break;
    }

    // Process data
    const char * end = data + sz;
    const char * c = data;
    for(; c != end; ++c) {
      switch(mState) {
      case FIELD_START:
	if (*c == ' ') {
	  break;
	} else if (*c == mPairSeparator) {
	  // TODO: Error.
	  break;
	} else {
	  if (fieldMark == NULL) {
	    fieldMark = c;
	  }
	  mState = FIELD;	  
	  break;
	}
      case FIELD:
	if (*c == mFieldValueSeparator) {
	  mState = VALUE_START;
	  mProcessor->onQueryStringField(fieldMark, c - fieldMark, true);
	  fieldMark = NULL;
	} 
	// TODO: Validate other characters?
	break;
      case VALUE_START:
	if (*c == mPairSeparator ||
	    *c == '\n') {
	  // Empty value OK call onQueryStringValue with empty
	  // string to distinguish
	  // case the field is present with empty from not present.
	  mProcessor->onQueryStringValue(c, 0, true);
	  if (*c == '\n') {
	    // This is not standard but is specific to DLR
	    // aggregation.
	    // Yuck.
	    mProcessor->onQueryStringComplete();
	  }
	  mState = FIELD_START;
	  break;
	} else {
	  valueMark = c;
	  mState = VALUE;
	  break;
	}
	// TODO: Validate other characters?
      case VALUE:
	if (*c == mPairSeparator || *c == '\n') {
	  mState = FIELD_START;
	  mProcessor->onQueryStringValue(valueMark, c - valueMark, true);
	  if (*c == '\n') {
	    // This is not standard but is specific to DLR
	    // aggregation.
	    // Yuck.
	    mProcessor->onQueryStringComplete();
	  }
	  valueMark = NULL;
	} 
	// TODO: Validate other characters?
	break;
      }  
    }

    // send any data to callback
    if (fieldMark != NULL) {
      mProcessor->onQueryStringField(fieldMark, end - fieldMark, false);
      fieldMark = NULL;
    }
    if (valueMark != NULL) {
      mProcessor->onQueryStringValue(valueMark, end - valueMark, false);
      valueMark = NULL;
    }
  }
};

class LogicalQueryString : public LogicalOperator
{
private:
  const RecordType * mFieldsType;
  RecordTypeTransfer2 * mTransfer;
  std::string mInputField;
public:
  LogicalQueryString();
  ~LogicalQueryString();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class QueryStringTempRecord
{
public:
  typedef std::map<std::string, FieldAddress>::const_iterator const_iterator;
private:
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  std::map<std::string, FieldAddress> mFields;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mFields);
  }
public:
  QueryStringTempRecord();
  QueryStringTempRecord(const RecordType * ty);
  ~QueryStringTempRecord();
  RecordBuffer malloc() const
  {
    return mMalloc.malloc();
  }
  void free(RecordBuffer buf) const
  {
    return mFree.free(buf);
  }
  const_iterator find(const std::string& field) const
  {
    return mFields.find(field);
  }
  const_iterator end() const
  {
    return mFields.end();
  }
};

class QueryStringOperatorType : public RuntimeOperatorType
{
  friend class QueryStringOperator;
private:
  // Create new records
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  QueryStringTempRecord mQueryStringFields;
  IQLTransferModule2 * mTransfer;
  FieldAddress mQueryString;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mQueryStringFields);
    ar & BOOST_SERIALIZATION_NVP(mTransfer);
    ar & BOOST_SERIALIZATION_NVP(mQueryString);
  }
  QueryStringOperatorType()
  {
  }  

public:
  QueryStringOperatorType(const RecordTypeTransfer2 * transfer,
			  const RecordType * input,
			  const std::string& queryStringField,
			  const RecordType * fields);

  ~QueryStringOperatorType();

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

#endif
