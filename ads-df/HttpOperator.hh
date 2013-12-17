#ifndef __HTTPOPERATOR_H__
#define __HTTPOPERATOR_H__

#include "RuntimeOperator.hh"
#include "StreamBufferBlock.hh"

class LogicalHttpRead : public LogicalOperator
{
private:
  unsigned short mPort;
  std::string mPostResource;
  std::string mAliveResource;
public:
  LogicalHttpRead();
  ~LogicalHttpRead();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

/**
 * Interface for dealing with a dynamic record
 * corresponding to an HTTP request.
 */
class HttpRequestType
{
private:
  FieldAddress mContentLength;
  FieldAddress mUserAgent;
  FieldAddress mContentType;
  FieldAddress mUrl;
  FieldAddress mBody;
  std::map<std::string, FieldAddress> mNVP;
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mContentLength);
    ar & BOOST_SERIALIZATION_NVP(mUserAgent);
    ar & BOOST_SERIALIZATION_NVP(mContentType);
    ar & BOOST_SERIALIZATION_NVP(mUrl);
    ar & BOOST_SERIALIZATION_NVP(mBody);
    ar & BOOST_SERIALIZATION_NVP(mNVP);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
  }

  void appendVarchar(const char * start, const char * end,
		     FieldAddress field, RecordBuffer buf) const;
public:
  HttpRequestType();
  HttpRequestType(const RecordType * ty);
  ~HttpRequestType();
  void setContentLength(int64_t contentLenth, RecordBuffer buf) const;  
  void appendUserAgent(const char * start, const char * end, 
		       RecordBuffer buf) const;
  void appendContentType(const char * start, const char * end, 
			 RecordBuffer buf) const;
  void appendUrl(const char * start, const char * end, 
		 RecordBuffer buf) const;
  void appendBody(const char * start, const char * end, 
		  RecordBuffer buf) const;
  bool hasField(const std::string& field) const;
  void setField(const std::string& field, const std::string& value,
		RecordBuffer buf) const;
  const char * getUrl(RecordBuffer buf) const;
  RecordBuffer malloc() const;
  void free(RecordBuffer buf) const;
};

class HttpReadOperatorType : public RuntimeOperatorType
{
  friend class HttpReadOperator;
private:
  // Create new records
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  // Dynamic record interface to HTTP request record.
  HttpRequestType mRequestType;
  // How to configure endpoint?
  // It may be OK to set a port (if we only have one operator
  // on a machine) but the address certainly doesn't go in here.
  // How do we handle multihomed machines or example.
  // OK: it's time to admit it; the motivation for building this
  // operator in the parallel execution case is pretty weak!!!!
  unsigned short mPort;
  // We process POST requests against this
  std::string mPostResource;
  // A place for liveness queries
  std::string mAliveResource;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mRequestType);
    ar & BOOST_SERIALIZATION_NVP(mPort);
    ar & BOOST_SERIALIZATION_NVP(mPostResource);
    ar & BOOST_SERIALIZATION_NVP(mAliveResource);
  }
  HttpReadOperatorType()
  {
  }  

public:
  HttpReadOperatorType(int32_t port, 
		       const std::string& postResource,
		       const std::string& aliveResource,
		       const RecordType * output)
    :
    RuntimeOperatorType("HttpReadOperatorType"),
    mMalloc(output->getMalloc()),
    mFree(output->getFree()),
    mRequestType(output),
    mPort(port),
    mPostResource(postResource),
    mAliveResource(aliveResource)
  {
  }

  ~HttpReadOperatorType()
  {
  }

  int32_t numServiceCompletionPorts() const 
  {
    return 1;
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

#endif
