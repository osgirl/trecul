#ifndef __TCPOPERATOR_H__
#define __TCPOPERATOR_H__

#include "RuntimeOperator.hh"
#include "StreamBufferBlock.hh"

/**
 * What are the appropriate patterns here for connection establishment?
 * Should the operator encapsulate the listening/accepting?
 * Think about an HTTP interface that involves some kind of per session/message
 * handler setup vs. a long lived TCP stream-oriented thingy.
 * Also for scalability reasons we don't necessarily want to let operators
 * own the connection since we may want the connection to be shut down
 * temporarily (think of a shuffle of > 10K connections or something like
 * that).  We also want to think about delegating the buffer allocation 
 * to a service too.  Think about robustness in the face of connection
 * loss (need a protocol for that!!!)
 *
 * Should the underlying socket autonomously schedule reads or should
 * it do that only in response to external requests (on an input port)?
 * Maybe both are useful...
 */

class LogicalTcpRead : public LogicalOperator
{
private:
  unsigned short mPort;
public:
  LogicalTcpRead();
  ~LogicalTcpRead();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class TcpReadOperatorType : public RuntimeOperatorType
{
  friend class TcpReadOperator;
private:
  // Create new records
  RecordTypeMalloc mMalloc;
  RecordTypeFree mFree;
  StreamBufferBlock mStreamBufferBlock;
  // How to configure endpoint?
  // It may be OK to set a port (if we only have one operator
  // on a machine) but the address certainly doesn't go in here.
  // How do we handle multihomed machines or example.
  // OK: it's time to admit it; the motivation for building this
  // operator in the parallel execution case is pretty weak!!!!
  unsigned short mPort;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mMalloc);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mStreamBufferBlock);
    ar & BOOST_SERIALIZATION_NVP(mPort);
  }
  TcpReadOperatorType()
  {
  }  

public:
  TcpReadOperatorType(int32_t port, 
		      const RecordType * streamBlockType)
    :
    RuntimeOperatorType("TcpReadOperatorType"),
    mMalloc(streamBlockType->getMalloc()),
    mFree(streamBlockType->getFree()),
    mStreamBufferBlock(streamBlockType),
    mPort(port)
  {
  }

  ~TcpReadOperatorType()
  {
  }

  int32_t numServiceCompletionPorts() const 
  {
    return 1;
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

class LogicalTcpWrite : public LogicalOperator
{
private:
  std::string mHost;
  unsigned short mPort;
public:
  LogicalTcpWrite();
  ~LogicalTcpWrite();
  void check(PlanCheckContext& log);
  void create(class RuntimePlanBuilder& plan);  
};

class TcpWriteOperatorType : public RuntimeOperatorType
{
  friend class TcpWriteOperator;
private:
  // Create new records
  RecordTypeFree mFree;
  StreamBufferBlock mStreamBufferBlock;
  // How to configure endpoint?
  // It may be OK to set a port (if we only have one operator
  // on a machine) but the address certainly doesn't go in here.
  // How do we handle multihomed machines or example.
  // OK: it's time to admit it; the motivation for building this
  // operator in the parallel execution case is pretty weak!!!!
  std::string mHost;
  unsigned short mPort;
  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(RuntimeOperatorType);
    ar & BOOST_SERIALIZATION_NVP(mFree);
    ar & BOOST_SERIALIZATION_NVP(mStreamBufferBlock);
    ar & BOOST_SERIALIZATION_NVP(mHost);
    ar & BOOST_SERIALIZATION_NVP(mPort);
  }
  TcpWriteOperatorType()
  {
  }  

public:
  TcpWriteOperatorType(const std::string& host,
		       int32_t port, 
		       const RecordType * streamBlockType)
    :
    RuntimeOperatorType("TcpWriteOperatorType"),
    mFree(streamBlockType->getFree()),
    mStreamBufferBlock(streamBlockType),
    mHost(host),
    mPort(port)
  {
  }

  ~TcpWriteOperatorType()
  {
  }

  int32_t numServiceCompletionPorts() const 
  {
    return 1;
  }

  RuntimeOperator * create(RuntimeOperator::Services & services) const;
};

#endif
