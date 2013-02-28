#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>

#include "RuntimeProcess.hh"
#include "TcpOperator.hh"

class TcpReadOperator : public RuntimeOperatorBase<TcpReadOperatorType>
{
private:
  enum State { START, ACCEPT, READ_BLOCK, WRITE_BLOCK, WRITE_EOF };
  State mState;  
  boost::asio::ip::tcp::acceptor * mAcceptor;
  boost::asio::ip::tcp::socket * mSocket;
  RecordBuffer mBuffer;

public:
  TcpReadOperator(RuntimeOperator::Services& services, 
		  const TcpReadOperatorType& opType)
    :
    RuntimeOperatorBase<TcpReadOperatorType>(services, opType)
  {
    using boost::asio::ip::tcp;
    mAcceptor = new tcp::acceptor(services.getIOService(),
				  tcp::endpoint(tcp::v4(), 
						opType.mPort));
    mSocket = new tcp::socket(services.getIOService());
  }
  
  ~TcpReadOperator()
  {
    delete mSocket;
    delete mAcceptor;
  }

  static void handleAccept(ServiceCompletionFifo * fifo,
			   const boost::system::error_code& error)
  {
    // Post the response into the scheduler
    RecordBuffer buf;
    // TODO: Error handling...
    fifo->write(buf);
  }

  static void handleRead(ServiceCompletionFifo * fifo,
			 const StreamBufferBlock * block,
			 RecordBuffer buf,
			 const boost::system::error_code& error,
			 size_t bytes_transferred)
  {
    if (!error) {
      block->setSize((int32_t) bytes_transferred, buf);
      fifo->write(buf);
    }
    else {
      block->setSize(0, buf);
      fifo->write(buf);
    }
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
      // accept a connection
      requestCompletion(0);
      mState = ACCEPT;
      mAcceptor->async_accept(*mSocket,
			      boost::bind(&TcpReadOperator::handleAccept, 
					  &(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo()),
					  boost::asio::placeholders::error));
      return;
    case ACCEPT:
      read(port, mBuffer);
      // TODO: Handle error....
      mBuffer = RecordBuffer();

      while(true) {
	requestCompletion(0);
	mState = READ_BLOCK;
	mBuffer = getMyOperatorType().mMalloc.malloc();
	mSocket->async_read_some(boost::asio::buffer(getMyOperatorType().mStreamBufferBlock.begin(mBuffer), 
						     getMyOperatorType().mStreamBufferBlock.capacity()),
				 boost::bind(&TcpReadOperator::handleRead, 
					     &dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo(),
					     &getMyOperatorType().mStreamBufferBlock,
					     mBuffer,
					     boost::asio::placeholders::error,
					     boost::asio::placeholders::bytes_transferred));
	return;
      case READ_BLOCK:
	read(port, mBuffer);
	// TODO: Is this the right way to determine when we are
	// done?
	if (0 == getMyOperatorType().mStreamBufferBlock.getSize(mBuffer)) {
	  getMyOperatorType().mFree.free(mBuffer);
	  mBuffer = RecordBuffer();
	  break;
	}
	requestWriteThrough(0);
	mState = WRITE_BLOCK;
	return;
      case WRITE_BLOCK:
	write(port, mBuffer, false);
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
    mSocket->close();
    mAcceptor->close();
  }  
};

LogicalTcpRead::LogicalTcpRead()
  :
  LogicalOperator(0,0,1,1),
  mPort(0)
{
}

LogicalTcpRead::~LogicalTcpRead()
{
}

void LogicalTcpRead::check(PlanCheckContext& ctxt)
{
  std::size_t cap = 8192;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("blockSize")) {
	int32_t tmp = getInt32Value(ctxt, *it);
	if (tmp < 0) {
	  ctxt.logError(*this, *it, (boost::format("Invalid block size "
						   "specified: ") 
				     % tmp).str());
	} else {
	  cap = (std::size_t) tmp;
	}
      } else if (it->equals("port")) {
	int32_t tmp = getInt32Value(ctxt, *it);
	if (tmp < 0 || tmp > std::numeric_limits<unsigned short>::max()) {
	  ctxt.logError(*this, *it, (boost::format("Invalid port specified: ") 
				     % tmp).str());
	} else {
	  mPort = (unsigned short) tmp;
	}
      } else {
	checkDefaultParam(*it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }

  getOutput(0)->setRecordType(StreamBufferBlock::getStreamBufferType(ctxt, cap));
}

void LogicalTcpRead::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = new TcpReadOperatorType(mPort, 
							 getOutput(0)->getRecordType());
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);    
}

RuntimeOperator * TcpReadOperatorType::create(RuntimeOperator::Services & services) const
{
  return new TcpReadOperator(services, *this);
}

class TcpWriteOperator : public RuntimeOperatorBase<TcpWriteOperatorType>
{
private:
  enum State { START, CONNECT, READ_BLOCK, WRITE_BLOCK, WRITE_EOF };
  State mState;  
  boost::asio::ip::tcp::socket * mSocket;
  RecordBuffer mBuffer;
  boost::asio::ip::tcp::socket::endpoint_type mEndpoint;
  // TODO: Should we fold these into the StreamBufferBlock????
  // Doing it this way feels very much like global state and
  // I'd rather pass the state around with messages; but does it
  // really matter?  It might if we allow multiple write requests
  // to be outstanding.
  boost::system::error_code mError;
  std::size_t mStartOffset;
public:
  TcpWriteOperator(RuntimeOperator::Services& services, 
		  const TcpWriteOperatorType& opType)
    :
    RuntimeOperatorBase<TcpWriteOperatorType>(services, opType),
    mStartOffset(0)
  {
    // TODO: Do an async resolve? Probably a good idea...
    using boost::asio::ip::tcp;
    tcp::resolver resolver(services.getIOService());
    tcp::resolver::query query(tcp::v4(), opType.mHost, 
			       boost::lexical_cast<std::string>(opType.mPort));
    tcp::resolver::iterator iterator = resolver.resolve(query);

    mSocket = new tcp::socket(services.getIOService());
    mEndpoint = *iterator;
  }
  
  ~TcpWriteOperator()
  {
    delete mSocket;
  }

  void handleConnect(ServiceCompletionFifo * fifo,
		     const boost::system::error_code& error)
  {
    // Post the response into the scheduler
    RecordBuffer buf;
    // TODO: Error handling...
    mError = error;
    fifo->write(buf);
  }

  void handleWrite(ServiceCompletionFifo * fifo,
			  const StreamBufferBlock * block,
			  RecordBuffer buf,
			  const boost::system::error_code& error,
			  size_t bytes_transferred)
  {
    mStartOffset += bytes_transferred;
    mError = error;
    fifo->write(buf);
  }

  void startWrite()
  {
    mSocket->async_write_some(boost::asio::buffer(getMyOperatorType().mStreamBufferBlock.begin(mBuffer)+mStartOffset, 
						  getMyOperatorType().mStreamBufferBlock.getSize(mBuffer)-mStartOffset),
			      boost::bind(&TcpWriteOperator::handleWrite,
					  this,
					  &dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo(),
					  &getMyOperatorType().mStreamBufferBlock,
					  mBuffer,
					  boost::asio::placeholders::error,
					  boost::asio::placeholders::bytes_transferred));
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
      // Make the connection
      requestCompletion(0);
      mState = CONNECT;
      mSocket->async_connect(mEndpoint,
			     boost::bind(&TcpWriteOperator::handleConnect, 
					 this,
					 &(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo()),
					 boost::asio::placeholders::error));
      return;
    case CONNECT:
      read(port, mBuffer);
      // TODO: Handle error....
      mBuffer = RecordBuffer();

      while(true) {
	requestRead(0);
	mState = READ_BLOCK;
	return;
      case READ_BLOCK:
	read(port, mBuffer);
	if (RecordBuffer::isEOS(mBuffer)) {
	  break;
	}
	
	mStartOffset = 0;
	while(mStartOffset < 
	      getMyOperatorType().mStreamBufferBlock.getSize(mBuffer)) {
	  requestCompletion(0);
	  mState = WRITE_BLOCK;
	  startWrite();
	  return;
	case WRITE_BLOCK:
	  {
	    RecordBuffer tmp;
	    read(port, tmp);
	    // TODO: Error handling.
	    BOOST_ASSERT(tmp.Ptr == mBuffer.Ptr);
	  }
	}
	// All done with the write (successful or not).
	getMyOperatorType().mFree.free(mBuffer);
      }
    }
  } 

  void shutdown()
  {
    mSocket->close();
  }  
};

LogicalTcpWrite::LogicalTcpWrite()
  :
  LogicalOperator(1,1,0,0),
  mPort(0)
{
}

LogicalTcpWrite::~LogicalTcpWrite()
{
}

void LogicalTcpWrite::check(PlanCheckContext& ctxt)
{
  std::size_t cap = 8192;
  // Validate the parameters
  for(const_param_iterator it = begin_params();
      it != end_params();
      ++it) {
    try {
      if (it->equals("blockSize")) {
	int32_t tmp = getInt32Value(ctxt, *it);
	if (tmp < 0) {
	  ctxt.logError(*this, *it, (boost::format("Invalid block size specified: ") 
				     % tmp).str());
	} else {
	  cap = (std::size_t) tmp;
	}
      } else if (it->equals("host")) {
	mHost = getStringValue(ctxt, *it);
      } else if (it->equals("port")) {
	int32_t tmp = getInt32Value(ctxt, *it);
	if (tmp < 0 || tmp > std::numeric_limits<unsigned short>::max()) {
	  ctxt.logError(*this, *it, (boost::format("Invalid port specified: ") 
				     % tmp).str());
	} else {
	  mPort = (unsigned short) tmp;
	}
      } else {
	checkDefaultParam(*it);
      }
    } catch(std::runtime_error& ex) {
      ctxt.logError(*this, *it, ex.what());
    }
  }
  
  // TODO: Make sure we have a stream input
}

void LogicalTcpWrite::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = new TcpWriteOperatorType(mHost,
							  mPort, 
							  getInput(0)->getRecordType());
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);    
}

RuntimeOperator * TcpWriteOperatorType::create(RuntimeOperator::Services & services) const
{
  return new TcpWriteOperator(services, *this);
}

