#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>

#include "RuntimeProcess.hh"
#include "TcpOperator.hh"
#include "HttpParser.hh"

HttpRequestLineParser::HttpRequestLineParser()
  :
  mState(METHOD_START)
{
}

HttpRequestLineParser::~HttpRequestLineParser()
{
}

ParserState HttpRequestLineParser::import(AsyncDataBlock& source, 
					  RecordBuffer target)
{
  while(!source.isEmpty()) {
    switch(mState) {
    case METHOD_START:
      if (!isWhitespace(source)) {
	if(!isToken(source)) {
	  return ParserState::error(-1);
	}
	mState = METHOD;
      }
      break;
    case METHOD:
      if (isToken(source)) {
	break;
      } else if (*source.begin() == ' ') {
	mState = URI_START;
	break;
      } else {
	return ParserState::error(-1);
      }
    case URI_START:
      if (*source.begin() == ' ') {
	break;
      } else if (*source.begin() == '/') {
	mState = URI_PATH;
	break;
      } else if (isAlpha(source)) {
	mState = URI_SCHEME;
	// Fall through
      } else {
	return ParserState::error(-1);
      }
    case URI_SCHEME:
      // TODO: We should only see absolute URIs
      // in a proxy scenario (which we aren't supporting).
      // TODO: * and authority may come in here but only with
      // methods we don't support.
      // TODO: Really we should only have http
      // and doing lookahead to match http: seems to
      // be the correct way to parse.
      if (isScheme(source)) {
	break;
      } else if (*source.begin() == ':') {
	mState = URI_SCHEME_SLASH;
	break;
      } else {
	return ParserState::error(-1);
      }
    case URI_SCHEME_SLASH:
      if (*source.begin() == '/') {
	mState = URI_SCHEME_SLASH_SLASH;
	break;
      } else {
	return ParserState::error(-1);
      }
    case URI_SCHEME_SLASH_SLASH:
      if (*source.begin() == '/') {
	mState = URI_HOST_START;
	break;
      } else {
	return ParserState::error(-1);
      }
    case URI_HOST_START:
      if (*source.begin() != '[') {
	mState = URI_HOST;
	// Fall Through
      } else {
	mState = URI_HOST_IP_LITERAL;
	break;
      }
    case URI_HOST:
      if (isHost(source)) {
	break;
      }
      // Fall through when done with host chars
    case URI_HOST_END:
      if(*source.begin() == ':') {
	mState = URI_PORT;
	break;
      } else if (*source.begin() == '/') {
	mState = URI_PATH;
	break;
      } else if (*source.begin() == ' ') {
	throw std::runtime_error("TODO: Not implemented yet");
      } else {
	return ParserState::error(-1);
      }
    case URI_HOST_IP_LITERAL:
      throw std::runtime_error("TODO: Not implemented yet");
    case URI_PORT:
      if (isDigit(source)) {
	break;
      } else if (*source.begin() == '/') {
	mState = URI_PATH;
	break;
      } else if (*source.begin() == ' ') {
	throw std::runtime_error("TODO: Not implemented yet");
      } else {
	return ParserState::error(-1);
      }
    case URI_PATH:
      if (*source.begin() == ' ') {
	// TODO: Handle extra spaces before version.
	mState = VERSION_HTTP_H;
	break;
      } else if (*source.begin() == '?') {
	mState = QUERY_STRING;
	break;
      } else if (*source.begin() == '/') {
	break;
      } else if (*source.begin() == '%') {
	// TODO: This is only valid if we have a % HEXDIGIT HEXDIGIT
	break;
      } else if (isPathChar(source)) {
	break;
      } else {
	return ParserState::error(-1);
      }
      break;
    case QUERY_STRING:
      throw std::runtime_error("TODO: Not implemented yet");
      break;
    case VERSION_HTTP_H:
      if (*source.begin() == 'H') {
	mState = VERSION_HTTP_HT;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_HT:
      if (*source.begin() == 'T') {
	mState = VERSION_HTTP_HTT;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_HTT:
      if (*source.begin() == 'T') {
	mState = VERSION_HTTP_HTTP;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_HTTP:
      if (*source.begin() == 'P') {
	mState = VERSION_HTTP_SLASH;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_SLASH:
      if (*source.begin() == '/') {
	mState = VERSION_HTTP_MAJOR_START;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_MAJOR_START:
      if (isDigit(source)) {
	mState = VERSION_HTTP_MAJOR;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_MAJOR:
      if (isDigit(source)) {
	break;
      } else if(*source.begin() == '.') {
	mState = VERSION_HTTP_MINOR_START;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_MINOR_START:
      if (isDigit(source)) {
	mState = VERSION_HTTP_MINOR;
	break;
      } else {
	return ParserState::error(-1);
      }
    case VERSION_HTTP_MINOR:
      if (isDigit(source)) {
	break;
      } else if(*source.begin() == '\r') {
	mState = NEWLINE;
	break;
      } else if(*source.begin() == ' ') {
	mState = CR;
	break;
      } else if(*source.begin() == '\n') {
	goto done;
      } else {
	return ParserState::error(-1);
      }
    case CR:
      if (*source.begin() == ' ') {
	break;    
      } else if (*source.begin() == '\r') {
	mState = NEWLINE;
	break;
      } else if (*source.begin() == '\n') {
	goto done;
      }
    case NEWLINE:
      if (*source.begin() == '\n') {
	goto done;
      } else {
	return ParserState::error(-1);
      }
    }
    source.consume(1);
  }
  return ParserState::exhausted();

 done:
  source.consume(1);
  return ParserState::success();
}

class HttpRequestParser
{
private:
  enum State { HEADER };
  State mState;
  ParserState mParserState;
  HttpRequestLineParser mHeader;
public:
  ParserState import(AsyncDataBlock& source, RecordBuffer target);  
};

ParserState HttpRequestParser::import(AsyncDataBlock& source,
				      RecordBuffer target)
{
  switch(mState) {
    while(true) {
    case HEADER:
      mParserState = mHeader.import(source, target);
      if (mParserState.isError()) {	
      } else if (mParserState.isExhausted()) {
	return mParserState;
      } 
    }
  }
}

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
