#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>

#include "RuntimeProcess.hh"
#include "HttpOperator.hh"
#include "QueryStringOperator.hh"
#include "HttpParser.hh"
#include "http_parser.h"

HttpRequestType::HttpRequestType()
{
}

HttpRequestType::HttpRequestType(const RecordType * ty)
  :
  mContentLength(ty->getFieldAddress("Content-Length")),
  mUserAgent(ty->getFieldAddress("User-Agent")),
  mContentType(ty->getFieldAddress("Content-Type")),
  mUrl(ty->getFieldAddress("RequestUrl")),
  mBody(ty->getFieldAddress("body"))
{
  for(RecordType::const_member_iterator m = ty->begin_members(),
	e = ty->end_members(); m != e; ++m) {
    mNVP[m->GetName()] = ty->getFieldAddress(m->GetName());
  }
}

HttpRequestType::~HttpRequestType()
{
}

void HttpRequestType::setContentLength(int64_t contentLength, RecordBuffer buf) const
{
  mContentLength.setInt64(contentLength, buf);
}

void HttpRequestType::appendUserAgent(const char * start, const char * end,
				      RecordBuffer buf) const
{
  appendVarchar(start, end, mUserAgent, buf);
}

void HttpRequestType::appendContentType(const char * start, const char * end, 
					RecordBuffer buf) const
{
  appendVarchar(start, end, mContentType, buf);
}

void HttpRequestType::appendUrl(const char * start, const char * end, 
		 RecordBuffer buf) const
{
  appendVarchar(start, end, mUrl, buf);
}

void HttpRequestType::appendBody(const char * start, const char * end, 
				 RecordBuffer buf) const
{
  // appendVarchar(start, end, mBody, buf);
}

bool HttpRequestType::hasField(const std::string& field) const
{
  return mNVP.find(field) != mNVP.end();
}

void HttpRequestType::setField(const std::string& field, const std::string& value,
			       RecordBuffer buf) const
{
  mNVP.find(field)->second.SetVariableLengthString(buf, value.c_str(), value.size());
}

void HttpRequestType::appendVarchar(const char * start, const char * end,
				    FieldAddress field, RecordBuffer buf) const
{
  if (field.isNull(buf)) {
    field.getVarcharPtr(buf)->assign(start, (int32_t) (end-start));
    field.clearNull(buf);
  } else {
    field.getVarcharPtr(buf)->append(start, (int32_t) (end-start));
  }
}

/**
 * The model here is that the http read operator listens on a port
 * and accepts HTTP messages.  The messages are parsed and sent as
 * records on the output port of the operator.
 * The operator internally manages multiple open sessions and writes to
 * its output when each such session has completed.  
 * Such a design creates some subtle scheduling decisions.  Name this
 * operator takes pains to make sure that when it is ready to write to
 * its output it continues to solicit and receive completion messages (e.g.
 * new connections or data arriving on an existing connection).
 *
 * Note an alternative design would be for the read operator to splice
 * in a union all operator to its output, wrap sessions in an operator
 * and then connect the session operators to the inputs of the union all
 * as they are created.  This would likely increase memory utilization
 * so we opted not to go this route.  In a world in which we wanted to
 * use dataflow operators to process a stream of request data we would 
 * likely need such a design however.
 */

class HttpSession
{
public:
  /**
   * HTTP parser callbacks
   */
  static int on_message_begin(http_parser * p);
  static int on_url(http_parser * p, const char * c, size_t length);
  static int on_status_complete(http_parser * p);
  static int on_header_field(http_parser * p, const char * c, size_t length);
  static int on_header_value(http_parser * p, const char * c, size_t length);
  static int on_headers_complete(http_parser * p);
  static int on_body(http_parser * p, const char * c, size_t length);
  static int on_message_complete(http_parser * p);
  
  /**
   * ASIO handlers
   */
  static void handleRead(HttpSession * session,
			 ServiceCompletionFifo * fifo,
			 const boost::system::error_code& error,
			 size_t bytes_transferred);
  static void handleWrite(HttpSession * session,
			  ServiceCompletionFifo * fifo,
			  const boost::system::error_code& error,
			  size_t bytes_transferred);
  /**
   * Doubly linked list so that session can be managed on queues.
   */
  typedef boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::normal_link> > link_type;
  link_type mSessionHook;
  typedef boost::intrusive::member_hook<HttpSession, 
					link_type, 
					&HttpSession::mSessionHook> SessionQueueOption;
  typedef boost::intrusive::list<HttpSession, SessionQueueOption, boost::intrusive::constant_time_size<false> > SessionQueue;

  enum State { START, READ, WRITE };
  State mState;
  boost::asio::ip::tcp::socket * mSocket;
  RecordBuffer mBuffer;
  http_parser mParser;
  http_parser_settings mParserSettings;
  bool mMessageComplete;
  bool mResponseComplete;
  // the mIO buffer points to an allocated region
  // sometimes and other times points to static text.
  bool mFreeBuffer;
  std::size_t mIOSize;
  char * mIO;
  const HttpRequestType& mRequestType;
  QueryStringParser<HttpSession> mQueryStringParser;

  // For processing name-value pairs (headers or query string
  // parameters).
  enum DecodeState { PERCENT_DECODE_START, PERCENT_DECODE_x, PERCENT_DECODE_xx };
  DecodeState mDecodeState;
  char mDecodeChar;
  std::string mField;
  std::string mValue;

  void close();
  
  HttpSession(boost::asio::ip::tcp::socket * socket,
	      RecordBuffer buf,
	      const HttpRequestType & requestType);
  ~HttpSession();
  boost::asio::ip::tcp::socket& socket()
  {
    return *mSocket; 
  }
  bool completed() const;
  bool error() const;
  RecordBuffer buffer();
  void onEvent(ServiceCompletionPort * p);

  /**
   * Parser callbacks
   */
  int32_t onMessageBegin();
  int32_t onUrl(const char * c, size_t length);
  int32_t onStatusComplete();
  int32_t onHeaderField(const char * c, size_t length);
  int32_t onHeaderValue(const char * c, size_t length);
  int32_t onHeadersComplete();
  int32_t onBody(const char * c, size_t length);
  int32_t onMessageComplete();
  int32_t onQueryStringField(const char * c, size_t length);
  int32_t onQueryStringValue(const char * c, size_t length);
};

int HttpSession::on_message_begin(http_parser * p)
{
  return ((HttpSession*) p->data)->onMessageBegin();
}
int HttpSession::on_url(http_parser * p, const char * c, size_t length)
{
  return ((HttpSession*) p->data)->onUrl(c, length);
}
int HttpSession::on_status_complete(http_parser * p)
{
  return ((HttpSession*) p->data)->onStatusComplete();
}
int HttpSession::on_header_field(http_parser * p, const char * c, size_t length)
{
  return ((HttpSession*) p->data)->onHeaderField(c, length);
}
int HttpSession::on_header_value(http_parser * p, const char * c, size_t length)
{
  return ((HttpSession*) p->data)->onHeaderValue(c, length);
}
int HttpSession::on_headers_complete(http_parser * p)
{
  return ((HttpSession*) p->data)->onStatusComplete();
}
int HttpSession::on_body(http_parser * p, const char * c, size_t length)
{
  return ((HttpSession*) p->data)->onBody(c, length);
}
int HttpSession::on_message_complete(http_parser * p)
{
  return ((HttpSession*) p->data)->onMessageComplete();
}

HttpSession::HttpSession(boost::asio::ip::tcp::socket * socket,
			 RecordBuffer buf,
			 const HttpRequestType& requestType)
  :
  mState(START),
  mSocket(socket),
  mBuffer(buf),
  mMessageComplete(false),
  mResponseComplete(false),
  mFreeBuffer(true),
  mIOSize(8192),
  mIO(new char [8192]),
  mRequestType(requestType),
  mQueryStringParser(this),
  mDecodeState(PERCENT_DECODE_START),
  mDecodeChar(0)
{
  ::http_parser_init(&mParser, HTTP_REQUEST);
  mParser.data = this;
  mParserSettings.on_message_begin = on_message_begin;
  mParserSettings.on_url = on_url;
  mParserSettings.on_status_complete = on_status_complete;
  mParserSettings.on_header_field = on_header_field;
  mParserSettings.on_header_value = on_header_value;
  mParserSettings.on_headers_complete = on_headers_complete;
  mParserSettings.on_body = on_body;
  mParserSettings.on_message_complete = on_message_complete;
}

HttpSession::~HttpSession()
{
  close();
  if (mFreeBuffer) {
    delete [] mIO;
    mIO = NULL;
  }
}

void HttpSession::close() 
{
  if (mSocket) {
    mSocket->close();
    delete mSocket;
    mSocket = NULL;
  }
}

bool HttpSession::completed() const
{
  // Request completed and response sent
  return mMessageComplete && mIOSize == 0;
}

bool HttpSession::error() const
{
  return false;
}

RecordBuffer HttpSession::buffer()
{
  return mBuffer;
}

void HttpSession::handleRead(HttpSession * session,
			     ServiceCompletionFifo * fifo,
			     const boost::system::error_code& error,
			     size_t bytes_transferred)
{
  // Just enqueue for later execution.
  RecordBuffer buf;
  buf.Ptr = (uint8_t *) session;
  fifo->write(buf);
  session->mIOSize = bytes_transferred;
}

void HttpSession::handleWrite(HttpSession * session,
			     ServiceCompletionFifo * fifo,
			     const boost::system::error_code& error,
			     size_t bytes_transferred)
{
  // Just enqueue for later execution.
  RecordBuffer buf;
  buf.Ptr = (uint8_t *) session;
  fifo->write(buf);
  BOOST_ASSERT(bytes_transferred <= session->mIOSize);
  session->mIO += bytes_transferred;
  session->mIOSize -= bytes_transferred;
}

void HttpSession::onEvent(ServiceCompletionPort * p)
{
  switch(mState) {
  case START:
  while(!mMessageComplete) {
    // Read and process request
    mSocket->async_read_some(boost::asio::buffer(mIO, 8192),
			     boost::bind(&HttpSession::handleRead, 
					 this,
					 &p->getFifo(),
					 boost::asio::placeholders::error,
					 boost::asio::placeholders::bytes_transferred));
    mState = READ;
    return;
  case READ:
    {
      size_t sz = ::http_parser_execute(&mParser, &mParserSettings, mIO,
					mIOSize);
      if (mParser.http_errno != HPE_OK) {
	// TODO: Just error out this request
	throw std::runtime_error("Invalid request");	
      }
      if (mParser.upgrade) {
	// TODO: Just error out this request
	throw std::runtime_error("Not supporting protocol upgrade");
      }
    }
  }
  // Send response
  static const char * resp200 = "HTTP/1.1 200 OK\r\n";
  static const char * resp403 = "HTTP/1.1 403 Forbidden\r\n";
  static const char * resp405 = "HTTP/1.1 405 Method Not Allowed\r\n";
  static const char * resp500 = "HTTP/1.1 500 Internal Server Error\r\n";
  delete [] mIO;
  mFreeBuffer = false;
  mIO = const_cast<char *>(resp200);
  mIOSize = ::strlen(mIO);
  while(mIOSize > 0) {
    // Read and process request
    mSocket->async_write_some(boost::asio::buffer(mIO, mIOSize),
			     boost::bind(&HttpSession::handleWrite, 
					 this,
					 &p->getFifo(),
					 boost::asio::placeholders::error,
					 boost::asio::placeholders::bytes_transferred));
    mState = WRITE;
    return;
  case WRITE:;
  }
  close();
  }
}

int32_t HttpSession::onMessageBegin()
{
  return 0;
}

int32_t HttpSession::onUrl(const char * c, size_t length)
{
  mRequestType.appendUrl(c, c+length, mBuffer);
  return 0;
}

int32_t HttpSession::onStatusComplete()
{
  return 0;
}

int32_t HttpSession::onHeaderField(const char * c, size_t length)
{
  if (mValue.size() > 0) {
    // TODO: Handle empty value
    BOOST_ASSERT(mField.size() > 0);
    BOOST_ASSERT(mRequestType.hasField(mField));
    mRequestType.setField(mField, mValue, mBuffer);
    mField.clear();
    mValue.clear();
  }
  mField.append(c, length);
  return 0;
}

int32_t HttpSession::onHeaderValue(const char * c, size_t length)
{
  if (mField.size() > 0) {
    if (mRequestType.hasField(mField)) {
      mValue.append(c, length);
    } else {
      mField.clear();
    }
  }
  return 0;
}

int32_t HttpSession::onHeadersComplete()
{
  return 0;
}

int32_t HttpSession::onBody(const char * c, size_t length)
{
  mQueryStringParser.parse(c, length);
  mRequestType.appendBody(c, c+length, mBuffer);
  return 0;
}

int32_t HttpSession::onMessageComplete()
{
  mMessageComplete = true;
  return 0;
}

int32_t HttpSession::onQueryStringField(const char * c, size_t length)
{
  if (mValue.size() > 0) {
    // TODO: Handle empty value
    BOOST_ASSERT(mField.size() > 0);
    BOOST_ASSERT(mRequestType.hasField(mField));
    mRequestType.setField(mField, mValue, mBuffer);
    mField.clear();
    mValue.clear();
  }
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
  return 0;
}

int32_t HttpSession::onQueryStringValue(const char * c, size_t length)
{
  if (mField.size() == 0)
    return 0;
  if (!mRequestType.hasField(mField)) {
    mField.clear();
    return 0;
  }
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
  return 0;
}

class HttpReadOperator : public RuntimeOperatorBase<HttpReadOperatorType>
{
private:
  enum State { START, WAIT, WRITE_EOF };
  State mState;  
  boost::asio::ip::tcp::acceptor * mAcceptor;
  // Sessions that are still processing requests
  HttpSession::SessionQueue mOpenSessions;
  // Sessions that are ready to write
  HttpSession::SessionQueue mCompletedSessions;
  RecordBuffer mBuffer;
  boost::asio::io_service& mIOService;

public:
  HttpReadOperator(RuntimeOperator::Services& services, 
		  const HttpReadOperatorType& opType)
    :
    RuntimeOperatorBase<HttpReadOperatorType>(services, opType),
    mIOService(services.getIOService())
  {
    using boost::asio::ip::tcp;
    mAcceptor = new tcp::acceptor(mIOService,
				  tcp::endpoint(tcp::v4(), 
						opType.mPort));
  }
  
  ~HttpReadOperator()
  {
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

  /**
   * intialize.
   */
  void start()
  {
    mState = START;
    onEvent(NULL);
  }
  
  static bool isAccept(RecordBuffer buf)
  {
    return buf.Ptr == NULL;
  }

  HttpSession& getSession(RecordBuffer buf)
  {
    return *((HttpSession *) buf.Ptr);
  }

  void accept()
  {
    // accept a connection
    using boost::asio::ip::tcp;
    HttpSession * session = new HttpSession(new tcp::socket(mIOService),
					    getMyOperatorType().mMalloc.malloc(),
					    getMyOperatorType().mRequestType);
    mAcceptor->async_accept(session->socket(),
			    boost::bind(&HttpReadOperator::handleAccept, 
					&(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo()),
					boost::asio::placeholders::error));
    mOpenSessions.push_back(*session);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      requestCompletion(0);
      accept();
      while(true) {
	mState = WAIT;
	return;
      case WAIT:
	if (port == getCompletionPorts()[0]) {
	  read(port, mBuffer);
	  // Is this a new connection or a read for a session
	  if (isAccept(mBuffer)) {
	    requestCompletion(0);
	    // Start the session and accept a new connection.
	    mOpenSessions.back().onEvent(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0]));
	    accept();
	  } else {
	    // Move session forward; if newly completed then we
	    // must request a write
	    HttpSession & session (getSession(mBuffer));
	    session.onEvent(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0]));
	    if (session.completed()) {
	      if(mCompletedSessions.empty()) {
		requestWrite(0);
	      }
	      mOpenSessions.erase(mOpenSessions.iterator_to(session));
	      mCompletedSessions.push_back(session);
	    } else if (session.error()) {
	      mOpenSessions.erase(mOpenSessions.iterator_to(session));
	      delete &session;
	    }
	    // TODO: How to shut down gracefully; I think if not listening
	    // and no more open sessions then don't request completion.
	    requestCompletion(0);
	  }
	} else {
	  // Write an output record.
	  BOOST_ASSERT(!mCompletedSessions.empty());
	  HttpSession & session(mCompletedSessions.front());
	  write(port, session.buffer(), true);
	  mCompletedSessions.pop_front();
	  delete &session;
	  if (!mCompletedSessions.empty()) {
	    requestWrite(0);
	  }
	}
      }

      // All done close up shop.
      requestWrite(0);
      mState = WRITE_EOF;
      return;
    case WRITE_EOF:
      write(port, RecordBuffer(), true);
    }
  } 

  void shutdown()
  {
    mAcceptor->close();
  }  
};

LogicalHttpRead::LogicalHttpRead()
  :
  LogicalOperator(0,0,1,1),
  mPort(0)
{
}

LogicalHttpRead::~LogicalHttpRead()
{
}

void LogicalHttpRead::check(PlanCheckContext& ctxt)
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

  // TODO: Figure out the interface here.
  // How much parsing do we do and how much do we punt.
  // Can we integrate regex to facilitate parsing query strings?
  // How about cookies?
  std::vector<RecordMember> members;
  members.push_back(RecordMember("Content-Length", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("User-Agent", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("Content-Type", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("RequestUrl", VarcharType::Get(ctxt, true))); 
  members.push_back(RecordMember("body", VarcharType::Get(ctxt, true)));

  // Query string parameters of interest
  members.push_back(RecordMember("v", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("c", VarcharType::Get(ctxt, true)));
  // TODO: Namespace for query strings?
  members.push_back(RecordMember("url", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("dE", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("cS", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("cE", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("rqS", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("rsS", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("rsE", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("sS", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("dl", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("di", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("dlS", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("dlE", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("dc", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("leS", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("leE", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("to", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("ol", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("cr", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("mt", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("mb", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("b", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("u", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("ua", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("pl", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("us", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("gh", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("t", VarcharType::Get(ctxt, true)));

  members.push_back(RecordMember("cip", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("eb", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("ev", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("ed", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("ct", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("country", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("regional_info", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("city", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("lat", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("long", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("continent", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("throughput", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("bw", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("asnum", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("network", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("network_type", VarcharType::Get(ctxt, true)));
  
  getOutput(0)->setRecordType(RecordType::get(ctxt, members));
}

void LogicalHttpRead::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = new HttpReadOperatorType(mPort, 
							 getOutput(0)->getRecordType());
  plan.addOperatorType(opType);
  plan.mapOutputPort(this, 0, opType, 0);    
}

RuntimeOperator * HttpReadOperatorType::create(RuntimeOperator::Services & services) const
{
  return new HttpReadOperator(services, *this);
}

/**
 * Work In Progress on an HTTP parser.  For now
 * I am using the Joyent one for Node.js
 */
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

