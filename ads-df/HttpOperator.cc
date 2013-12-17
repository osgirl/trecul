#include <deque>
#include <boost/algorithm/string/trim.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/format.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/tokenizer.hpp>

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
  mBody(ty->getFieldAddress("body")),
  mMalloc(ty->getMalloc()),
  mFree(ty->getFree())
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

const char * HttpRequestType::getUrl(RecordBuffer buf) const
{
  Varchar * v = mUrl.getVarcharPtr(buf);
  return v != NULL ? v->c_str() : NULL;
}

RecordBuffer HttpRequestType::malloc() const
{
  return mMalloc.malloc();
}

void HttpRequestType::free(RecordBuffer buf) const
{
  mFree.free(buf);
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

class HttpSessionCallback
{
public:
  virtual ~HttpSessionCallback() {}
  virtual void onReadWriteRequest() =0;
  virtual void onReadWriteCompletion(RecordBuffer buf) =0;
};

class HttpSession
{
private:
  void setResponse(const char * resp);
  void setResponseOK();
  void setResponseBadRequest();
  void setResponseForbidden();
  void setResponseNotFound();
  void setResponseMethodNotAllowed();
  void setResponseInternalServerError();
  void setResponseNotImplemented();
  
  std::string mPostResource;
  std::string mAliveResource;

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
			 HttpSessionCallback * fifo,
			 const boost::system::error_code& error,
			 size_t bytes_transferred);
  static void handleWrite(HttpSession * session,
			  HttpSessionCallback * fifo,
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

  boost::asio::ip::tcp::socket * mSocket;
  RecordBuffer mConstructing;
  std::deque<RecordBuffer> mCompletedBuffers;
  http_parser mParser;
  http_parser_settings mParserSettings;
  std::size_t mIOSize;
  char * mIO;
  const HttpRequestType& mRequestType;
  QueryStringParser<HttpSession> mQueryStringParser;
  std::string mField;
  std::string mValue;
  int32_t mQueueIndex;
  char mDecodeChar;
  enum State { START, READ, WRITE };
  State mState : 2;
  bool mMessageComplete : 1;
  bool mResponseComplete : 1;
  // the mIO buffer points to an allocated region
  // sometimes and other times points to static text.
  bool mFreeBuffer : 1;
  enum UrlState { URL_UNVALIDATED=0, URL_VALID, URL_INVALID };
  UrlState mUrlState : 2;

  // For processing name-value pairs (headers or query string
  // parameters).
  enum DecodeState { PERCENT_DECODE_START=0, PERCENT_DECODE_x, PERCENT_DECODE_xx };
  DecodeState mDecodeState : 2;

  void close();
  void validateUrl();
  
  HttpSession(boost::asio::ip::tcp::socket * socket,
	      const std::string& postResource,
	      const std::string& aliveResource,
	      const HttpRequestType & requestType);
  ~HttpSession();
  boost::asio::ip::tcp::socket& socket()
  {
    return *mSocket; 
  }
  bool completed() const;
  bool error() const;
  std::deque<RecordBuffer> & buffers();
  void onEvent(HttpSessionCallback * p);

  int32_t getQueueIndex() const
  {
    return mQueueIndex;
  }
  void setQueueIndex(int32_t queueIndex)
  {
    mQueueIndex = queueIndex;
  }

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
  int32_t onQueryStringField(const char * c, size_t length, bool done);
  int32_t onQueryStringValue(const char * c, size_t length, bool done);
  int32_t onQueryStringComplete();
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
  return ((HttpSession*) p->data)->onHeadersComplete();
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
			 const std::string& postResource,
			 const std::string& aliveResource,
			 const HttpRequestType& requestType)
  :
  mPostResource(postResource),
  mAliveResource(aliveResource),
  mSocket(socket),
  mIOSize(8192),
  mIO(new char [8192]),
  mRequestType(requestType),
  mQueryStringParser(this),
  mQueueIndex(0),
  mDecodeChar(0),
  mState(START),
  mMessageComplete(false),
  mResponseComplete(false),
  mFreeBuffer(true),
  mUrlState(URL_UNVALIDATED),
  mDecodeState(PERCENT_DECODE_START)
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
  mConstructing = mRequestType.malloc();
}

HttpSession::~HttpSession()
{
  close();
  if (mFreeBuffer) {
    delete [] mIO;
    mIO = NULL;
  }
  if (mConstructing != RecordBuffer()) {
    mRequestType.free(mConstructing);
  }
  for(std::deque<RecordBuffer>::iterator b = mCompletedBuffers.begin(),
  	e = mCompletedBuffers.end(); b != e; ++b) {
    mRequestType.free(*b);
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

void HttpSession::validateUrl()
{
  if (mUrlState == URL_UNVALIDATED) {
    const char * c = mRequestType.getUrl(mConstructing);
    if (c && 
	((boost::algorithm::iequals(c, mPostResource) && mParser.method == HTTP_POST) ||
	 (boost::algorithm::iequals(c, mAliveResource) && mParser.method == HTTP_GET))) {
      mUrlState = URL_VALID;
    } else {
      mUrlState = URL_INVALID;
    }
  }
}

bool HttpSession::completed() const
{
  // Request completed and response sent
  return mMessageComplete && mIOSize == 0;
}

bool HttpSession::error() const
{
  // Don't return error() until we are done with the response.
  // TODO: Perhaps we should rename this since we are calling
  // an aliveness ping an error so that the operator doesn't output
  // anything.
  return (mSocket == NULL && (mParser.http_errno != HPE_OK || 
			      mUrlState == URL_INVALID ||
			      mParser.method == HTTP_GET)) ||
    (mSocket != NULL && !mSocket->is_open());
}

std::deque<RecordBuffer>& HttpSession::buffers()
{
  return mCompletedBuffers;
}

void HttpSession::handleRead(HttpSession * session,
			     HttpSessionCallback * fifo,
			     const boost::system::error_code& error,
			     size_t bytes_transferred)
{
  // Just enqueue for later execution.
  RecordBuffer buf;
  buf.Ptr = (uint8_t *) session;
  fifo->onReadWriteCompletion(buf);
  session->mIOSize = bytes_transferred;
  if (error) {
    session->mSocket->close();
  }
}

void HttpSession::handleWrite(HttpSession * session,
			     HttpSessionCallback * fifo,
			     const boost::system::error_code& error,
			     size_t bytes_transferred)
{
  // Just enqueue for later execution.
  RecordBuffer buf;
  buf.Ptr = (uint8_t *) session;
  fifo->onReadWriteCompletion(buf);
  BOOST_ASSERT(bytes_transferred <= session->mIOSize);
  session->mIO += bytes_transferred;
  session->mIOSize -= bytes_transferred;
  if (error) {
    session->mSocket->close();
  }
}

void HttpSession::setResponse(const char * resp)
{
  delete [] mIO;
  mFreeBuffer = false;
  mIO = const_cast<char *>(resp);
  mIOSize = ::strlen(mIO);
}

void HttpSession::setResponseOK()
{
  static const char * resp = "HTTP/1.1 200 OK\r\n";
  setResponse(resp);
}

void HttpSession::setResponseBadRequest()
{
  static const char * resp = "HTTP/1.1 400 Bad Request\r\n";
  setResponse(resp);
}

void HttpSession::setResponseForbidden()
{
  static const char * resp = "HTTP/1.1 403 Forbidden\r\n";
  setResponse(resp);
}

void HttpSession::setResponseNotFound()
{
  static const char * resp = "HTTP/1.1 404 Not Found\r\n";
  setResponse(resp);
}

void HttpSession::setResponseMethodNotAllowed()
{
  static const char * resp = "HTTP/1.1 405 Method Not Allowed\r\n";
  setResponse(resp);
}

void HttpSession::setResponseInternalServerError()
{
  static const char * resp = "HTTP/1.1 500 Internal Server Error\r\n";
  setResponse(resp);
}

void HttpSession::setResponseNotImplemented()
{
  static const char * resp = "HTTP/1.1 501 Not Implemented\r\n";
  setResponse(resp);
}

void HttpSession::onEvent(HttpSessionCallback * p)
{
  switch(mState) {
  case START:
    while(mSocket->is_open()) {
      // Read and process request
      mSocket->async_read_some(boost::asio::buffer(mIO, 8192),
			       boost::bind(&HttpSession::handleRead, 
					   this,
					   p,
					   boost::asio::placeholders::error,
					   boost::asio::placeholders::bytes_transferred));
      p->onReadWriteRequest();
      mState = READ;
      return;
    case READ:
      {
	size_t sz = ::http_parser_execute(&mParser, &mParserSettings, mIO,
					  mIOSize);
	if (mParser.http_errno != HPE_OK) {
	  if (mUrlState == URL_INVALID) {
	    setResponseNotFound();
	  } else {
	    setResponseBadRequest();
	  }
	  break;
	} else if (mParser.upgrade) {
	  setResponseNotImplemented();
	  break;
	} else if (mMessageComplete) {
	  setResponseOK();
	  break;
	}
      }
    }

    // Send response
    while(mIOSize > 0 && mSocket->is_open()) {
      // Read and process request
      mSocket->async_write_some(boost::asio::buffer(mIO, mIOSize),
				boost::bind(&HttpSession::handleWrite, 
					    this,
					    p,
					    boost::asio::placeholders::error,
					    boost::asio::placeholders::bytes_transferred));
      p->onReadWriteRequest();
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
  mRequestType.appendUrl(c, c+length, mConstructing);
  return 0;
}

int32_t HttpSession::onStatusComplete()
{
  return 0;
}

int32_t HttpSession::onHeaderField(const char * c, size_t length)
{
  validateUrl();
  if (mUrlState == URL_INVALID) {
    return -1;
  }
  if (mValue.size() > 0) {
    // TODO: Handle empty value
    BOOST_ASSERT(mField.size() > 0);
    BOOST_ASSERT(mRequestType.hasField(mField));
    mRequestType.setField(mField, mValue, mConstructing);
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
  validateUrl();
  if (mUrlState == URL_INVALID) {
    return -1;
  }
  if (mField.size() > 0) {
    mRequestType.setField(mField, mValue, mConstructing);
    mField.clear();
    mValue.clear();
  }
  return 0;
}

int32_t HttpSession::onBody(const char * c, size_t length)
{
  int32_t ret = mQueryStringParser.parse(c, length);
  mRequestType.appendBody(c, c+length, mConstructing);
  return ret;
}

int32_t HttpSession::onMessageComplete()
{
  if (mConstructing != RecordBuffer()) {
    mCompletedBuffers.push_back(mConstructing);
    mConstructing = RecordBuffer();
  }
  BOOST_ASSERT(mCompletedBuffers.size() > 0);
  mMessageComplete = true;
  return 0;
}

int32_t HttpSession::onQueryStringField(const char * c, size_t length, 
					bool done)
{
  if (mConstructing == RecordBuffer()) {
    mConstructing = mRequestType.malloc();
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
	  return -1;
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
	  return -1;
	}
      }
      mDecodeState = PERCENT_DECODE_START;
      break;
    }
  }
  if (done && !mRequestType.hasField(mField)) {
    mField.clear();
    return 0;
  }
  return 0;
}

int32_t HttpSession::onQueryStringValue(const char * c, size_t length,
					bool done)
{
  if (mConstructing == RecordBuffer()) {
    mConstructing = mRequestType.malloc();
  }
  std::string tmp(c,length);
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
	  return -1;
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
	  return -1;
	}
      }
      mDecodeState = PERCENT_DECODE_START;
      break;
    }
  }
  if (done) {
    BOOST_ASSERT(mField.size() > 0);
    BOOST_ASSERT(mRequestType.hasField(mField));
    mRequestType.setField(mField, mValue, mConstructing);
    mField.clear();
    mValue.clear();
  }
  return 0;
}

int32_t HttpSession::onQueryStringComplete()
{
  mCompletedBuffers.push_back(mConstructing);
  mConstructing = RecordBuffer();
  return 0;
}

class HttpReadOperator : public RuntimeOperatorBase<HttpReadOperatorType>, public HttpSessionCallback
{
private:
  enum State { START, WAIT, WRITE_EOF };
  State mState;  
  boost::asio::ip::tcp::acceptor * mAcceptor;
  HttpSession * mAcceptingSession;
  // Sessions that are still processing requests and
  // have no output records available.
  HttpSession::SessionQueue mOpenSessions;
  // Sessions that are still parsing but have an output
  // record ready.
  HttpSession::SessionQueue mOutputReadySessions;
  // Sessions that are done parsing
  HttpSession::SessionQueue mCompletedSessions;
  RecordBuffer mBuffer;
  boost::asio::io_service& mIOService;
  boost::asio::signal_set mShutdownSignal;
  int32_t mNumAsyncRequests;
  bool mListening;

  ServiceCompletionFifo * getServiceCompletionFifo()
  {
    return &(dynamic_cast<ServiceCompletionPort*>(getCompletionPorts()[0])->getFifo());
  }

public:
  HttpReadOperator(RuntimeOperator::Services& services, 
		  const HttpReadOperatorType& opType)
    :
    RuntimeOperatorBase<HttpReadOperatorType>(services, opType),
    mIOService(services.getIOService()),
    mShutdownSignal(services.getIOService(), SIGURG),
    mNumAsyncRequests(0),
    mListening(true)
  {
    // Bind with reuse_addr = true (i.e. set sockopt SO_REUSEADDR).
    using boost::asio::ip::tcp;
    mAcceptor = new tcp::acceptor(mIOService,
				  tcp::endpoint(tcp::v4(), 
						opType.mPort),
				  true);
  }
  
  ~HttpReadOperator()
  {
    delete mAcceptor;
  }

  void handleAccept(ServiceCompletionFifo * fifo,
		    const boost::system::error_code& error)
  {
    if (error) {
      std::cerr << "Received error in asynchronous accept: " << error << std::endl;
      // Communicate error by clearing session.
      delete mAcceptingSession;
      mAcceptingSession = NULL;
    }
    // Post the response into the scheduler
    RecordBuffer buf;
    // TODO: Error handling...
    onReadWriteCompletion(buf);
  }

  void handleShutdown(const boost::system::error_code& error,
		      int signal_number)
  {
    std::cout << "Received shutdown request (signal number = " << signal_number << ")" << std::endl;
    if (mAcceptor) {
      boost::system::error_code ec;
      mAcceptor->close(ec);
      if (ec) {
	std::cerr << "Failed cancelling accept" << std::endl;
      }
    }
    mListening = false;
    // Just decrement directly instead of calling onReadWriteCompletion
    // since we have nothing to post.
    mNumAsyncRequests -= 1;
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

  void waitForShutdown()
  {
    mShutdownSignal.async_wait(boost::bind(&HttpReadOperator::handleShutdown, this, 
					   boost::asio::placeholders::error, 
					   boost::asio::placeholders::signal_number));
    onReadWriteRequest();
  }

  void accept()
  {
    // accept a connection
    if (mListening) {
      using boost::asio::ip::tcp;
      HttpSession * session = new HttpSession(new tcp::socket(mIOService),
					      getMyOperatorType().mPostResource,
					      getMyOperatorType().mAliveResource,
					      getMyOperatorType().mRequestType);
      mAcceptor->async_accept(session->socket(),
			      boost::bind(&HttpReadOperator::handleAccept, 
					  this,
					  getServiceCompletionFifo(),
					  boost::asio::placeholders::error));
      onReadWriteRequest();
      mAcceptingSession = session;
    } else {
      std::cout << "Turned off listening on port" << std::endl;
    }
  }

  bool requestIOs()
  {
    // TODO: Figure out an abstraction for this horrifically 
    // non intuitive issue: the requestIO method may link the 
    // completion port and output port to make sure they are both
    // dequeued from the scheduler request queues when one of them
    // is scheduled.  If they remain linked subsequent calls will 
    // relink them.  Rethink the API here.
    getCompletionPorts()[0]->request_unlink();
    // END TODO
    uint32_t requestState(0);
    if (mNumAsyncRequests > 0) {
      requestState |= 1;
    }
    if (!mOutputReadySessions.empty() ||
	!mCompletedSessions.empty()) {
      // Want to output a record.
      requestState |= 2;
    }
    switch(requestState) {
    case 0:
      std::cout << "HttpOperator::requestIOs: No longer accepting new connections and no open sessions. Shutting down operator." << std::endl;
      return false;
    case 1:
      requestCompletion(0);
      return true;
    case 2:
      requestWrite(0);
      return true;
    case 3:
      requestIO(*getCompletionPorts()[0], *getOutputPorts()[0]);
      return true;
    }
  }

  void dequeue(HttpSession& session)
  {
    switch(session.getQueueIndex()) {
    case 1:
      mOpenSessions.erase(mOpenSessions.iterator_to(session));
      break;
    case 2:
      mOutputReadySessions.erase(mOutputReadySessions.iterator_to(session));
      break;
    case 3:
      mCompletedSessions.erase(mCompletedSessions.iterator_to(session));
      break;
    default:
      break;
    }
    session.setQueueIndex(0);
  }

  void enqueueOpen(HttpSession& session)
  {
    if (session.getQueueIndex() == 1) return;
    dequeue(session);
    mOpenSessions.push_back(session);
    session.setQueueIndex(1);
  }

  void enqueueOutputReady(HttpSession& session)
  {
    if (session.getQueueIndex() == 2) return;
    dequeue(session);
    mOutputReadySessions.push_back(session);
    session.setQueueIndex(2);
  }

  void enqueueCompleted(HttpSession& session)
  {
    if (session.getQueueIndex() == 3) return;
    dequeue(session);
    mCompletedSessions.push_back(session);
    session.setQueueIndex(3);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      waitForShutdown();
      requestIOs();
      accept();
      do {
	mState = WAIT;
	return;
      case WAIT:
	if (port == getCompletionPorts()[0]) {
	  read(port, mBuffer);
	  // Is this a new connection or a read for a session
	  if (isAccept(mBuffer)) {
	    // Start the session and accept a new connection.	    
	    if (mAcceptingSession) {
	      mAcceptingSession->onEvent(this);
	      enqueueOpen(*mAcceptingSession);
	      mAcceptingSession = NULL;
	    }
	    accept();
	  } else {
	    // Move session forward; if newly completed then we
	    // must request a write
	    HttpSession & session (getSession(mBuffer));
	    session.onEvent(this);
	    if (session.buffers().size() > 0) {
	      enqueueOutputReady(session);
	    } else if (session.error()) {
	      dequeue(session);
	      // TODO: Logging....
	      delete &session;
	    } else if (session.completed()) {
	      if (session.buffers().size() == 0) {
		// Quite likely that we don't have an output here.  The output
		// was already sent and we have been writing our HTTP response since
		// that point.
		dequeue(session);
		delete &session;
	      } else {
		enqueueCompleted(session);
	      }
	    }
	  }
	} else {
	  // Write an output record.
	  // TODO: Priority?
	  BOOST_ASSERT(!mCompletedSessions.empty() ||
		       !mOutputReadySessions.empty());
	  HttpSession & session(mOutputReadySessions.empty() ?
				mCompletedSessions.front() :
				mOutputReadySessions.front());
	  BOOST_ASSERT(session.buffers().size() > 0);
	  write(port, session.buffers().front(), false);
	  session.buffers().pop_front();
	  if (0 == session.buffers().size()) {
	    if (session.completed()) {
	      dequeue(session);
	      delete &session;
	    } else {
	      enqueueOpen(session);
	    }
	  }
	}
	// Make next request for IO based on current server state.	
      } while(requestIOs());

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

  void onReadWriteRequest()
  {
    mNumAsyncRequests += 1;
  }

  void onReadWriteCompletion(RecordBuffer buf)
  {
    getServiceCompletionFifo()->write(buf);
    BOOST_ASSERT(mNumAsyncRequests > 0);
    mNumAsyncRequests -= 1;
  }
};

LogicalHttpRead::LogicalHttpRead()
  :
  LogicalOperator(0,0,1,1),
  mPort(0),
  mPostResource("/"),
  mAliveResource("/alive")
{
}

LogicalHttpRead::~LogicalHttpRead()
{
}

void LogicalHttpRead::check(PlanCheckContext& ctxt)
{
  std::size_t cap = 8192;

  // Intrinsic Members
  // TODO: Expand the list and then allow selection from it.
  std::vector<RecordMember> members;
  members.push_back(RecordMember("Content-Length", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("User-Agent", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("Content-Type", VarcharType::Get(ctxt, true)));
  members.push_back(RecordMember("RequestUrl", VarcharType::Get(ctxt, true))); 
  members.push_back(RecordMember("body", VarcharType::Get(ctxt, true)));

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
      } else if (it->equals("fields")) {
	std::string str = getStringValue(ctxt, *it);
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	boost::char_separator<char> sep(",");
	tokenizer tok(str, sep);
	for(tokenizer::iterator tokIt = tok.begin();
	    tokIt != tok.end();
	    ++tokIt) {
	  members.push_back(RecordMember(boost::trim_copy(*tokIt),
					VarcharType::Get(ctxt, true)));
	}	
      } else if (it->equals("port")) {
	int32_t tmp = getInt32Value(ctxt, *it);
	if (tmp < 0 || tmp > std::numeric_limits<unsigned short>::max()) {
	  ctxt.logError(*this, *it, (boost::format("Invalid port specified: ") 
				     % tmp).str());
	} else {
	  mPort = (unsigned short) tmp;
	}
      } else if (it->equals("postresource")) {
	mPostResource = getStringValue(ctxt, *it);
      } else if (it->equals("aliveresource")) {
	mAliveResource = getStringValue(ctxt, *it);
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
  getOutput(0)->setRecordType(RecordType::get(ctxt, members));
}

void LogicalHttpRead::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = new HttpReadOperatorType(mPort,
							  mPostResource,
							  mAliveResource,
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

