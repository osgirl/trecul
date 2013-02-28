#ifndef __HTTP_PARSER_H__
#define __HTTP_PARSER_H__

#include "AsyncRecordParser.hh"

/**
 * Async parser for HTTP requests.
 */
class HttpParserBase
{
public:
  HttpParserBase() {}
  ~HttpParserBase() {}

  static char lower(char c) 
  {
    return ((uint8_t) c) | 0x20;
  }

  static bool isAlpha(char c) 
  {
    // make lower
    uint8_t ch = lower(c);
    return c >= 'a' && c <= 'z';
  }

  // RFC3986
  static bool isUnreserved(char c)
  {
    return isAlpha(c) || isDigit(c) || c == '.' || c == '-'
      || c == '_' || c == '~';
  }

  static bool isSubdelim(char c)
  {
    switch(c) {
    case '!':
    case '$':
    case '&':
    case '\'':
    case '(':
    case ')':
    case '*':
    case '+':
    case ',':
    case ';':
    case '=':
      return true;
    default:
      return false;
    }
  }

  static bool isPathChar(char c)
  {
    return isUnreserved(c) || isSubdelim(c) || c == ':' || c == '@';
  }

  static bool isDigit(char c) 
  {
    return c >= '0' && c <= '9';
  }

  static bool isHost(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return isAlpha(c) || isDigit(c) || c == '.' || c == '-';
  }

  static bool isScheme(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return  isAlpha(c) || isDigit(c) || c == '+' ||
      c == '-' || c == '.';
  }

  static bool isChar(const AsyncDataBlock& source)
  {
    uint8_t c = *source.begin();
    return c <= 127;
  }

  static bool isCtl(const AsyncDataBlock& source)
  {
    uint8_t c = *source.begin();
    return c <= 31 || c == 127;
  }

  static bool isSeparator(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    switch (c) {
    case '(': case ')': case '<': case '>': case '@':
    case ',': case ';': case ':': case '\\': case '"':
    case '/': case '[': case ']': case '?': case '=':
    case '{': case '}': case ' ': case '\t':
      return true;
    default:
      return false;
    }
  }

  static bool isAlpha(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return isAlpha(c);
  }

  static bool isDigit(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return isDigit(c);
  }

  static bool isHexDigit(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return (c >= '0' && c <= '9') || isAlpha(c);
  }

  static bool isToken(const AsyncDataBlock& source)
  {
    return isChar(source) && !isCtl(source) && !isSeparator(source);
  }
  
  static bool isPathChar(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return isPathChar(c);
  }

  static bool isWhitespace(const AsyncDataBlock& source)
  {
    char c = *((const char *) source.begin());
    return c == ' ' || c == '\r' || c == '\n';
  }
};

class HttpRequestLineParser : public HttpParserBase
{
private:
  enum State { METHOD_START,
	       METHOD,
	       URI_START,
	       URI_SCHEME,
	       URI_SCHEME_SLASH,
	       URI_SCHEME_SLASH_SLASH,
	       URI_HOST_START,
	       URI_HOST,
	       URI_HOST_END,
	       URI_HOST_IP_LITERAL,
	       URI_PORT,
	       URI_PATH,
	       QUERY_STRING,
	       VERSION_HTTP_H,
	       VERSION_HTTP_HT,
	       VERSION_HTTP_HTT,
	       VERSION_HTTP_HTTP,
	       VERSION_HTTP_SLASH,
	       VERSION_HTTP_MAJOR_START,
	       VERSION_HTTP_MAJOR,
	       VERSION_HTTP_MINOR_START,
	       VERSION_HTTP_MINOR,
	       NEWLINE,
	       CR};
  State mState;
public:
  HttpRequestLineParser();
  ~HttpRequestLineParser();
  ParserState import(AsyncDataBlock& source, RecordBuffer target);  
};

#endif
