/**
 * Copyright (c) 2013, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <iostream>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>

#include "FileSystem.hh"
#include "RuntimeProcess.hh"

using boost::asio::ip::tcp;

const int32_t httpPort(9876);
std::string basicProgram("r = http_read[port=9876,fields=\"a,b,c\", postResource=\"/test\"];\n"
			 "c = copy[output=\"a,b,c\"];\n"
			 "r -> c;\n"
			 "w = write[file=\"output.txt\", mode=\"text\"];\n"
			 "c -> w;\n");

void read200Response(tcp::socket & s)
{
  char resp[128];
  boost::system::error_code ec;
  std::size_t numRead = boost::asio::read(s, boost::asio::buffer(&resp[0], 128), ec);
  BOOST_CHECK_EQUAL(17, numRead);
  resp[numRead] = 0;
  BOOST_CHECK(ec == boost::asio::error::eof);
  BOOST_CHECK(boost::algorithm::equals("HTTP/1.1 200 OK\r\n", resp));
}

void read404Response(tcp::socket & s)
{
  char resp[128];
  boost::system::error_code ec;
  std::size_t numRead = boost::asio::read(s, boost::asio::buffer(&resp[0], 128), ec);
  BOOST_CHECK_EQUAL(24, numRead);
  resp[numRead] = 0;
  BOOST_CHECK(ec == boost::asio::error::eof);
  BOOST_CHECK(boost::algorithm::equals("HTTP/1.1 404 Not Found\r\n", resp));
}

void checkOutput(const std::string& expected)
{
  std::stringstream ostr;
  boost::filesystem::fstream istr("output.txt", std::ios_base::in);
  std::copy(std::istreambuf_iterator<char>(istr),
	    std::istreambuf_iterator<char>(),
	    std::ostreambuf_iterator<char>(ostr));
  if(!boost::algorithm::equals(expected, ostr.str())) {
    std::cout << "Expected: " << expected.c_str();
    std::cout << "Got: " << ostr.str().c_str();
    BOOST_CHECK(false);
  }
  boost::filesystem::remove("output.txt");
}

class TestFile
{
private:
  boost::filesystem::path mPath;
public:
  TestFile(const std::string& contents);
  ~TestFile();
  std::string getName() const;
};

TestFile::TestFile(const std::string& contents)
  :
  mPath(FileSystem::getTempFileName())
{
  boost::filesystem::fstream ostr(mPath, std::ios_base::out);
  ostr << contents.c_str();
  ostr.close();
}

TestFile::~TestFile()
{
  boost::filesystem::remove(mPath);
}

std::string TestFile::getName() const
{
  return mPath.string();
}

class HttpOperatorProcess
{
private:
  PosixProcessFactory mProcess;
  TestFile mTestFile;
public:
  HttpOperatorProcess(const std::string& program);
  void kill();
  int32_t waitForCompletion();
};

HttpOperatorProcess::HttpOperatorProcess(const std::string& program)
  :
  mTestFile(program)
{
  boost::filesystem::path exe =
    Executable::getPath().parent_path()/boost::filesystem::path("ads-df");
  if (!boost::filesystem::exists(exe))
    throw std::runtime_error((boost::format("Couldn't find ads-df "
					    "executable: %1%.  "
					    "Check installation") % exe.string()).str());
  
  typedef boost::shared_ptr<PosixProcessInitializer> ppiptr;
  std::vector<ppiptr> v;
  v.push_back(ppiptr(new PosixPath(exe.string())));     
  v.push_back(ppiptr(new PosixArgument("--file")));
  v.push_back(ppiptr(new PosixArgument(mTestFile.getName())));
  v.push_back(boost::shared_ptr<PosixProcessInitializer>(new PosixParentEnvironment()));
  mProcess.create(v);

  // Wait for process to start
  boost::asio::io_service io_service;
  tcp::resolver resolver(io_service);
  tcp::resolver::query query(tcp::v4(), "localhost", "9876");
  tcp::resolver::iterator iterator = resolver.resolve(query);
  int32_t i=0;
  for(; i< 10; i++) {
    tcp::socket s(io_service);
    try {
      s.connect(*iterator);
      s.close();
      break;
    } catch(std::exception & e) {
    }
    boost::this_thread::sleep(boost::posix_time::seconds(1));
  }  
  if (i==10) {
    throw std::runtime_error("Failed to start server");
  }
}

void HttpOperatorProcess::kill()
{
  boost::system::error_code ec;
  mProcess.kill(SIGURG, ec);
  if (ec) {
    throw std::runtime_error("Failed to kill process");
  }
}

int32_t HttpOperatorProcess::waitForCompletion()
{
  return mProcess.waitForCompletion();
}

std::string getPostRequest(const std::string& resource, const std::string& postBody)
{
  return (boost::format("POST %3% HTTP/1.1\r\n"
			"User-Agent: curl/7.19.7 (x86_64-pc-linux-gnu) libcurl/7.19.7 OpenSSL/0.9.8k zlib/1.2.3.3 libidn/1.15\r\n"
			"Host: localhost:9876\r\n"
			"Accept: */*\r\n"
			"Content-Length: %1%\r\n"
			"Content-Type: application/x-www-form-urlencoded\r\n"
			"\r\n"
			"%2%") % postBody.size() % postBody % resource).str();
}

std::string getPostRequest(const std::string& postBody)
{
  return getPostRequest("/test", postBody);
}

BOOST_AUTO_TEST_CASE(testStartupShutdown)
{
  try {
    HttpOperatorProcess p(basicProgram);
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postOneMessage)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s(io_service);
    s.connect(*iterator);
    std::string request(getPostRequest("a=1&b=2&c=3\n"));
    boost::asio::write(s, boost::asio::buffer(request.c_str(), request.size()));
    read200Response(s);
    s.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postOneMessageTwoRecords)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s(io_service);
    s.connect(*iterator);
    std::string request(getPostRequest("a=1&b=2&c=3\n"
				       "a=4&b=5&c=6\n"));
    boost::asio::write(s, boost::asio::buffer(request.c_str(), request.size()));
    read200Response(s);
    s.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n"
		"4\t5\t6\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsSequential)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s(io_service);
    s.connect(*iterator);
    std::string request(getPostRequest("a=1&b=2&c=3\n"));
    boost::asio::write(s, boost::asio::buffer(request.c_str(), request.size()));
    read200Response(s);
    s.close();
    s.connect(*iterator);
    request = getPostRequest("a=4&b=5&c=6\n");
    boost::asio::write(s, boost::asio::buffer(request.c_str(), request.size()));
    read200Response(s);
    s.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n"
		"4\t5\t6\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsInterleaved1)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    std::string request1(getPostRequest("a=1&b=2&c=3\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), 3));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str() + 3, request1.size() - 3));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), 4));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str() + 4, request1.size() - 4));
    read200Response(s1);
    s1.close();
    read200Response(s2);
    s2.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n"
		"4\t5\t6\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsInterleaved2)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    std::string request1(getPostRequest("a=1&b=2&c=3\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), 3));
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    boost::asio::write(s1, boost::asio::buffer(request1.c_str() + 3, request1.size() - 3));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), 4));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str() + 4, request1.size() - 4));
    read200Response(s1);
    s1.close();
    read200Response(s2);
    s2.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n"
		"4\t5\t6\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsInterleaved3)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    std::string request1(getPostRequest("a=1&b=2&c=3\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), 3));
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), 4));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str() + 4, request1.size() - 4));
    read200Response(s2);
    s2.close();
    boost::asio::write(s1, boost::asio::buffer(request1.c_str() + 3, request1.size() - 3));
    read200Response(s1);
    s1.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("4\t5\t6\n"
		"1\t2\t3\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsInterleavedWithShutdown)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::system::error_code ec;
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    std::string request1(getPostRequest("a=1&b=2&c=3\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), 3));
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), 4));
    // Now wait for a bit to make sure that we've context switched
    // then kill.  We should not be able to make a new connection
    // but should be good to complete processing on existing ones.
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    p.kill();
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    tcp::socket s3(io_service);
    s3.connect(*iterator, ec);
    BOOST_CHECK(boost::asio::error::connection_refused == ec);
    boost::asio::write(s2, boost::asio::buffer(request2.c_str() + 4, request1.size() - 4));
    read200Response(s2);
    s2.close();
    boost::asio::write(s1, boost::asio::buffer(request1.c_str() + 3, request1.size() - 3));
    read200Response(s1);
    s1.close();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("4\t5\t6\n"
		"1\t2\t3\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsInterleavedWithErrorRequest)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::system::error_code ec;
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    std::string request1(getPostRequest("a=1&b=2&c=3\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\n"));
    std::string request3(getPostRequest("/bad/resource", "a=4&b=5&c=6\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), 3));
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), 4));
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    tcp::socket s3(io_service);
    s3.connect(*iterator, ec);
    boost::asio::write(s3, boost::asio::buffer(request3.c_str(), request1.size()));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str() + 4, request1.size() - 4));
    read404Response(s3);
    s3.close();
    read200Response(s2);
    s2.close();
    boost::asio::write(s1, boost::asio::buffer(request1.c_str() + 3, request1.size() - 3));
    read200Response(s1);
    s1.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("4\t5\t6\n"
		"1\t2\t3\n");
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsMultipleRecordsInterleaved1)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    std::string request1(getPostRequest("a=1&b=2&c=3\na=7&b=8&c=9\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\na=10&b=11&c=12\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), request1.size()-6));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), request2.size()-2));
    // This pause allows the first records of the two sockets to be processed before sending
    // through the rest of the data.
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str() + request1.size()-6, 6));
    read200Response(s1);
    s1.close();
    boost::asio::write(s2, boost::asio::buffer(request2.c_str() + request2.size()-2, 2));
    read200Response(s2);
    s2.close();
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n"
		"4\t5\t6\n"
		"7\t8\t9\n"
		"10\t11\t12\n"
		);
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

BOOST_AUTO_TEST_CASE(postTwoRequestsMultipleRecordsInterleavedWithClosedConnection)
{
  try {
    HttpOperatorProcess p(basicProgram);
    boost::asio::io_service io_service;
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(tcp::v4(), "localhost", "9876");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s1(io_service);
    s1.connect(*iterator);
    tcp::socket s2(io_service);
    s2.connect(*iterator);
    // These early closes happen after a full record is sent so we expect
    // to see the full record get accepted.
    std::string request1(getPostRequest("a=1&b=2&c=3\na=7&b=8&c=9\n"));
    std::string request2(getPostRequest("a=4&b=5&c=6\na=10&b=11&c=12\n"));
    boost::asio::write(s1, boost::asio::buffer(request1.c_str(), request1.size()-6));
    boost::asio::write(s2, boost::asio::buffer(request2.c_str(), request2.size()-2));
    s1.close();
    s2.close();
    // Wait before killing; there is a race condition for when the close
    // gets to the operator.
    boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    p.kill();
    int32_t ret = p.waitForCompletion();
    BOOST_CHECK_EQUAL(0, ret);
    checkOutput("1\t2\t3\n"
		"4\t5\t6\n"
		);
  } catch(std::exception& e) {
    std::cerr << "Unexpected exception: " << e.what() << std::endl;
    BOOST_CHECK(false);
  } 
}

