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

#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>

#include "AsyncRecordParser.hh"

BOOST_AUTO_TEST_CASE(testConsumeTerminatedString)
{
  const char * testString = "abcdefghijklmnopqrstwxyz";
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + strlen(testString)));
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('n', (char) *blk.begin());
  }
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 5));
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    blk.rebind((uint8_t *) (testString + 5), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('n', (char) *blk.begin());
  }
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 5));
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    blk.rebind((uint8_t *) (testString + 5), 
	       (uint8_t *) (testString + 11));
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    blk.rebind((uint8_t *) (testString + 5), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('n', (char) *blk.begin());
  }
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 5));
    ConsumeTerminatedString importer('m');
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK(importer.import(blk, RecordBuffer()).isError());
  }
}

BOOST_AUTO_TEST_CASE(testImportDecimalInt32)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", Int32Type::Get(ctxt, false)));
  RecordType recTy(members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "1392342\t";
  recTy.getFieldAddress("a").setInt32(0, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + strlen(testString)));
    ImportDecimalInt32 importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK_EQUAL(1392342, recTy.getFieldAddress("a").getInt32(buf));
  }
  recTy.getFieldAddress("a").setInt32(0, buf);
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 3));
    ImportDecimalInt32 importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK_EQUAL(0, recTy.getFieldAddress("a").getInt32(buf));
    blk.rebind((uint8_t *) (testString + 3), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK_EQUAL(1392342, recTy.getFieldAddress("a").getInt32(buf));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportFixedLengthString)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", CharType::Get(ctxt, 14, true)));
  RecordType recTy(members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "13923429923434\t";
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  // Fast path test
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + strlen(testString)));
    ImportFixedLengthString importer(recTy.getFieldAddress("a"), 14);
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  // Slow path test : 2 bites
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 3));
    ImportFixedLengthString importer(recTy.getFieldAddress("a"), 14);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk.rebind((uint8_t *) (testString + 3), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFieldAddress("a").setNull(buf);
  memset(recTy.getFieldAddress("a").getCharPtr(buf), 'x', 14);
  // Slow path test : 3 bites
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 3));
    ImportFixedLengthString importer(recTy.getFieldAddress("a"), 14);
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk.rebind((uint8_t *) (testString + 3), 
	       (uint8_t *) (testString + 5));
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK(recTy.getFieldAddress("a").isNull(buf));
    blk.rebind((uint8_t *) (testString + 5), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK(!recTy.getFieldAddress("a").isNull(buf));
    BOOST_CHECK(boost::algorithm::equals("13923429923434", 
					 recTy.getFieldAddress("a").getCharPtr(buf)));
  }
  recTy.getFree().free(buf);
}

BOOST_AUTO_TEST_CASE(testImportDouble)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> members;
  members.push_back(RecordMember("a", DoubleType::Get(ctxt, false)));
  RecordType recTy(members);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "1392342.2384452\t";
  // Fast path
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + strlen(testString)));
    ImportDouble importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK_EQUAL(1392342.2384452, 
		      recTy.getFieldAddress("a").getDouble(buf));
  }
  // Slow path 2 bites
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 3));
    ImportDouble importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK_EQUAL(0.0, recTy.getFieldAddress("a").getDouble(buf));
    blk.rebind((uint8_t *) (testString + 3), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK_EQUAL(1392342.2384452, 
		      recTy.getFieldAddress("a").getDouble(buf));
  }
  // Slow path 3 bites
  {
    recTy.getFieldAddress("a").setDouble(0, buf);
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + 3));
    ImportDouble importer(recTy.getFieldAddress("a"), '\t');
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK_EQUAL(0.0, recTy.getFieldAddress("a").getDouble(buf));
    blk.rebind((uint8_t *) (testString + 3), 
	       (uint8_t *) (testString + 7));
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    BOOST_CHECK_EQUAL(0.0, recTy.getFieldAddress("a").getDouble(buf));
    blk.rebind((uint8_t *) (testString + 7), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('\t', (char) *blk.begin());
    BOOST_CHECK_EQUAL(1392342.2384452, 
		      recTy.getFieldAddress("a").getDouble(buf));
  }
  recTy.getFree().free(buf);
}

class ImporterIterator
{
private:
  enum State { START, AGAIN };
  State mState;
  ParserState mPS;
  GenericRecordImporter mImporter;
  GenericRecordImporter::iterator mIt;
public:
  ImporterIterator(std::vector<ImporterSpec*>::const_iterator begin,
			std::vector<ImporterSpec*>::const_iterator end)
  :
    mState(START),
    mImporter(begin, end)
  {
  }
  ParserState import(AsyncDataBlock & block, 
		     RecordBuffer buf);
};

ParserState ImporterIterator::import(AsyncDataBlock & block, 
				     RecordBuffer buf)
{
  switch(mState) {
    while(true) {
      for(mIt = mImporter.begin();
	    mIt != mImporter.end(); ++mIt) {
	do {
	  mPS = (*mIt)(block, buf);
	  if (mPS.isSuccess()) {
	    break;
	  }
	  mState = AGAIN;
	  return mPS;
        case AGAIN:;
	} while(true);
      }
      mState = START;
      return ParserState::success();
    case START:;
    }
  }
}

BOOST_AUTO_TEST_CASE(testCustomImporter)
{
  DynamicRecordContext ctxt;
  std::vector<RecordMember> baseMembers;
  baseMembers.push_back(RecordMember("file_id", Int32Type::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("id", Int64Type::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("akid", CharType::Get(ctxt, 22, false)));
  baseMembers.push_back(RecordMember("cre_date", CharType::Get(ctxt, 19, false)));
  baseMembers.push_back(RecordMember("cre_hour", Int32Type::Get(ctxt, false)));
  baseMembers.push_back(RecordMember("campaign_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("creative_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("lineitem_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("member_page_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("advert_type_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("imp_cost", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cost_model", VarcharType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cost", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpm_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpc_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("vc_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cc_rev_raw", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpm_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cpc_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("vc_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("cc_rev", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("view_ts", DatetimeType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("click_ts", DatetimeType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("conv_ts", DatetimeType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("tid", VarcharType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("sale", DecimalType::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("rev_calc", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("coop_id", Int32Type::Get(ctxt, true)));
  baseMembers.push_back(RecordMember("dummy", VarcharType::Get(ctxt, true)));
  RecordType baseTy(baseMembers);
  std::vector<RecordMember> members;
  members.push_back(RecordMember("akid", CharType::Get(ctxt, 22, false)));
  members.push_back(RecordMember("cre_date", CharType::Get(ctxt, 19, false)));
  members.push_back(RecordMember("coop_id", Int32Type::Get(ctxt, false)));

  RecordType recTy(members);
  std::vector<ImporterSpec*> specs;
  ImporterSpec::createDefaultImport(&recTy, &baseTy, '\t', '\n', specs);
  RecordBuffer buf = recTy.getMalloc().malloc();
  const char * testString = "3007174\t2518829283\tAAA-AP_7AU46SAAA0BMmLg\t2011-08-02 13:59:39\t13\t16191\t68404\t15624\t\\N\t1\t200.00\tCPM\t0.002000\t\\N\t\\N\t\\N\t\\N\t0.000000\t0.00\t0.00\t0.00\t2011-08-02 13:59:39\t\\N\t\\N\t\\N\t\\N\t1\t1159\t232\t17060\tRMKT\t\\N\t\\N\t\\N\n"
    "4568685\t3985370792\tAAA-AWOj8E-HHgAAYBFMbA\t2012-07-02 08:50:47\t8\t37074\t1209848\t16707\t\\N\t1\t3.44\tDYNA\t0.000034\t\\N\t\\N\t\\N\t\\N\t0.000000\t0.00\t0.00\t0.00\t2012-07-02 08:50:47\t\\N\t\\N\t\\N\t\\N\t1\t304\t266\t887371\tACQ\t\\N\t\\N\t3718402554839679057\n"
    "4590336\t4129420988\tAAA-AWOj8E-HHgAAYBFMbA\t2012-07-05 23:25:06\t23\t44652\t1119241\t16707\t\\N\t1\t29.40\tDYNA\t0.000294\t0.003000\t\\N\t\\N\t\\N\t0.003000\t0.00\t0.00\t0.00\t2012-07-05 23:25:06\t\\N\t\\N\t\\N\t\\N\t1\t1452\t266\t803732\tRMKT\t\\N\t\\N\t7228796745580141893\n"
    "3791703\t3296814760\tAAA-Ad9j_06HBQAAuFT0Dg\t2012-01-22 14:52:15\t14\t35938\t733766\t16707\t\\N\t1\t69.18\tDYNA\t0.000692\t0.002450\t\\N\t\\N\t\\N\t0.000000\t0.00\t0.00\t0.00\t2012-01-22 14:52:15\t\\N\t\\N\t\\N\t\\N\t1\t1385\t266\t507103\tATMG\t\\N\t\\N\t3206783830354137187\n";
  // Fast path
  {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + strlen(testString)));
    ImporterIterator importer(specs.begin(), specs.end());
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('4', (char) *blk.begin());
    BOOST_CHECK(boost::algorithm::equals("AAA-AP_7AU46SAAA0BMmLg", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK(boost::algorithm::equals("2011-08-02 13:59:39", 
					 recTy.getFieldAddress("cre_date").getCharPtr(buf)));
    BOOST_CHECK_EQUAL(1159, 
		      recTy.getFieldAddress("coop_id").getInt32(buf));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('4', (char) *blk.begin());
    BOOST_CHECK(boost::algorithm::equals("AAA-AWOj8E-HHgAAYBFMbA", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK(boost::algorithm::equals("2012-07-02 08:50:47", 
					 recTy.getFieldAddress("cre_date").getCharPtr(buf)));
    BOOST_CHECK_EQUAL(304, 
		      recTy.getFieldAddress("coop_id").getInt32(buf));
  }
  // Slow path : 2 bites
  for(int32_t i=0; i< 100; i++) {
    AsyncDataBlock blk((uint8_t *) testString,
		       (uint8_t *) (testString + i + 1));
    ImporterIterator importer(specs.begin(), specs.end());
    BOOST_CHECK(importer.import(blk, buf).isExhausted());
    BOOST_CHECK(blk.isEmpty());
    blk.rebind((uint8_t *) (testString + i + 1), 
	       (uint8_t *) (testString + strlen(testString)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('4', (char) *blk.begin());
    BOOST_CHECK(boost::algorithm::equals("AAA-AP_7AU46SAAA0BMmLg", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK(boost::algorithm::equals("2011-08-02 13:59:39", 
					 recTy.getFieldAddress("cre_date").getCharPtr(buf)));
    BOOST_CHECK(importer.import(blk, buf).isSuccess());
    BOOST_CHECK(!blk.isEmpty());
    BOOST_CHECK_EQUAL('4', (char) *blk.begin());
    BOOST_CHECK(boost::algorithm::equals("AAA-AWOj8E-HHgAAYBFMbA", 
					 recTy.getFieldAddress("akid").getCharPtr(buf)));
    BOOST_CHECK(boost::algorithm::equals("2012-07-02 08:50:47", 
					 recTy.getFieldAddress("cre_date").getCharPtr(buf)));
  }
}

