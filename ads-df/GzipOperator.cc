#include "zlib.h"
#include "GzipOperator.hh"

LogicalGunzip::LogicalGunzip()
  :
  LogicalOperator(1,1,1,1),
  mBufferCapacity(64*1024)
{
}

LogicalGunzip::~LogicalGunzip()
{
}

void LogicalGunzip::check(PlanCheckContext& ctxt)
{
  if (!StreamBufferBlock::isStreamBufferType(getInput(0)->getRecordType())) {
    ctxt.logError(*this, "Invalid type on input; must be stream type");
  }
  mStreamBlock = StreamBufferBlock::getStreamBufferType(ctxt, mBufferCapacity);
  getOutput(0)->setRecordType(mStreamBlock);
}

void LogicalGunzip::create(class RuntimePlanBuilder& plan)
{
  RuntimeOperatorType * opType = 
    new RuntimeGunzipOperatorType(mStreamBlock);
  plan.addOperatorType(opType);
  plan.mapInputPort(this, 0, opType, 0);  
  plan.mapOutputPort(this, 0, opType, 0);  
}

class RuntimeGunzipOperator : public RuntimeOperatorBase<RuntimeGunzipOperatorType>
{
private:
  typedef RuntimeGunzipOperatorType operator_type;

  enum State { START, GET_BUFFER, READ, WRITE, WRITE_LAST, WRITE_EOF };
  State mState;
  RecordBuffer mInput;
  RecordBuffer mOutput;
  z_stream mStream;

  bool usesExternalBuffers() const
  {
    return getNumInputs()==2;
  }

public:
  RuntimeGunzipOperator(RuntimeOperator::Services& services, 
		 const RuntimeGunzipOperatorType& opType)
    :
    RuntimeOperatorBase<RuntimeGunzipOperatorType>(services, opType)
  {
  }

  ~RuntimeGunzipOperator()
  {
  }

  /**
   * intialize.
   */
  void start()
  {
    mStream.zalloc = Z_NULL;
    mStream.zfree = Z_NULL;
    mStream.opaque = Z_NULL;
    ::inflateInit2(&mStream, 31);
    mStream.avail_in = 0;
    mStream.next_in = NULL;
    mState = START;
    onEvent(NULL);
  }

  void onEvent(RuntimePort * port)
  {
    switch(mState) {
    case START:
      while(true) {
	// Fill up a buffer with uncompressed data
	BOOST_ASSERT(mOutput == RecordBuffer());
	// if (usesExternalBuffers()) {
	//   // In this mode we get a buffer from an input;
	//   // often fed by the consumer of this operator's output
	//   requestRead(1);
	//   mState = GET_BUFFER;
	//   return;
	// case GET_BUFFER:
	//   read(port, mOutput);
	// } else {
	  // In this mode, we create the buffer
	  mOutput = getMyOperatorType().mMalloc.malloc();
	  BOOST_ASSERT(0 == getMyOperatorType().mStreamBlock.getSize(mOutput));
	// }
	mStream.avail_out = getMyOperatorType().mStreamBlock.capacity() -
	  getMyOperatorType().mStreamBlock.getSize(mOutput);
	mStream.next_out = (Bytef *)getMyOperatorType().mStreamBlock.end(mOutput);
	do {
	  if (mStream.avail_in == 0) {
	    if (mInput != RecordBuffer()) {
	      getMyOperatorType().mFree.free(mInput);
	      mInput = RecordBuffer();
	    }
	    requestRead(0);
	    mState = READ;
	    return;
	  case READ:
	    BOOST_ASSERT(mInput == RecordBuffer());
	    read(port, mInput);
	    if (mInput == RecordBuffer()) {
	      // Send out any decompressed data
	      if (mStream.avail_out != 
		  getMyOperatorType().mStreamBlock.capacity()) {
		getMyOperatorType().mStreamBlock.setSize(mStream.next_out ? 
							 mStream.next_out - (Bytef *)getMyOperatorType().mStreamBlock.begin(mOutput) : 
							 getMyOperatorType().mStreamBlock.capacity(),
							 mOutput);
		mState = WRITE_LAST;
		requestWrite(0);
		return;
	      case WRITE_LAST:
		// Flush always; these are big chunks of memory
		write(port, mOutput, true);
		mOutput = RecordBuffer();
	      } else {
		getMyOperatorType().mFree.free(mOutput);
	      }
	      // Close up shop
	      mState = WRITE_EOF;
	      requestWrite(0);
	      return;
	    case WRITE_EOF:
	      // Flush always; these are big chunks of memory
	      write(port, RecordBuffer(), true);
	      return;
	    }
	    mStream.avail_in = (uInt) getMyOperatorType().mStreamBlock.getSize(mInput);
	    mStream.next_in = (Bytef *)getMyOperatorType().mStreamBlock.begin(mInput);
	    if (mStream.avail_in == 0) {
	      // We actually got a zero read without being at EOF
	      throw std::runtime_error("Error reading compressed file");
	    }
	  }
	  {
	  int ret = ::inflate(&mStream, 1);
	  if (mStream.avail_in == 0) {
	    // We've exhausted input.  Try to get more next time
	    // around.
	    // TODO: Send buffer back to producer if we must
	    getMyOperatorType().mFree.free(mInput);
	    mInput = RecordBuffer();
	  }
	  if (mStream.avail_out == 0) {
	    break;
	  } else {
	    // If there wasn't a problem, then either mStream.avail_in == 0 ||
	    // mStream.avail_out == 0.
	    if (mStream.avail_in != 0) {
	      throw std::runtime_error((boost::format("Error decompressing file: inflate returned %1%") %
					ret).str());
	    }
	    // Exhausted input but still have some space 
	    // output; not a problem just keep going.
	  }
	  }

	} while(true);
	// Done trying to read.
	getMyOperatorType().mStreamBlock.setSize(mStream.next_out ? 
						 mStream.next_out - (Bytef *)getMyOperatorType().mStreamBlock.begin(mOutput) : 
						 getMyOperatorType().mStreamBlock.capacity(),
						 mOutput);
	// Flush always and write through to
	// avoid local queue; these are big chunks of memory
	mState = WRITE;
	requestWriteThrough(0);
	return;
      case WRITE:
	write(port, mOutput, true);
	mOutput = RecordBuffer();
      }
    }
  }

  void shutdown()
  {
    ::inflateEnd(&mStream);
  }
};

RuntimeGunzipOperatorType::RuntimeGunzipOperatorType(const RecordType * bufferTy)
  :
  mMalloc(bufferTy->getMalloc()),
  mFree(bufferTy->getFree()),
  mStreamBlock(bufferTy)
{
}

RuntimeGunzipOperatorType::~RuntimeGunzipOperatorType()
{
}

RuntimeOperator * RuntimeGunzipOperatorType::create(RuntimeOperator::Services & s) const
{
  return new RuntimeGunzipOperator(s, *this);
}
