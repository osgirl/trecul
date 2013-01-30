#include <vector>
#include "StreamBufferBlock.hh"

const RecordType * 
StreamBufferBlock::getStreamBufferType(DynamicRecordContext& ctxt,
				       int32_t bufferCapacity)
{
  std::vector<RecordMember> members;
  members.push_back(RecordMember("size", Int32Type::Get(ctxt)));
  members.push_back(RecordMember("buffer", CharType::Get(ctxt, bufferCapacity)));  
  return RecordType::get(ctxt, members);
}

bool StreamBufferBlock::isStreamBufferType(const RecordType * ty)
{
  // TODO: Make sure we get a "stream block" record.
  return true;
}

StreamBufferBlock::StreamBufferBlock(const RecordType * streamBlockType)
  :
  mBufferAddress(streamBlockType->getFieldAddress("buffer")),
  mBufferSize(streamBlockType->getFieldAddress("size")),
  mBufferCapacity(streamBlockType->getMember("buffer").GetType()->GetSize())
{
}

