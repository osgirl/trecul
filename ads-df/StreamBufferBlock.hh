#ifndef __STREAMBUFFERBLOCK_HH__
#define __STREAMBUFFERBLOCK_HH__

#include "RecordBuffer.hh"
#include "RecordType.hh"

#include <boost/serialization/serialization.hpp>

/**
 * Block buffer metadata abstraction on top of a Trecul
 * dynamic record.  
 * This provides a statically typed interface for accessing
 * instances of a particular dynamic record type.
 * There is a bit of silliness here in that we are
 * using dynamic records for something that isn't dynamic
 * at all.  I'm really just avoiding littering the code
 * with a bunch of casting (gack).  Better
 * would be to refactor things so that some operators in a flow
 * could use dynamic records and others could use honest/static C++ 
 * classes; there are some issues in the scheduler with this making 
 * this happen (ideally what flows along a channel should be invisible
 * to anything but the operators connected to it).
 */
class StreamBufferBlock
{
public:
  static const RecordType * getStreamBufferType(DynamicRecordContext& ctxt,
						int32_t bufferCapacity);
  static bool isStreamBufferType(const RecordType * ty);
private:
  // Data address of the buffer; a CHAR(N) field
  // for now
  FieldAddress mBufferAddress;
  // INTEGER offset to current end of buffer
  FieldAddress mBufferSize;
  // Amount of data we can store in the block (this
  // is the N for which the buffer field is a CHAR(N) type).
  int32_t mBufferCapacity;

  // Serialization
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & BOOST_SERIALIZATION_NVP(mBufferAddress);
    ar & BOOST_SERIALIZATION_NVP(mBufferSize);    
    ar & BOOST_SERIALIZATION_NVP(mBufferCapacity);
  }
public:
  StreamBufferBlock()
    :
    mBufferCapacity(0)
  {
  }
  StreamBufferBlock(const RecordType * streamBlockType);
  uint8_t * begin(RecordBuffer buf) const
  {
    return (uint8_t *) mBufferAddress.getCharPtr(buf);
  }
  uint8_t * end(RecordBuffer buf) const
  {
    return (uint8_t *) mBufferAddress.getCharPtr(buf) +
      mBufferSize.getInt32(buf);
  }
  int32_t getSize(RecordBuffer buf) const
  {
    return mBufferSize.getInt32(buf);
  }
  void setSize(int32_t sz, RecordBuffer buf) const
  {
    mBufferSize.setInt32(sz, buf);
  }
  int32_t capacity() const
  {
    return mBufferCapacity; 
  }
};

#endif
