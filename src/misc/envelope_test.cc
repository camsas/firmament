// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Tests for envelope container class.

#include <gtest/gtest.h>

#include <vector>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "messages/test_message.pb.h"
#include "misc/envelope.h"
#include "misc/protobuf_envelope.h"

namespace firmament {
namespace misc {

using ::google::protobuf::Message;

// The fixture for testing the Envelope container class.
class EnvelopeTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  EnvelopeTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
  }

  virtual ~EnvelopeTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests.
};

// Tests allocation of an empty envelope and puts an integer into it (using
// memcopy internally).
TEST_F(EnvelopeTest, EmptyParseInteger) {
  uint64_t testInteger = 5;
  Envelope<uint64_t> envelope;
  CHECK(envelope.Parse(&testInteger, sizeof(uint64_t)));
  CHECK_NE(envelope.data_, &testInteger);
  CHECK(envelope.is_owner_);
  CHECK_EQ(*(envelope.data_), testInteger);
  CHECK_EQ(*(envelope.data()), testInteger);
}

// Tests synchronous send of an integer.
TEST_F(EnvelopeTest, StashInteger) {
  uint64_t testInteger = 5;
  Envelope<uint64_t> envelope(&testInteger);
  CHECK_EQ(envelope.data_, &testInteger);
  CHECK(!envelope.is_owner_);
}

// Tests allocation of an empty envelope and puts a protobuf into it, copying it
// using the protobuf Parse method.
TEST_F(EnvelopeTest, EmptyParseProtobuf) {
  BaseMessage testMsg;
  TestMessage* tm = testMsg.MutableExtension(test_extn);
  tm->set_test(43);
  // We need to serialize the message first, since otherwise Parse() will fail.
  vector<char> buf(testMsg.ByteSize());
  CHECK(testMsg.SerializeToArray(&buf[0], testMsg.ByteSize()));
  // Now parse it back into an envelope.
  Envelope<BaseMessage> envelope;
  CHECK(envelope.Parse(&buf[0], testMsg.ByteSize()));
  CHECK_NE(envelope.data_, &testMsg);
  CHECK(envelope.is_owner_);
  CHECK_EQ(envelope.data_->GetExtension(test_extn).test(),
           testMsg.GetExtension(test_extn).test());
}

// Tests allocation of an empty envelope and puts a blank protobuf, i.e. one
// without any fields set, into it, copying it using the protobuf Parse
// method.
TEST_F(EnvelopeTest, EmptyParseBlankProtobuf) {
  BaseMessage testMsg;
  Envelope<BaseMessage> envelope;
  CHECK(envelope.Parse(&testMsg, testMsg.ByteSize()));
  CHECK_NE(envelope.data_, &testMsg);
  CHECK(envelope.is_owner_);
  // The value is not set, and hence takes the default. That should still be
  // identical, though!
  CHECK_EQ(envelope.data_->GetExtension(test_extn).test(),
           testMsg.GetExtension(test_extn).test());
}

// Tests allocation of a wrapping envelope around a protobuf. Internal pointer
// of the envelope should point to said protobuf, not a copy thereof.
TEST_F(EnvelopeTest, StashProtobuf) {
  BaseMessage testMsg;
  TestMessage* tm = testMsg.MutableExtension(test_extn);
  tm->set_test(43);
  Envelope<BaseMessage> envelope(&testMsg);
  CHECK_EQ(envelope.data_, &testMsg);
  CHECK(!envelope.is_owner_);
  CHECK_EQ(envelope.data_->GetExtension(test_extn).test(),
           testMsg.GetExtension(test_extn).test());
}


}  // namespace misc
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
