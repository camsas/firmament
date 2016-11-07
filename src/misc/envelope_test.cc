/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
  TestMessage* tm = testMsg.mutable_test();
  tm->set_test(43);
  // We need to serialize the message first, since otherwise Parse() will fail.
  vector<char> buf(testMsg.ByteSize());
  CHECK(testMsg.SerializeToArray(&buf[0], testMsg.ByteSize()));
  // Now parse it back into an envelope.
  Envelope<BaseMessage> envelope;
  CHECK(envelope.Parse(&buf[0], testMsg.ByteSize()));
  CHECK_NE(envelope.data_, &testMsg);
  CHECK(envelope.is_owner_);
  CHECK_EQ(envelope.data_->test().test(),
           testMsg.test().test());
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
  CHECK_EQ(envelope.data_->test().test(),
           testMsg.test().test());
}

// Tests allocation of a wrapping envelope around a protobuf. Internal pointer
// of the envelope should point to said protobuf, not a copy thereof.
TEST_F(EnvelopeTest, StashProtobuf) {
  BaseMessage testMsg;
  TestMessage* tm = testMsg.mutable_test();
  tm->set_test(43);
  Envelope<BaseMessage> envelope(&testMsg);
  CHECK_EQ(envelope.data_, &testMsg);
  CHECK(!envelope.is_owner_);
  CHECK_EQ(envelope.data_->test().test(),
           testMsg.test().test());
}


}  // namespace misc
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
