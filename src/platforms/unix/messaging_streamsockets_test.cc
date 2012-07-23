// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Worker class unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.pb.h"
#include "platforms/unix/messaging_streamsockets.h"

using firmament::platform_unix::streamsockets::StreamSocketsMessaging;
using firmament::platform_unix::streamsockets::StreamSocketsChannel;
using firmament::common::InitFirmament;
using firmament::TestMessage;

namespace {

// The fixture for testing the stream socket messaging adapter.
class StreamSocketsMessagingTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  StreamSocketsMessagingTest() {
    // You can do set-up work for each test here.
  }

  virtual ~StreamSocketsMessagingTest() {
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
  StreamSocketsMessaging adapter_;
};

// Tests channel establishment.
TEST_F(StreamSocketsMessagingTest, TCPChannelEstablishAndSendTestMessage) {
  FLAGS_v = 2;
  string uri = "tcp://localhost:9998";
  StreamSocketsMessaging mess_adapter;
  StreamSocketsChannel<TestMessage>
      channel(StreamSocketsChannel<TestMessage>::SS_TCP);
  VLOG(1) << "Calling Listen";
  mess_adapter.Listen(uri);
  // Need to block and wait for the socket to become ready, otherwise race
  // ensues.
  VLOG(1) << "Waiting for server to be ready...";
  while (!mess_adapter.ListenReady()) {
    VLOG(1) << "Waiting until ready to listen in server...";
  }
  VLOG(1) << "Calling EstablishChannel";
  mess_adapter.EstablishChannel(uri, &channel);
  // Need to block and wait until the connection is ready, too.
  while (!channel.Ready()) {
    VLOG(1) << "Waiting until channel established...";
  }
  // Send a test protobuf message through the channel
  VLOG(1) << "Calling SendS";
  mess_adapter.SendOnConnection(0);
  // Receive the protobuf at the other end of the channel
  TestMessage tm;
  VLOG(1) << "Calling RecvS";
  CHECK(channel.RecvS(&tm));
  // The received message should have the "test" field set to 43 (instead of the
  // default 42).
  CHECK_EQ(tm.test(), 43);
  VLOG(1) << tm.test();
  VLOG(1) << "closing channel";
  channel.Close();
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
