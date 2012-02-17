// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Worker class unit tests.

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.pb.h"
#include "platforms/unix/messaging_streamsockets.h"

using namespace firmament;

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

  // Objects declared here can be used by all tests in the test case for Worker.
  StreamSocketsMessaging adapter_;
};

// Tests channel establishment.
TEST_F(StreamSocketsMessagingTest, TCPChannelEstablish) {
  FLAGS_v = 2;
  uint32_t port = 9998;
  string uri = "tcp://localhost:9998";
  string host = "localhost";
  StreamSocketsMessaging mess_adapter;
  StreamSocketsChannel<TestMessage> channel(StreamSocketsChannel<TestMessage>::SS_TCP);
  VLOG(1) << "Calling Listen";
  mess_adapter.Listen(host, port);
  VLOG(1) << "Calling EstablishChannel";
  mess_adapter.EstablishChannel(uri, &channel);
  VLOG(1) << "Calling SendS";
  mess_adapter.SendOnConnection(0);
  VLOG(1) << "Calling RecvS";
  channel.RecvS();
  VLOG(1) << "closing channel";
  channel.Close();
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  common::InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
