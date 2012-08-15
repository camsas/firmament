// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Worker class unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "messages/test_message.pb.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.pb.h"
#include "platforms/unix/common.h"
#include "platforms/unix/messaging_streamsockets-inl.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

using firmament::platform_unix::streamsockets::StreamSocketsMessaging;
using firmament::platform_unix::streamsockets::StreamSocketsChannel;
using firmament::common::InitFirmament;
using firmament::misc::Envelope;
using boost::shared_ptr;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

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
/*TEST_F(StreamSocketsMessagingTest, TCPChannelEstablishAndSendTestMessage) {
  FLAGS_v = 2;
  string uri = "tcp://localhost:7777";
  // We need to hold at least one shared pointer to the messaging adapter before
  // it can use shared_from_this().
  shared_ptr<StreamSocketsMessaging> mess_adapter(new StreamSocketsMessaging());
  StreamSocketsChannel<BaseMessage>
      channel(StreamSocketsChannel<BaseMessage>::SS_TCP);
  VLOG(1) << "Calling Listen";
  mess_adapter->Listen(uri);
  // Need to block and wait for the socket to become ready, otherwise race
  // ensues.
  VLOG(1) << "Waiting for server to be ready...";
  while (!mess_adapter->ListenReady()) {
    VLOG(1) << "Waiting until ready to listen in server...";
  }
  VLOG(1) << "Calling EstablishChannel";
  mess_adapter->EstablishChannel(uri, &channel);
  // Need to block and wait until the connection is ready, too.
  while (!channel.Ready()) {
    VLOG(1) << "Waiting until channel established...";
  }
  // Send a test protobuf message through the channel
  VLOG(1) << "Calling SendS";
  mess_adapter->SendOnConnection(0);
  // Receive the protobuf at the other end of the channel
  BaseMessage tm;
  Envelope<BaseMessage> envelope(&tm);
  VLOG(1) << "Calling RecvS";
  CHECK(channel.RecvS(&envelope));
  // The received message should have the "test" field set to 43 (instead of the
  // default 42).
  CHECK_EQ(tm.GetExtension(test_extn).test(), 43);
  VLOG(1) << "closing channel";
  channel.Close();
  mess_adapter->StopListen();
}*/

// Tests send and receive of arbitrary protobufs.
/*TEST_F(StreamSocketsMessagingTest, ArbitraryProtobufSendRecv) {
  string uri = "tcp://localhost:7778";
  // We need to hold at least one shared pointer to the messaging adapter before
  // it can use shared_from_this().
  shared_ptr<StreamSocketsMessaging> mess_adapter(new StreamSocketsMessaging());
  StreamSocketsChannel<BaseMessage>
      channel(StreamSocketsChannel<BaseMessage>::SS_TCP);
  mess_adapter->Listen(uri);
  // Need to block and wait for the socket to become ready, otherwise race
  // ensues.
  while (!mess_adapter->ListenReady()) { }
  mess_adapter->EstablishChannel(uri, &channel);
  // Need to block and wait until the connection is ready, too.
  while (!channel.Ready()) {  }
  // Send a test protobuf message through the channel
  BaseMessage tm1;
  Envelope<BaseMessage> envelope1(&tm1);
  tm1.MutableExtension(test_extn)->set_test(43);
  mess_adapter->SendOnConnection(0);
  // Receive the protobuf at the other end of the channel
  BaseMessage tm2;
  Envelope<BaseMessage> envelope2(&tm2);
  CHECK(channel.RecvS(&envelope2));
  // The received message should have the "test" field set to 43 (instead of the
  // default 42).
  CHECK_EQ(tm2.GetExtension(test_extn).test(), tm1.GetExtension(test_extn).test());
  channel.Close();
}*/


// Tests backchannel establishment by sending a protobuf through the
// backchannel.
TEST_F(StreamSocketsMessagingTest, BackchannelEstablishment) {
  FLAGS_v = 2;
  string uri1 = "tcp://localhost:7779";
  // We need to hold at least one shared pointer to the messaging adapter before
  // it can use shared_from_this().
  shared_ptr<StreamSocketsMessaging> mess_adapter1(
      new StreamSocketsMessaging());
  shared_ptr<StreamSocketsMessaging> mess_adapter2(
      new StreamSocketsMessaging());
  StreamSocketsChannel<BaseMessage>
      channel(StreamSocketsChannel<BaseMessage>::SS_TCP);
  mess_adapter1->Listen(uri1);
  // Need to block and wait for the socket to become ready, otherwise race
  // ensues.
  while (!mess_adapter1->ListenReady()) { }
  // Make a channel MA2 -> MA1
  mess_adapter2->EstablishChannel(uri1, &channel);
  // Need to block and wait until the connection is ready, too.
  while (!channel.Ready()) {  }
  VLOG(1) << "channel is ready; getting backchannel...";
  // Send a message through the explicitly established channel.
  //mess_adapter1->SendOnConnection(0);
  // Check if we have a backchannel MA1 -> MA2
  while (mess_adapter1->active_channels().size() == 0) {}
  shared_ptr<StreamSocketsChannel<BaseMessage> > backchannel =
      mess_adapter1->GetChannelForConnection(0);
  while (!backchannel->Ready()) {  }
  // Send a test message through the backchannel (MA2 -> MA1), and check if we
  // have received it.
  BaseMessage s_tm;
  Envelope<BaseMessage> s_envelope(&s_tm);
  s_tm.MutableExtension(test_extn)->set_test(44);
  BaseMessage r_tm;
  Envelope<BaseMessage> r_envelope(&r_tm);
  channel.SendS(s_envelope);
  CHECK(backchannel->RecvS(&r_envelope));
  CHECK_EQ(s_tm.GetExtension(test_extn).test(), r_tm.GetExtension(test_extn).test());
  CHECK_EQ(r_tm.GetExtension(test_extn).test(), 44);
  // Clean up the channels.
  backchannel->Close();
  channel.Close();
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
