// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Worker class unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "messages/test_message.pb.h"
// N.B. this needs to go above messaging_interface.h due to the #define for
// __PLATFORM_UNIX__
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.pb.h"
#include "platforms/unix/stream_sockets_adapter-inl.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

using firmament::platform_unix::streamsockets::StreamSocketsAdapter;
using firmament::platform_unix::streamsockets::StreamSocketsChannel;
using firmament::common::InitFirmament;
using firmament::misc::Envelope;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// The fixture for testing the stream socket messaging adapter.
class StreamSocketsAdapterTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  StreamSocketsAdapterTest() {
    // You can do set-up work for each test here.
  }

  virtual ~StreamSocketsAdapterTest() {
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

// Tests channel establishment.
TEST_F(StreamSocketsAdapterTest, TCPChannelEstablishAndSendTestMessage) {
  string uri = "tcp://localhost:7777";
  // We need to hold at least one shared pointer to the messaging adapter before
  // it can use shared_from_this().
  StreamSocketsAdapter<BaseMessage>* mess_adapter =
      new StreamSocketsAdapter<BaseMessage>();
  StreamSocketsChannel<BaseMessage>* channel =
      new StreamSocketsChannel<BaseMessage>(
          StreamSocketsChannel<BaseMessage>::SS_TCP);
  VLOG(1) << "Calling Listen";
  mess_adapter->ListenURI(uri);
  // Need to block and wait for the socket to become ready, otherwise race
  // ensues.
  VLOG(1) << "Waiting for server to be ready...";
  while (!mess_adapter->ListenReady()) {
    VLOG(1) << "Waiting until ready to listen in server...";
  }
  VLOG(1) << "Calling EstablishChannel";
  mess_adapter->EstablishChannel(uri, channel);
  // Need to block and wait until the connection is ready, too.
  while (!channel->Ready()) {
    VLOG(1) << "Waiting until channel established...";
  }
  // Send a test protobuf message through the channel
  BaseMessage s_tm;
  s_tm.MutableExtension(test_extn)->set_test(43);
  while (!mess_adapter->GetChannelForEndpoint(
      channel->LocalEndpointString())) {
    VLOG(3) << "Waiting for channel with endpoint "
            << channel->LocalEndpointString();
  }
  VLOG(1) << "Calling SendS";
  CHECK(mess_adapter->SendMessageToEndpoint(
      channel->LocalEndpointString(), s_tm));
  // Receive the protobuf at the other end of the channel
  BaseMessage r_tm;
  Envelope<BaseMessage> envelope(&r_tm);
  VLOG(1) << "Calling RecvS";
  CHECK(channel->RecvS(&envelope));
  // The received message should have the "test" field set to 43 (instead of the
  // default 42).
  CHECK_EQ(r_tm.GetExtension(test_extn).test(), 43);
  VLOG(1) << "closing channel";
  channel->Close();
  mess_adapter->StopListen();
}

// Tests send and receive of arbitrary protobufs.
/*TEST_F(StreamSocketsAdapterTest, ArbitraryProtobufSendRecv) {
  string uri = "tcp://localhost:7778";
  // We need to hold at least one shared pointer to the messaging adapter before
  // it can use shared_from_this().
  shared_ptr<StreamSocketsAdapter> mess_adapter(new StreamSocketsAdapter());
  StreamSocketsChannel<BaseMessage>
      channel(StreamSocketsChannel<BaseMessage>::SS_TCP);
  mess_adapter->ListenURI(uri);
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
TEST_F(StreamSocketsAdapterTest, BackchannelEstablishment) {
  string uri1 = "tcp://localhost:7779";
  // We need to hold at least one shared pointer to the messaging adapter before
  // it can use shared_from_this().
  StreamSocketsAdapter<BaseMessage>* mess_adapter1 =
      new StreamSocketsAdapter<BaseMessage>();
  StreamSocketsAdapter<BaseMessage>* mess_adapter2 =
      new StreamSocketsAdapter<BaseMessage>();
  StreamSocketsChannel<BaseMessage>*
      channel(new StreamSocketsChannel<BaseMessage>(
          StreamSocketsChannel<BaseMessage>::SS_TCP));
  mess_adapter1->ListenURI(uri1);
  // Need to block and wait for the socket to become ready, otherwise race
  // ensues.
  while (!mess_adapter1->ListenReady()) { }
  // Make a channel MA2 -> MA1
  mess_adapter2->EstablishChannel(uri1, channel);
  // Need to block and wait until the connection is ready, too.
  while (!channel->Ready()) {  }
  VLOG(1) << "channel is ready; getting backchannel...";
  // Send a message through the explicitly established channel.
  //mess_adapter1->SendOnConnection(0);
  // Check if we have a backchannel MA1 -> MA2
  while (mess_adapter1->NumActiveChannels() == 0) {}
  MessagingChannelInterface<BaseMessage>* backchannel =
      mess_adapter1->GetChannelForEndpoint(channel->LocalEndpointString());
  while (!backchannel->Ready()) {  }
  // Send a test message through the backchannel (MA2 -> MA1), and check if we
  // have received it.
  BaseMessage s_tm;
  Envelope<BaseMessage> s_envelope(&s_tm);
  s_tm.MutableExtension(test_extn)->set_test(44);
  BaseMessage r_tm;
  Envelope<BaseMessage> r_envelope(&r_tm);
  channel->SendS(s_envelope);
  CHECK(backchannel->RecvS(&r_envelope));
  CHECK_EQ(s_tm.GetExtension(test_extn).test(),
           r_tm.GetExtension(test_extn).test());
  CHECK_EQ(r_tm.GetExtension(test_extn).test(), 44);
  // Clean up the channels.
  backchannel->Close();
  channel->Close();
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
