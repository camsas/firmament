// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stream-based channel tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "messages/test_message.pb.h"
#include "misc/envelope.h"
#include "misc/map-util.h"
// N.B. this needs to go above messaging_interface.h due to the #define for
// __PLATFORM_UNIX__
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_channel.h"

using firmament::platform_unix::streamsockets::StreamSocketsAdapter;
using firmament::platform_unix::streamsockets::StreamSocketsChannel;
using firmament::common::InitFirmament;
using firmament::misc::Envelope;
using ::google::protobuf::Message;

DEFINE_uint64(heartbeat_interval, 1000000,
              "Heartbeat interval in microseconds.");

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// The fixture for testing the stream socket messaging adapter.
class StreamSocketsChannelTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  StreamSocketsChannelTest()
    : local_adapter_(new StreamSocketsAdapter<BaseMessage>()),
      remote_adapter_(new StreamSocketsAdapter<BaseMessage>()),
      local_uri_("tcp:localhost:7777"),
      remote_uri_("tcp:localhost:7778") {
    // You can do set-up work for each test here.
  }

  virtual ~StreamSocketsChannelTest() {
    // You can do clean-up work that doesn't throw exceptions here.
    VLOG(2) << "Now in destructor.";
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:
  virtual void SetUp() {
    // Wait briefly in order for sockets to close down
    boost::this_thread::sleep(boost::posix_time::seconds(1));
    // Code here will be called immediately after the constructor (right
    // before each test).
    remote_adapter_->ListenURI(remote_uri_);
    // Need to block and wait for the socket to become ready, otherwise race
    // ensues.
    VLOG(1) << "Waiting for remote end to be ready...";
    while (!remote_adapter_->ListenReady()) {
      VLOG(1) << "Waiting until ready to listen on remote adapter...";
    }
    channel_ = new StreamSocketsChannel<BaseMessage>(
        StreamSocketsChannel<BaseMessage>::SS_TCP);
    VLOG(1) << "Establishing channel";
    local_adapter_->EstablishChannel(remote_uri_, channel_);
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
    channel_->Close();
    remote_adapter_->StopListen();
    local_adapter_->StopListen();
  }

  // Objects declared here can be used by all tests.
  StreamSocketsAdapter<BaseMessage>* local_adapter_;
  StreamSocketsAdapter<BaseMessage>* remote_adapter_;
  const string local_uri_;
  const string remote_uri_;
  StreamSocketsChannel<BaseMessage>* channel_;
};

// Tests synchronous send of an integer.
TEST_F(StreamSocketsChannelTest, TCPSyncIntSend) {
  // TODO(malte): tidy this test up and add some commentary as to how it is
  // different from others.
  StreamSocketsAdapter<uint64_t>* local_uint_adapter =
      new StreamSocketsAdapter<uint64_t>();
  StreamSocketsAdapter<uint64_t>* remote_uint_adapter =
      new StreamSocketsAdapter<uint64_t>();
  remote_uint_adapter->ListenURI("tcp:localhost:7788");
  while (!remote_uint_adapter->ListenReady()) {
    VLOG(3) << "Waiting for adapter to be ready...";
  }
  StreamSocketsChannel<uint64_t>* uint_channel =
      new StreamSocketsChannel<uint64_t>(
          StreamSocketsChannel<uint64_t>::SS_TCP);
  local_uint_adapter->EstablishChannel("tcp:localhost:7788",
                                       uint_channel);
  while (!uint_channel->Ready()) {
    VLOG(3) << "Waiting for adapter to be ready...";
  }
  uint64_t testInteger = 5;
  Envelope<uint64_t> envelope(&testInteger);
  uint_channel->SendS(envelope);
  while (remote_uint_adapter->NumActiveChannels() == 0) {
    VLOG(3) << "Waiting for back-channel to appear...";
  }
  MessagingChannelInterface<uint64_t>* backchannel =
      remote_uint_adapter->GetChannelForEndpoint(
          uint_channel->LocalEndpointString());
  uint64_t recvdInteger = 0;
  Envelope<uint64_t> recv_env(&recvdInteger);
  while (!backchannel->Ready()) {
    VLOG(3) << "Waiting for back-channel to be ready...";
  }
  backchannel->RecvS(&recv_env);
  CHECK_EQ(recvdInteger, testInteger);
  CHECK_EQ(recvdInteger, 5);
  uint_channel->Close();
}

// Tests synchronous send of a protobuf.
TEST_F(StreamSocketsChannelTest, TCPSyncProtobufSendReceive) {
  BaseMessage tm;
  SUBMSG_WRITE(tm, test, test, 5);
  Envelope<BaseMessage> envelope(&tm);
  while (!channel_->Ready()) {
    VLOG(3) << "Waiting for adapter to be ready...";
  }
  channel_->SendS(envelope);
  // Spin-wait for backchannel to become available
  while (remote_adapter_->NumActiveChannels() == 0) {
    VLOG(3) << "Waiting for back-channel to appear...";
  }
  MessagingChannelInterface<BaseMessage>* backchannel =
      remote_adapter_->GetChannelForEndpoint(channel_->LocalEndpointString());
  BaseMessage r_tm;
  Envelope<BaseMessage> recv_env(&r_tm);
  while (!backchannel->Ready()) {
    VLOG(3) << "Waiting for back-channel to be ready...";
  }
  backchannel->RecvS(&recv_env);
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
}

// Tests synchronous send of multiple subsequent protobufs.
TEST_F(StreamSocketsChannelTest, TCPSyncProtobufSendReceiveMulti) {
  BaseMessage tm1;
  SUBMSG_WRITE(tm1, test, test, 5);
  Envelope<BaseMessage> envelope1(&tm1);
  // Spin-wait for channel to become available
  while (!channel_->Ready()) {
    VLOG(3) << "Waiting for adapter to be ready...";
  }
  // Spin-wait for backchannel to become available
  while (remote_adapter_->NumActiveChannels() == 0) {
    VLOG(3) << "Waiting for back-channel to appear...";
  }
  MessagingChannelInterface<BaseMessage>* backchannel =
      remote_adapter_->GetChannelForEndpoint(channel_->LocalEndpointString());
  CHECK(backchannel);
  while (!backchannel->Ready()) {
    VLOG(3) << "Waiting for back-channel to be ready...";
  }
  BaseMessage tm2;
  SUBMSG_WRITE(tm2, test, test, 7);
  Envelope<BaseMessage> envelope2(&tm2);
  // send first time
  channel_->SendS(envelope1);
  BaseMessage r_tm;
  Envelope<BaseMessage> recv_env(&r_tm);
  // first receive
  CHECK(backchannel->RecvS(&recv_env));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm1, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
  // send second time
  channel_->SendS(envelope2);
  // second receive
  CHECK(backchannel->RecvS(&recv_env));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm2, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 7);
}

// Tests asynchronous send (and synchronous receive) of a protobuf.
TEST_F(StreamSocketsChannelTest, TCPAsyncProtobufSend) {
  BaseMessage tm;
  SUBMSG_WRITE(tm, test, test, 5);
  Envelope<BaseMessage> envelope(&tm);
  channel_->SendS(envelope);
  //channel_->SendA(envelope, boost::bind(std::plus<int>(), 0, 0));
  while (remote_adapter_->NumActiveChannels() == 0) {
    VLOG(3) << "Waiting for back-channel to appear...";
  }
  MessagingChannelInterface<BaseMessage>* backchannel =
      remote_adapter_->GetChannelForEndpoint(channel_->LocalEndpointString());
  BaseMessage r_tm;
  Envelope<BaseMessage> recv_env(&r_tm);
  while (!backchannel->Ready()) {
    VLOG(3) << "Waiting for back-channel to be ready...";
  }
  backchannel->RecvS(&recv_env);
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
