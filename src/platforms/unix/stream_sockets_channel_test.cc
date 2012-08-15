// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stream-based channel tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "messages/test_message.pb.h"
#include "misc/envelope.h"
#include "misc/messaging_interface.h"
#include "platforms/common.pb.h"
#include "platforms/unix/common.h"
#include "platforms/unix/messaging_streamsockets-inl.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

using firmament::platform_unix::streamsockets::StreamSocketsMessaging;
using firmament::platform_unix::streamsockets::StreamSocketsChannel;
using firmament::common::InitFirmament;
using firmament::misc::Envelope;
using ::google::protobuf::Message;
using boost::shared_ptr;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// The fixture for testing the stream socket messaging adapter.
class StreamSocketsChannelTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  StreamSocketsChannelTest()
    : local_uri_("tcp://localhost:7777"),
      remote_uri_("tcp://localhost:7778"),
      local_adapter_(new StreamSocketsMessaging()),
      remote_adapter_(new StreamSocketsMessaging()) {
    // You can do set-up work for each test here.
  }

  virtual ~StreamSocketsChannelTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    FLAGS_v = 2;
    // Code here will be called immediately after the constructor (right
    // before each test).
    remote_adapter_->Listen(remote_uri_);
    // Need to block and wait for the socket to become ready, otherwise race
    // ensues.
    VLOG(1) << "Waiting for remote end to be ready...";
    while (!remote_adapter_->ListenReady()) {
      VLOG(1) << "Waiting until ready to listen on remote adapter...";
    }
    channel_.reset(new StreamSocketsChannel<BaseMessage>(
        StreamSocketsChannel<BaseMessage>::SS_TCP));
    VLOG(1) << "Establishing channel";
    local_adapter_->EstablishChannel(remote_uri_, channel_.get());
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
    channel_->Close();
    remote_adapter_->StopListen();
    local_adapter_->StopListen();
  }

  // Objects declared here can be used by all tests.
  shared_ptr<StreamSocketsMessaging> local_adapter_;
  shared_ptr<StreamSocketsMessaging> remote_adapter_;
  const string local_uri_;
  const string remote_uri_;
  shared_ptr<StreamSocketsChannel<BaseMessage> > channel_;
};

// Tests synchronous send of an integer.
/*TEST_F(StreamSocketsChannelTest, TCPSyncIntSend) {
  FLAGS_v = 2;
  uint64_t testInteger = 5;
  Envelope<uint64_t> envelope(&testInteger);
  channel_->SendS(envelope);
  shared_ptr<StreamSocketsChannel<uint64_t> > backchannel =
      remote_adapter_->GetChannelForConnection(0);
  uint64_t recvdInteger = 0;
  Envelope<uint64_t> recv_env(&recvdInteger);
  backchannel.RecvS(recv_env);
  CHECK_EQ(recvdInteger, testInteger);
  CHECK_EQ(recvdInteger, 5);
}*/

// Tests synchronous send of a protobuf.
TEST_F(StreamSocketsChannelTest, TCPSyncProtobufSendReceive) {
  FLAGS_v = 2;
  BaseMessage tm;
  tm.MutableExtension(test_extn)->set_test(5);
  Envelope<BaseMessage> envelope(&tm);
  while (!channel_->Ready()) {  }
  channel_->SendS(envelope);
  // Spin-wait for backchannel to become available
  while (remote_adapter_->active_channels().size() == 0) { }
  shared_ptr<StreamSocketsChannel<BaseMessage> > backchannel =
      remote_adapter_->GetChannelForConnection(0);
  BaseMessage r_tm;
  Envelope<BaseMessage> recv_env(&r_tm);
  while (!backchannel->Ready()) {  }
  backchannel->RecvS(&recv_env);
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
}

// Tests synchronous send of multiple subsequent protobufs.
TEST_F(StreamSocketsChannelTest, TCPSyncProtobufSendReceiveMulti) {
  FLAGS_v = 2;
  BaseMessage tm;
  SUBMSG_WRITE(tm, test, test, 5);
  Envelope<BaseMessage> envelope(&tm);
  while (!channel_->Ready()) {  }
  // send first time
  channel_->SendS(envelope);
  // send second time
  channel_->SendS(envelope);
  // Spin-wait for backchannel to become available
  while (remote_adapter_->active_channels().size() == 0) { }
  shared_ptr<StreamSocketsChannel<BaseMessage> > backchannel =
      remote_adapter_->GetChannelForConnection(0);
  BaseMessage r_tm;
  Envelope<BaseMessage> recv_env(&r_tm);
  while (!backchannel->Ready()) {  }
  // first receive
  backchannel->RecvS(&recv_env);
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
  // second receive
  backchannel->RecvS(&recv_env);
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
}


// Tests asynchronous send (and synchronous receive) of a protobuf.
/*TEST_F(StreamSocketsChannelTest, TCPAsyncProtobufSend) {
  FLAGS_v = 2;
  BaseMessage tm;
  SUBMSG_WRITE(tm, test, test, 5);
  Envelope<BaseMessage> envelope(&tm);
  channel_->SendA(envelope);
  shared_ptr<StreamSocketsChannel<BaseMessage> > backchannel =
      remote_adapter_->GetChannelForConnection(0);
  BaseMessage r_tm;
  Envelope<BaseMessage> recv_env(&r_tm);
  backchannel->RecvS(&recv_env);
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), SUBMSG_READ(tm, test, test));
  CHECK_EQ(SUBMSG_READ(r_tm, test, test), 5);
}*/

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
