// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_PLATFORMS_SIM_SIMULATED_MESSAGING_ADAPTER_H
#define FIRMAMENT_PLATFORMS_SIM_SIMULATED_MESSAGING_ADAPTER_H

#include "misc/messaging_interface.h"

namespace firmament {
namespace platforms {
namespace sim {

template <class T>
class SimulatedMessagingAdapter : public MessagingAdapterInterface<T>,
  public boost::enable_shared_from_this<SimulatedMessagingAdapter<T> >,
  private boost::noncopyable {
 public:
  void AwaitNextMessage();
  void CloseChannel(MessagingChannelInterface<T>* channel);
  bool EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan);
  MessagingChannelInterface<T>* GetChannelForEndpoint(const string& endpoint);
  void ListenURI(const string& endpoint_uri);
  bool ListenReady();
  void RegisterAsyncMessageReceiptCallback(
      typename AsyncMessageRecvHandler<T>::type callback);
  void RegisterAsyncErrorPathCallback(
      typename AsyncErrorPathHandler<T>::type callback);
  bool SendMessageToEndpoint(const string& endpoint_uri, T& message);  // NOLINT
  ostream& ToString(ostream* stream) const;
};

}  // namespace sim
}  // namespace platforms
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_SIM_SIMULATED_MESSAGING_ADAPTER_H
