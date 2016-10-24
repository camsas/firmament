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

#ifndef FIRMAMENT_PLATFORMS_SIM_SIMULATED_MESSAGING_ADAPTER_H
#define FIRMAMENT_PLATFORMS_SIM_SIMULATED_MESSAGING_ADAPTER_H

#include "misc/messaging_interface.h"

#include <string>

#include "messages/base_message.pb.h"

namespace firmament {
namespace platform {
namespace sim {

template <class T>
class SimulatedMessagingAdapter : public MessagingAdapterInterface<T>,
  public boost::enable_shared_from_this<SimulatedMessagingAdapter<T> >,
  private boost::noncopyable {
 public:
  void AwaitNextMessage() {
    // No-op.
  }

  void CloseChannel(MessagingChannelInterface<T>* channel) {
    // No-op.
    // We don't have to close the channel because we haven't established it.
  }

  bool EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan) {
    return true;
  }

  MessagingChannelInterface<T>* GetChannelForEndpoint(const string& endpoint) {
    // No-op.
    return NULL;
  }

  void ListenURI(const string& endpoint_uri) {
    // No-op.
  }

  bool ListenReady() {
    return true;
  }

  void RegisterAsyncMessageReceiptCallback(
      typename AsyncMessageRecvHandler<T>::type callback) {
    // No-op.
    // Callback not required.
  }

  void RegisterAsyncErrorPathCallback(
      typename AsyncErrorPathHandler<T>::type callback) {
    // No-op.
    // We don't simulate any errors.
  }

  bool SendMessageToEndpoint(const string& endpoint_uri, T& message) { // NOLINT
    // No-op.
    return true;
  }

  ostream& ToString(ostream* stream) const {
    // No-op.
    return *stream << "(MessagingAdapter,type=Simulated,at=" << this << ")";
  }
};

}  // namespace sim
}  // namespace platform
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_SIM_SIMULATED_MESSAGING_ADAPTER_H
