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

// Platform-independent worker class definition. This is subclassed by the
// platform-specific worker classes.

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include <string>

// XXX(malte): Think about the Boost dependency!
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build worker!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "engine/node.h"
#include "messages/base_message.pb.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_channel.h"

namespace firmament {

class Worker : public Node {
 public:
  explicit Worker();
  void Run();
  bool RunCoordinatorDiscovery(const string &coordinator_uri);
  bool ConnectToCoordinator(const string& coordinator_uri);

 protected:
  StreamSocketsChannel<BaseMessage>* chan_;
  // TODO(malte): transform this into a better representation
  string coordinator_uri_;

  void HandleIncomingMessage(BaseMessage *bm,
                             const string& remote_endpoint);
  bool RegisterWithCoordinator();
  void SendHeartbeat();
  bool SendMessageToCoordinator(BaseMessage* msg);
};

}  // namespace firmament

#endif
