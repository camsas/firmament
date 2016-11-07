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

// Common header file for UNIX platform-specific implementations. Imports Boost
// constructs used throughout the UNIX platform.

#ifndef FIRMAMENT_PLATFORMS_UNIX_COMMON_H
#define FIRMAMENT_PLATFORMS_UNIX_COMMON_H

#include <string>

#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <boost/system/error_code.hpp>

#include "base/types.h"
#include "misc/envelope.h"

namespace firmament {
namespace platform_unix {

#define __PLATFORM_UNIX__

namespace streamsockets {
class TCPConnection;  // Forward declaration

string EndpointToString(boost::asio::ip::tcp::endpoint endpoint);
string EndpointToString(boost::asio::local::stream_protocol::endpoint endpoint);
}  // namespace streamsockets

struct AcceptHandler {
  typedef boost::function<void(shared_ptr<streamsockets::TCPConnection>)> type;
};

template <typename T>
struct AsyncSendHandler {
  typedef boost::function<void(const boost::system::error_code &, size_t)> type;
};

template <typename T>
struct AsyncRecvHandler {
  typedef boost::function<void(const boost::system::error_code &, size_t,
                               misc::Envelope<T>*)> type;
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_COMMON_H
