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

// Common methods for the UNIX platform.

#include "platforms/unix/common.h"

#include <string>

namespace firmament {
namespace platform_unix {

namespace streamsockets {

string EndpointToString(boost::asio::ip::tcp::endpoint endpoint) {
  string address;
  string port;
  string protocol;
  boost::system::error_code ec;
  protocol = "tcp:";
  address = endpoint.address().to_string();
  port = to_string<uint64_t>(endpoint.port());
  if (ec)
    return "";
  else
    return protocol + address + ":" + port;
}

string EndpointToString(
    boost::asio::local::stream_protocol::endpoint endpoint) {
  string address;
  string port;
  string protocol;
  // XXX(malte): This does not actually work (I think), because ASIO's UNIX
  // socket representation does not have an address() member. We need to
  // figure out how to deal with this when implementing something other than
  // TCP. path() in local::stream_protocol::endpoint seems like a good
  // choice.
  protocol = "unix:";
  address = endpoint.path();
  return protocol + address;
}

}  // namespace streamsockets

}  // namespace platform_unix
}  // namespace firmament
