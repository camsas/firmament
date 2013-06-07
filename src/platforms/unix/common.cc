// The Firmament project
// Copyright (c) 2011-2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
