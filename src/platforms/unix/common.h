// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common header file for UNIX platform-specific implementations. Imports Boost
// constructs used throughout the UNIX platform.

#ifndef FIRMAMENT_PLATFORMS_UNIX_COMMON_H
#define FIRMAMENT_PLATFORMS_UNIX_COMMON_H

#include <boost/function.hpp>
#include <boost/system/error_code.hpp>

#include "base/types.h"
#include "misc/envelope.h"
#include "platforms/common.pb.h"

namespace firmament {
namespace platform_unix {

#define __PLATFORM_UNIX__

namespace streamsockets {
class TCPConnection;  // Forward declaration
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
