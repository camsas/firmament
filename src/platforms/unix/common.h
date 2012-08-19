// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common header file for UNIX platform-specific implementations. Imports Boost
// constructs used throughout the UNIX platform.

#ifndef FIRMAMENT_PLATFORMS_UNIX_COMMON_H
#define FIRMAMENT_PLATFORMS_UNIX_COMMON_H

#include "platforms/common.pb.h"

#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/system/error_code.hpp>

namespace firmament {
namespace platform_unix {

#define __PLATFORM_UNIX__

using boost::shared_ptr;
using boost::scoped_ptr;

typedef boost::function<void(const boost::system::error_code &,
                             size_t)> AsyncSendHandler;
typedef boost::function<void(const boost::system::error_code &,
                             size_t)> AsyncRecvHandler;


}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_COMMON_H
