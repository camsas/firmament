// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common header file for UNIX platform-specific implementations. Imports Boost
// constructs used throughout the UNIX platform.

#ifndef FIRMAMENT_PLATFORMS_UNIX_COMMON_H
#define FIRMAMENT_PLATFORMS_UNIX_COMMON_H

#include "platforms/common.pb.h"

#include <boost/shared_ptr.hpp>

namespace firmament {

using boost::shared_ptr;

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_COMMON_H
