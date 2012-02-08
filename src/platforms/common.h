// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common header file for platform-specific implementations.

#ifndef FIRMAMENT_PLATFORMS_COMMON_H
#define FIRMAMENT_PLATFORMS_COMMON_H

#include "platforms/common.pb.h"

namespace firmament {

// Converts a platform name string to a PlatformID.
inline PlatformID GetPlatformID(const string &platform_name) {
  PlatformID platform_id;
  CHECK(PlatformID_Parse(platform_name, &platform_id))
      << "Invalid or unknown platform specified: " << platform_name;
  return platform_id;
}

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_COMMON_H
