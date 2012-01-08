// TODO: header

#ifndef FIRMAMENT_PLATFORMS_COMMON_H
#define FIRMAMENT_PLATFORMS_COMMON_H

#include "platforms/common.pb.h"

namespace firmament {

inline PlatformID GetPlatformID(const string &platform_name) {
  VLOG(1) << platform_name;
  return UNIX; // TODO: fix
}

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_COMMON_H
