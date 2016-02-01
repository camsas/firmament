// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common header file for platform-specific implementations.

#ifndef FIRMAMENT_PLATFORMS_COMMON_H
#define FIRMAMENT_PLATFORMS_COMMON_H

#include <string>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#endif

#include "base/types.h"

namespace firmament {

inline ResourceID_t ResourceIDFromString(const string& str) {
#ifdef __PLATFORM_HAS_BOOST__
  boost::uuids::string_generator gen;
  return gen(str);
#else
#error unimplemented
#endif
}

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_COMMON_H
