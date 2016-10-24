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
