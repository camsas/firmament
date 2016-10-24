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

// String tools.

#ifndef FIRMAMENT_MISC_STRING_UTILS_H
#define FIRMAMENT_MISC_STRING_UTILS_H

#include <string>
#include <cstdarg>

using std::string;

namespace firmament {

// sprintf-like string formatter
void spf(string* s, const std::string fmt, ...);

// sprintf-like string formatter with append
void spfa(string* s, const std::string fmt, ...);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_STRING_UTILS_H
