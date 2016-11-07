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

#include <stdint.h>
#include <string>
#include <cstdarg>
#include <cstdio>

#include "misc/string_utils.h"

namespace firmament {

// sprintf-like string formatter
void spf(string* s, const std::string fmt, ...) {
  int64_t n, size = 100;
  bool b = false;
  va_list marker;

  while (!b) {
    s->resize(static_cast<size_t>(size));
    va_start(marker, fmt);
    // N.B. the below relies on a slightly dodgy cast via c_str()!
    n = vsnprintf((char*)s->c_str(), size, fmt.c_str(), marker);  // NOLINT -- deconstify
    va_end(marker);
    if ((n > 0) && ((b = (n < size)) == true))
      s->resize(static_cast<size_t>(n));
    else
      size *= 2;
  }
}

// sprintf-like string formatter with append
void spfa(string *s, const std::string fmt, ...) {
  string ss;
  int64_t n, size = 100;
  bool b = false;
  va_list marker;

  while (!b) {
    ss.resize(static_cast<size_t>(size));
    va_start(marker, fmt);
    // N.B. the below relies on a slightly dodgy cast via c_str()!
    n = vsnprintf((char*)ss.c_str(), size, fmt.c_str(), marker);  // NOLINT -- deconstify
    va_end(marker);
    if ((n > 0) && ((b = (n < size)) == true))
      ss.resize(static_cast<size_t>(n));
    else
      size *= 2;
  }
  *s += ss;
}

}  // namespace firmament
