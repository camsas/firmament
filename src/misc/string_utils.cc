// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
    s->resize(size);
    va_start(marker, fmt);
    // N.B. the below relies on a slightly dodgy cast via c_str()!
    n = vsnprintf((char*)s->c_str(), size, fmt.c_str(), marker);  // NOLINT -- deconstify
    va_end(marker);
    if ((n > 0) && ((b = (n < size)) == true))
      s->resize(n);
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
    ss.resize(size);
    va_start(marker, fmt);
    // N.B. the below relies on a slightly dodgy cast via c_str()!
    n = vsnprintf((char*)ss.c_str(), size, fmt.c_str(), marker);  // NOLINT -- deconstify
    va_end(marker);
    if ((n > 0) && ((b = (n < size)) == true))
      ss.resize(n);
    else
      size *= 2;
  }
  *s += ss;
}

}  // namespace firmament
