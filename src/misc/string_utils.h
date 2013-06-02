// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
