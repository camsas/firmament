// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common static data structures and methods.

#ifndef FIRMAMENT_BASE_COMMON_H
#define FIRMAMENT_BASE_COMMON_H

#include <stdint.h>

#include <sstream>
#include <iostream>
#include <vector>
#include <string>
#include <limits>

#include <glog/logging.h>
#include <gflags/gflags.h>

using namespace std;

namespace firmament {

// TODO(malte): Deprecated in favour of protobuf.
typedef enum kPlatformID {
  UNIX = 0,
  SCC = 1,
} PlatformID;

// Helper function to convert an arbitrary object to a string via the
// stringstream standard library class.
template <class T> inline string to_string (const T& t) {
  std::stringstream ss;
  ss << t;
  return ss.str();
}

namespace common {

// Helper function to perform common init tasks for user-facing Firmament
// binaries.
inline void InitFirmament(int argc, char *argv[]) {
  // Use gflags to parse command line flags
  google::ParseCommandLineFlags(&argc, &argv, true);

  // Set up glog for logging output
  google::InitGoogleLogging(argv[0]);
}

// TODO(malte): Deprecated in favour of an implementation in platforms.
inline PlatformID GetPlatformID(const string &platform_name) {
  VLOG(1) << platform_name;
  return UNIX;
}

}  // namespace firmament::common

}

#endif  // FIRMAMENT_BASE_COMMON_H
