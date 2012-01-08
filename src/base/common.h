// TODO: header

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

// Helper function to convert an arbitrary object to a string via thei
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

}  // namespace firmament::common

}

#endif  // FIRMAMENT_BASE_COMMON_H
