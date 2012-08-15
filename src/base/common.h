// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common static data structures and methods.

#ifndef FIRMAMENT_BASE_COMMON_H
#define FIRMAMENT_BASE_COMMON_H

#include <stdint.h>

#include <sstream>  // NOLINT
#include <vector>
#include <string>
#include <limits>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <gtest/gtest_prod.h>

using namespace std;  // NOLINT

namespace firmament {

#define SUBMSG_READ(obj, submsg, member) obj.GetExtension(submsg ## _extn).member()
#define SUBMSG_WRITE(obj, submsg, member, val) obj.MutableExtension(submsg ## _extn)->set_ ## member(val)

// Helper function to convert an arbitrary object to a string via the
// stringstream standard library class.
template <class T> inline string to_string(const T& t) {
  stringstream ss;
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

}  // namespace firmament

#endif  // FIRMAMENT_BASE_COMMON_H
