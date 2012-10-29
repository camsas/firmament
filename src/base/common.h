// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common static data structures and methods.

#ifndef FIRMAMENT_BASE_COMMON_H
#define FIRMAMENT_BASE_COMMON_H

#include <stdint.h>

#include <sstream>  // NOLINT
#include <vector>
#include <set>
#include <string>
#include <limits>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <gtest/gtest_prod.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>

namespace firmament {

using namespace std;  // NOLINT

using google::protobuf::RepeatedPtrField;

#define SUBMSG_READ(obj, submsg, member) \
    obj.GetExtension(submsg ## _extn).member()
#define SUBMSG_WRITE(obj, submsg, member, val) \
    obj.MutableExtension(submsg ## _extn)->set_ ## member(val)

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

// Helper function to convert a repeated protobuf field to a STL set.
// This method copies the input collection, so it is O(N) in time and space.
template <typename T>
inline set<T> pb_to_set(const RepeatedPtrField<T>& pb_field) {
  set<T> return_set;
  // N.B.: using GNU-style RTTI (typeof)
  for (typeof(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_set.insert(*iter);
  return return_set;
}

// Helper function to convert a repeated protobuf field to a STL vector.
// This method copies the input collection, so it is O(N) in time and space.
template <typename T>
inline vector<T> pb_to_vector(const RepeatedPtrField<T>& pb_field) {
  vector<T> return_vec;
  // N.B.: using GNU-style RTTI (typeof)
  for (typeof(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_vec.push_back(*iter);
  return return_vec;
}

}  // namespace firmament::common

}  // namespace firmament

#endif  // FIRMAMENT_BASE_COMMON_H
