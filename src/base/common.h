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

DECLARE_string(debug_output_dir);

namespace firmament {

using namespace std;  // NOLINT

using google::protobuf::RepeatedPtrField;
using google::protobuf::RepeatedField;

#define SUBMSG_READ(obj, submsg, member) \
    obj.submsg().member()
#define SUBMSG_WRITE(obj, submsg, member, val) \
    obj.mutable ## _ ## submsg()->set_ ## member(val)
#define SUBMSG_READ_PTR(obj, submsg, member) \
    obj->submsg().member()
#define SUBMSG_WRITE_PTR(obj, submsg, member, val) \
    obj->mutable ## _ ## submsg()->set_ ## member(val)

// C++11 mode switch macro
#if __cplusplus >= 201103L
#define __CPP11_MODE__
#else
#undef __CPP11_MODE__
#endif

// Helper function to convert an arbitrary object to a string via the
// stringstream standard library class.
template <class T> inline string to_string(const T& t) {
  stringstream ss;
  ss << t;
  return ss.str();
}

// Helper function to convert an arbitrary object to a hexadecimal
// string via the stringstream standard library class.
template <class T> inline string to_hex_string(const T& t) {
  stringstream ss;
  ss << hex << t;
  return ss.str();
}

namespace common {

// Helper function to perform common init tasks for user-facing Firmament
// binaries.
inline void InitFirmament(int argc, char *argv[]) {
  // Use gflags to parse command line flags
  // The final (boolean) argument determines whether gflags-parsed flags should
  // be removed from the array (if true), otherwise they will re-ordered such
  // that all gflags-parsed flags are at the beginning. Removing them has proven
  // to have problematic interactions with task_lib, since the modified pointer
  // is lost. For this reason, we re-arrange instead, and have our own logic to
  // drop flags.
  // TODO(malte): figure out if the above can be fixed; hard-coded flag removal
  // logic is annoying!
  google::ParseCommandLineFlags(&argc, &argv, false);

  // Set up glog for logging output
  google::InitGoogleLogging(argv[0]);
}

// Helper function to convert a repeated protobuf field to a STL set.
// Overload for primitive numeric types using a RepeatedField.
// This method copies the input collection, so it is O(N) in time and space.
template <typename T>
inline set<T> pb_to_set(const RepeatedField<T>& pb_field) {
  set<T> return_set;
  // N.B.: using GNU-style RTTI (typeof)
  for (__typeof__(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_set.insert(*iter);
  return return_set;
}

// Helper function to convert a repeated protobuf field to a STL set.
// Overload for strings and message types using a RepeatedPtrField.
// This method copies the input collection, so it is O(N) in time and space.
template <typename T>
inline set<T> pb_to_set(const RepeatedPtrField<T>& pb_field) {
  set<T> return_set;
  // N.B.: using GNU-style RTTI (typeof)
  for (__typeof__(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_set.insert(*iter);
  return return_set;
}

// Helper function to convert a repeated protobuf field to a STL vector.
// Overload for primitive numeric types using a RepeatedField.
// This method copies the input collection, so it is O(N) in time and space.
template <typename T>
inline vector<T> pb_to_vector(const RepeatedField<T>& pb_field) {
  vector<T> return_vec;
  // N.B.: using GNU-style RTTI (typeof)
  for (__typeof__(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_vec.push_back(*iter);
  return return_vec;
}

// Helper function to convert a repeated protobuf field to a STL vector.
// Overload for strings and message types using a RepeatedPtrField.
// This method copies the input collection, so it is O(N) in time and space.
template <typename T>
inline vector<T> pb_to_vector(const RepeatedPtrField<T>& pb_field) {
  vector<T> return_vec;
  // N.B.: using GNU-style RTTI (typeof)
  for (__typeof__(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_vec.push_back(*iter);
  return return_vec;
}

}  // namespace common
}  // namespace firmament

#endif  // FIRMAMENT_BASE_COMMON_H
