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

using namespace std;

namespace firmament {

// Helper function to convert an arbitrary object to a string via the stringstream
// standard library class.
template <class T> inline std::string to_string (const T& t) {
  std::stringstream ss;
  ss << t;
  return ss.str();
}

}

#endif  // FIRMAMENT_BASE_COMMON_H
