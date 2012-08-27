// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common type definitions.

#ifndef FIRMAMENT_BASE_TYPES_H
#define FIRMAMENT_BASE_TYPES_H

#include <stdint.h>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid.hpp>
#endif


// Smart pointers
// Depending on compiler support and library available, we use the following, in
// this order:
// 1) C++11 smart pointers
// 2) Boost smart pointers
// 3) Yasper library pointers wrapped by homebrew classes
#ifdef __CXX_11_ENABLED__
// Have C++11 support from compiler
#include <memory>

namespace firmament {

using std::unique_ptr;
using std::shared_ptr;
using std::weak_ptr;

}  // namespace firmament

#elif __PLATFORM_HAS_BOOST__
// No C++11 support, but Boost is present
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

namespace firmament {

using boost::shared_ptr;
using boost::scoped_ptr;
using boost::weak_ptr;

}  // namespace firmament

#else
// Neither C++11 support nor Boost available; fall back to Yasper wrapped with
// home-brew classes.
#include "ext/yasper/yasper.h"

namespace firmament {

// reference-counted shared pointer
class shared_ptr<T> : public yasper::ptr<T> {
};

// TODO(malte): Add support for scoped and weak pointers
#error "No current support for scoped_ptr and weak_ptr using yasper " \
    "Smart pointers not available on this platform; this needs fixing before " \
    "we can compile. Alternatively, install Boost if available."
}
#endif

namespace firmament {

// Various utility typedefs
#ifdef __PLATFORM_HAS_BOOST__
typedef boost::uuids::uuid ResourceID_t;
#else
typedef uint64_t ResourceID_t;
#endif

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TYPES_H
