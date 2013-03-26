// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common type definitions.

#ifndef FIRMAMENT_BASE_TYPES_H
#define FIRMAMENT_BASE_TYPES_H

#include <stdint.h>
#include <map>
#include <utility>
#include <string>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/system/error_code.hpp>
#endif

#include "../ext/thread-safe-stl-containers-svn/thread_safe_vector.h"
#include "../ext/thread-safe-stl-containers-svn/thread_safe_map.h"
#include "../ext/thread-safe-stl-containers-svn/thread_safe_set.h"
#include "../ext/thread-safe-stl-containers-svn/thread_safe_deque.h"

#include "base/resource_status.h"
#include "base/resource_desc.pb.h"
#include "base/job_desc.pb.h"
#include "base/task_graph.h"

using std::map;
using std::pair;
using std::string;

// Use TR1 implementation for hash map.
#include <tr1/unordered_map>
#include <tr1/unordered_set>

namespace firmament {

using tr1::unordered_map;
using tr1::unordered_set;

}  // namespace firmament

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
#include <boost/function.hpp>
#include <boost/functional/hash.hpp>
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

// TODO(malte): consider using Google's scoped_ptr implementation here (Apache
// 2.0 license).

// TODO(malte): Add support for scoped and weak pointers
#error "No current support for scoped_ptr and weak_ptr using yasper " \
    "Smart pointers not available on this platform; this needs fixing before " \
    "we can compile. Alternatively, install Boost if available."
}
#endif

#include "storage/reference_interface.h"

namespace firmament {

// Various utility typedefs
typedef uint32_t TaskOutputID_t;
typedef uint64_t TaskID_t;
#ifdef __PLATFORM_HAS_BOOST__
typedef boost::uuids::uuid ResourceID_t;
typedef boost::uuids::uuid JobID_t;
/*typedef unordered_map<ResourceID_t, ResourceStatus*,
        boost::hash<boost::uuids::uuid> > ResourceMap_t;
typedef unordered_map<JobID_t, JobDescriptor,
        boost::hash<boost::uuids::uuid> > JobMap_t;
typedef unordered_map<JobID_t, TaskGraph*,
        boost::hash<boost::uuids::uuid> > TaskGraphMap_t;*/
typedef thread_safe::map<ResourceID_t, ResourceStatus*> ResourceMap_t;
typedef thread_safe::map<JobID_t, JobDescriptor> JobMap_t;
typedef thread_safe::map<JobID_t, TaskGraph*> TaskGraphMap_t;
#else
typedef uint64_t ResourceID_t;
typedef uint64_t JobID_t;
typedef unordered_map<ResourceID_t, ResourceStatus*> ResourceMap_t;
typedef unordered_map<JobID_t, JobDescriptor> JobMap_t;
typedef unordered_map<JobID_t, TaskGraph*> TaskGraphMap_t;
#endif
// N.B.: the type of the second element here is a pointer, since the
// TaskDescriptor objects will be part of the JobDescriptor protobuf that is
// already held in the job table.
//typedef unordered_map<TaskID_t, TaskDescriptor*> TaskMap_t;
typedef thread_safe::map<TaskID_t, TaskDescriptor*> TaskMap_t;

#ifdef __PLATFORM_HAS_BOOST__
// Message handler callback type definition
template <typename T>
struct AsyncMessageRecvHandler {
  typedef boost::function<void(T* message,
                               const string& remote_endpoint)> type;
};

// Error handler callback type definition
template <typename T>
struct AsyncErrorPathHandler {
  typedef boost::function<void(const boost::system::error_code& error,
                               const string& remote_endpoint)> type;
};
#else
#error "Handler types do not currently have non-Boost versions."
#endif

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TYPES_H
