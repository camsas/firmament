// The Firmament project
// Copyright (c) 2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Type definitions used by the object store.

#ifndef FIRMAMENT_STORAGE_TYPES_H
#define FIRMAMENT_STORAGE_TYPES_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/sync/upgradable_lock.hpp>
#include <boost/interprocess/sync/lock_options.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/named_upgradable_mutex.hpp>
#endif

#include <thread_safe_map.h>

#include "storage/reference_interface.h"
#include "storage/StorageInfo.h"

namespace firmament {
namespace store {

// TODO(malte): import individual classes instead of the namespace
using namespace boost::interprocess;  // NOLINT

// Forward declaration
class StorageInfo;

typedef thread_safe::map<DataObjectID_t, set<ReferenceInterface*> >
    DataObjectMap_t;

//    typedef struct  mutex_struct() {
//             sharable_lock<interprocess_mutex> read_lock(interprocess_mutex);
//             scoped_lock<interprocess_mutex> write_lock(interprocess_mutex);
//
//             mutex_struct() {} ;
//
//    } Mutex_t;


// typedef pair<DataObjectID_t, Mutex_t> DataObject_t ;

// typedef boost::interprocess::allocator<DataObject_t,
//         managed_shared_memory::segment_manager>  SharedmemAllocator_t;

// typedef map<DataObjectID_t, DataObject_t, SharedmemAllocator_t> Cache_t;

typedef boost::interprocess::allocator<DataObjectID_t,
        managed_shared_memory::segment_manager> SharedmemAllocator_t;

typedef vector<DataObjectID_t, SharedmemAllocator_t> SharedVector_t;

typedef map<string, StorageInfo*> NodeTable_t;

typedef struct cache {
  size_t capacity;
  size_t size;
  // offset_ptr<SharedVector_t> ;
  SharedVector_t* object_list;
  cache(size_t cap) : capacity(cap) { }
} Cache_t;

typedef sharable_lock<named_upgradable_mutex> ReadLock_t;

typedef scoped_lock<named_upgradable_mutex> WriteLock_t;

enum notification_types {
  PUT_OBJECT,
  GET_OBJECT,
  FREE
};

typedef struct RefNot {
  ReferenceDescriptor rd;
  interprocess_mutex mutex;
  interprocess_condition cond_read;
  interprocess_condition cond_added;
  bool writable;
  size_t size;
  notification_types request_type;
  bool success;

  // Constructor
  RefNot() : writable(true),  size(0), request_type(FREE), success(false) {}
} ReferenceNotification_t;

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_STORAGE_TYPES_H
