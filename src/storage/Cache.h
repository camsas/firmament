// The Firmament project
// Copyright (c) 2012-2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Simple in-memory cache implementation for object store.

#ifndef FIRMAMENT_STORAGE_CACHE_H
#define FIRMAMENT_STORAGE_CACHE_H

#include <string>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/segment_manager.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>

#include "base/types.h"
#include "messages/storage_message.pb.h"
#include "misc/utils.h"
#include "storage/object_store_interface.h"
#include "storage/reference_interface.h"
#include "storage/simple_object_store.h"
#include "storage/types.h"

namespace firmament {
namespace store {

// TODO(malte): replace this is with individual imports
using namespace boost::interprocess;  // NOLINT

class SimpleObjectStore;
class StorageInfo;

class Cache {
 public:
  Cache(SimpleObjectStore* obj, size_t size_,  string cache_name_);
  virtual ~Cache();

  bool obtain_object(DataObjectID_t id);
  void print_cache();
  // For the moment, find out size from object table
  void remove_from_cache(DataObjectID_t id);
  bool write_object_to_cache(const ObtainObjectMessage& msg);
  bool write_object_to_cache(DataObjectID_t id);

  inline size_t getCapacity() {
    return cache->capacity;
  }
  inline size_t getSize() {
    return size;
  }

 private:
  string cache_name;
  const size_t size;
  Cache_t* cache;
  SimpleObjectStore* store;
  managed_shared_memory* segment;
  named_mutex* mutex;
  scoped_lock<named_mutex>* cache_lock;

  void create_cache(const char* cache_name);
  void clearCache();
  void handle_notification_references(ReferenceNotification_t* ref);
  // Currently doing LRU but could do anything else
  void make_space_in_cache();
  void write_object_to_disk(DataObjectID_t id, const char* data, size_t size);
};

} // namespace store
} // namespace firmament
#endif  // FIRMAMENT_STORAGE_CACHE_H
