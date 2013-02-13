/* 
 * File:   Cache.h
 * Author: nscc2
 *
 * Created on 07 December 2012, 13:46
 */

#ifndef CACHE_H
#define	CACHE_H

#include "storage/types.h"
#include "base/types.h"
#include "storage/reference_interface.h"
#include "storage/object_store_interface.h"
#include "boost/interprocess/segment_manager.hpp"
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <messages/storage_message.pb.h>
#include "misc/utils.h"
#include "simple_object_store.h"


namespace firmament {
  namespace store {

    using namespace boost::interprocess;

    class SimpleObjectStore;
    class StorageInfo;

    class Cache {
    public:
      Cache(SimpleObjectStore* obj, size_t size_,  string cache_name_);
      
      
      //   Cache(const Cache& orig);
      virtual ~Cache();

      /* For the moment, find out size from object table */
      void remove_from_cache(DataObjectID_t id);

      bool write_object_to_cache(const ObtainObjectMessage& msg);
      bool write_object_to_cache(DataObjectID_t id);
      
      bool obtain_object(DataObjectID_t id) ; 

      inline size_t getCapacity() {
        return cache->capacity;
      }

      inline size_t getSize() {
        return size;
      }

      void print_cache();
    private:
      string cache_name;
      const size_t size;
      Cache_t* cache;
      SimpleObjectStore* store;
      managed_shared_memory* segment;
      named_mutex* mutex;
      scoped_lock<named_mutex>* cache_lock;

      /* Currently doing LRU but could do anything else*/
      void make_space_in_cache();
      void create_cache(const char* cache_name);

      void write_object_to_disk(DataObjectID_t id, const char* data, size_t size);

      void clearCache();

      void handle_notification_references(ReferenceNotification_t* ref);






    };

  } //store
} // firmament
#endif	/* CACHE_H */

