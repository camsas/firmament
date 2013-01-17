/* 
 * File:   types.h
 * Author: nscc2
 *
 * Created on 04 December 2012, 14:41
 */

#ifndef TYPES_H
#define	TYPES_H

#include "storage/reference_interface.h"
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/sync/upgradable_lock.hpp>
#include <boost/interprocess/sync/lock_options.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/named_upgradable_mutex.hpp>

namespace firmament {
  namespace store {

    using namespace boost::interprocess;

    typedef map<DataObjectID_t, ReferenceDescriptor*> DataObjectMap_t;

    //    typedef struct  mutex_struct() { 
    //             sharable_lock<interprocess_mutex> read_lock(interprocess_mutex); 
    //             scoped_lock<interprocess_mutex> write_lock(interprocess_mutex); 
    //
    //             mutex_struct() {} ; 
    //
    //    } Mutex_t;


    //   typedef pair<DataObjectID_t, Mutex_t> DataObject_t ; 

    // typedef boost::interprocess::allocator<DataObject_t, managed_shared_memory::segment_manager>  SharedmemAllocator_t;

    // typedef map<DataObjectID_t, DataObject_t, SharedmemAllocator_t> Cache_t;

    typedef boost::interprocess::allocator<DataObjectID_t, managed_shared_memory::segment_manager> SharedmemAllocator_t;

    typedef vector<DataObjectID_t, SharedmemAllocator_t> SharedVector_t;

    
    typedef struct cache {
      size_t capacity;
      
      size_t size ; 
      //       offset_ptr<SharedVector_t> ; 
      SharedVector_t* object_list;

      cache(size_t cap) : capacity(cap) {
      }

    } Cache_t;

    typedef sharable_lock<named_upgradable_mutex> ReadLock_t;

    typedef scoped_lock<named_upgradable_mutex> WriteLock_t;
    
    enum notification_types {
      PUT_OBJECT, 
      GET_OBJECT, 
      FREE
    };

    typedef struct RefNot {       
      
      DataObjectID_t id;
     
      interprocess_mutex mutex;

      interprocess_condition cond_read;

      interprocess_condition cond_added;

      bool writable;

      size_t size;
      
      notification_types request_type; 
      
      bool success; 
      
      RefNot():writable(true) {} ;
      
    } ReferenceNotification_t;

  }
}

#endif	/* TYPES_H */

