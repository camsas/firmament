/* 
 * File:   types.h
 * Author: nscc2
 *
 * Created on 04 December 2012, 14:41
 */

#ifndef TYPES_H
#define	TYPES_H

#include "storage/reference_interface.h"
#include "boost/bimap.hpp"
#include <boost/bimap/list_of.hpp> 
#include <boost/bimap/set_of.hpp> 
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

namespace firmament { 
namespace store { 
    
    typedef map<DataObjectID_t, ReferenceDescriptor> DataObjectMap_t;
    
      typedef struct Mutex_t { 
        
        boost::interprocess::interprocess_mutex mut;
        
        boost::shared_lock<boost::interprocess::interprocess_mutex> read_lock(mut, defer_lock); 
        boost::unique_lock<boost::interprocess::interprocess_mutex> write_lock(mut, defer_lock);
    };
    
   typedef pair<DataObjectID_t, Mutex_t> DataObject_t ; 
     
   typedef allocator<DataObject_t, boost::interprocess::managed_shared_memory::segment_manager>  SharedmemAllocator_t;

   
   typedef map<DataObjectID_t, DataObject_t, SharedmemAllocator_t> Cache_t;
     
     
  
    

}
}

#endif	/* TYPES_H */

