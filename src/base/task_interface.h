// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task representation.

#ifndef FIRMAMENT_BASE_TASK_INTERFACE_H
#define FIRMAMENT_BASE_TASK_INTERFACE_H

#include <string>

#include "base/common.h"
#include "base/types.h"
#include "misc/printable_interface.h"
#include "storage/types.h"
#include "boost/interprocess/sync/interprocess_mutex.hpp"
#include <boost/interprocess/file_mapping.hpp>
#include "data_object.h"
#include <fstream>


namespace firmament {

    using namespace boost::interprocess;
    
// Main task invocation method. This will be linked to the
// implementation-specific task_main() procedure in the task implementation.
// TODO(malte): Ideally, we wouldn't need this level of indirection.
extern void task_main(TaskID_t task_id, Cache_t* cache );

class TaskInterface : public PrintableInterface {
 public:
  explicit TaskInterface(TaskID_t task_id, Cache_t* cache)
    : id_(task_id), cache(cache_) {}
  // Top-level task run invocation.
  virtual void Invoke() = 0;

  // Print-friendly representation
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Task, id=" << id_ << ">";
  }
  
  /* Finds data from cache and returns pointer. Acquires shared
   read lock. Contacts storage engine if data is not in cache */
  void* GetObjectStart(DataObjectID_t id ) { 
      VLOG(3) << "GetObjectStart " << id << endl ; 
      DataObject_t object = FindOrNull(cache,id); 
      if (object!=NULL) { 
          boost::interprocess::sync::lock((object.read_lock)) ;
         file_mapping m_file(id, read_only);
         mapped_region region(m_file, read_only);
         void* object = region.getAddress() ;
         return object ; 
      }
      else { 
          VLOG(1) << "Object " << id << " was not found in cache. Contacting"
                  "Object Store " << endl ; 
          
          return null ; 
          /* Data is not in cache - Let object store do the work */
          
          
          
          /* Local set up named conditions - wait until data is in
           cache */
          
          
          /* Non local - Not implemented */
      }
      
  }
  
  /* Releases shared read lock */
   void GetObjectEnd(DataObjectID_t id ) { 
       VLOG(3) << "GetObjectEnd " << endl ; 
      DataObject_t* object = FindOrNull(cache,id); 
      if (object!=NULL) { 
          boost::interprocess::sync::unlock((object->read_lock)) ;  
          file_mapping::remove(id); 
      }
      else {
          VLOG(1) << "This should not have happened. To fix " ; 
      }
      
  }
  
  void* PutObjectStart(DataObjectID_t id, size_t size) { 
    VLOG(3) << "PutObjectStart " << endl ;   
    /* Create file */
    std::ofstream ofs("id", std::ios::binary | std::ios::out);
    ofs.seekp(size - 1);
    ofs.write("", 1);
    /* Map it*/
    file_mapping m_file(id, read_write);
    mapped_region region(m_file, read_write);  
    void* address = region.getAddress() ;
    DataObject_t* object = new DataObject_t(id, new Mutex_t); 
    /* Acquire write lock */
    lock(object->write_lock) ;   
   /* Add it to cache */
    cache->insert(pair<id,object>); 
 
    return address ; 
  }
  
  /* Release exclusive lock */
  void PutObjectEnd(DataObjectID_t id) { 
      VLOG(3) << "PutObjectEnd " << endl ; 
      DataObject_t* object = FindOrNull(cache,id); 
      file_mapping m_file(id, read_write);
      mapped_region region(m_file, read_write);  
      region.flush() ;
      file_mapping::remove(id); 
      if (object!=NULL) { 
          boost::interprocess::sync::unlock((object->write_lock)) ;        
      }
      else {
          VLOG(1) << "This should not have happened. To fix " ; 
      }
  }
  
  

 private:
  // The task's unique identifier. Note that any TaskID_t is by definition
  // const, i.e. immutable.
  TaskID_t id_;
  string uri ; // Address of the storage engine 
  Cache_t* cache ; 
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_INTERFACE_H
