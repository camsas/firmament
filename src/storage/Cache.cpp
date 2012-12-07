/* 
 * File:   Cache.cpp
 * Author: nscc2
 * 
 * Created on 07 December 2012, 13:46
 */

#include "Cache.h"

namespace firmament {
    namespace store {
        
           using boost::interprocess;

Cache::Cache(ObjectStoreInterface* obj, size_t size_): cache(new Cache_t), store(obj), size(size_), capacity(size_)  {
    VLOG(1) << "Setting up Cache of size: "  << size  << endl ; 
    
    
}

Cache::Cache(const Cache& orig) {
}

Cache::~Cache() {
    shared_memory_object::remove("cache");
    
}

 bool Cache::insert_in_cache(DataObjectID_t id, void* data)  { 
     
     VLOG(1) << "Inserting in cache Object ID: " << id ; 
     
     ReferenceDescriptor* rd = store->GetReference(id); 
     
     size_t size = rd->size(); 
     //TODO concurrency control  
     if (this->capacity>=size) { 
        
         this->capacity-=size;
         cache->insert(pair<id,<pair<data, size()> >); 
         return true ; 
     }
     
     make_space_in_cache() ; 
     return false ; 
 }
 
 /* Currently using LRU*/
void Cache::make_space_in_cache() { 
    
    /* Identify object to remove */
    size_t size = cache->right.begin()->first->second ; 
    this->capacity+=size ; 
    VLOG(1) << "Removing Object UUID: " << cache->right.begin()->second ; 

    cache->right.erase(cache->right.begin()) ;  /* LRU */ 

}


void Cache::print_cache()
{
    VLOG(1) << "Cache contents : "  ; 
    for( Cache_t::const_iterator iter = cache->begin(), iend = cache.end(); iter != iend; ++iter )
    {
        VLOG(1) << iter->first << "-" << iter->second->second << endl;
    }
}

/* create a special (managed) shared memory segment, 
 * declare a Boost.Interprocess allocator and 
 * construct the map in shared memory just if it 
 * was any other object.*/
        

void Cache::create_cache() {
    

   shared_memory_object shm_obj(open_or_create,"cache",read_only );
   shm_obj.truncate(size);
   mapped_region region(shm, read_write);
   region.getAddress() ; 
}
        

} //store

 } // firmament
