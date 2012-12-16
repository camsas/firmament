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

Cache::Cache(ObjectStoreInterface* obj, size_t size_, const string& cache_name_): cache(new Cache_t), store(obj), size(size_), capacity(size_), cache_name(cache_name_)  {
    VLOG(1) << "Setting up Cache of size: "  << size  << endl ; 
    create_cache(cache_name); 
    
}

Cache::Cache(const Cache& orig) {
}

Cache::~Cache() {
    clearCache() ; 
    shared_memory_object::remove("cache");
    
}

 
 /* Currently using LRU*/
void Cache::make_space_in_cache() { 
    
    /* Identify object to remove */
    DataObjectID_t id = cache->begin()->first ;
    ReferenceDescriptor* rd = store->GetReference(id); 
     size_t size = rd->size();  
    this->capacity+=size ; 
    VLOG(1) << "Removing Object UUID: " <<  id ; 

    /* Make sure file will be written to disk  */
    DataObject_t obj = cache->begin()->second; 
    /* TODO: delete other files if this one is locked
     Right now will only block */
    obj.second.write_lock.lock() ; 
    file_mapping m_file(id, read_only);
    mapped_region region(m_file, read_only);
    region.flush() ;   
    /*Erase from cache before releasing lock*/
    cache->erase(cache->begin()) ;  /* LRU */ 
    obj.second.write_lock.unlock() ; 

}

/* From Remote TCP */
//TODO: not very efficient, shouldn't have to write to disk first, THEN
// map the file
bool Cache::write_object_to_cache(const ObtainObjectMessage&  msg) {
    
    DataObjectID_t id = msg.uuid() ; 
    void* data = msg.data() ; 
    size_t size = msg.size() ; 
    ifstream is;
    is.open (id, ios::binary ); 
    is.read (data,size);
    is.close();
    
    write_object_to_cache(id) ; 
    
}

/* From File on disk */
bool Cache::write_object_to_cache(DataObjectID_t id) { 
    
     VLOG(1) << "Inserting in cache Object ID: " << id ; 
     
     ReferenceDescriptor* rd = store->GetReference(id); 
     
     size_t size = rd->size(); 
     //TODO concurrency control  
     if (this->capacity>=size) { 
        
         this->capacity-=size;
         
         /* Map File Read Only */
         file_mapping m_file(id, read_only);
         mapped_region region(m_file, read_only);
         
         DataObject_t* object = new DataObject_t(id, new Mutex_t); 
         cache->insert(pair<id,object>); 
         return true ; 
     }
     
     make_space_in_cache() ; 
     return false ; 
     
    
  
}

/* From File on disk */
void Cache::write_object_to_disk(DataObject_t id, void* data, size_t size) {
    
    ofstream os(id);
    os.write (data,size);
    os.close();
    
    
  
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
        

void Cache::create_cache(const string& cache_name) {
    
        /* Ensure no existing cache exists */
    
    shared_memory_object::remove(cache_name); 
    
  managed_shared_memory segment(create_only ,cache_name  ,size);       
      
   SharedmemAllocator_t alloc_inst (segment.get_segment_manager());
   
   cache =   segment.construct<Cache_t>(cache_name)      
                                 (std::less<DataObjectID_t>() 
                                 ,alloc_inst);    
   
   
}
        
void Cache::clearCache() { 
    
    while (capacity!=size()) { 
        make_space_in_cache(); 
    }
}

} //store

 } // firmament
