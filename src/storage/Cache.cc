/* 
 * File:   Cache.cpp
 * Author: nscc2
 * 
 * Created on 07 December 2012, 13:46
 */

#include "Cache.h"

namespace firmament {
    namespace store {
        

Cache::Cache(SimpleObjectStore* obj, size_t size_, const char* cache_name_): cache_name(cache_name_), size(size_), cache(new Cache_t(size_)), store(obj) {
    VLOG(1) << "Setting up Cache of size: "  << size_  << endl ; 
    create_cache(cache_name); 
    
}

//Cache::Cache(const Cache& orig):cache_name(orig.cache_name), size(orig.size), cache(orig.cache), store(orig.store) {
//    
//}


Cache::~Cache() {
    clearCache() ; 
    shared_memory_object::remove("cache");
    
}

 
 /* Currently using LRU*/
void Cache::make_space_in_cache() { 
    
    /* Identify object to remove */
    DataObjectID_t id = *(cache->object_list->begin());
    ReferenceDescriptor* rd = store->GetReference(id); 
    size_t size = rd->size(); 
    cache_lock->lock(); 
    cache->capacity+=size ; 
    
    VLOG(1) << "Removing Object UUID: " <<  id ; 
      
    /* TODO: delete other files if this one is locked
     Right now will only block */
     named_mutex mut(open_only, StringFromDataObjectIdMut(id));
     WriteLock_t lock(mut); 
    lock.lock(); 
    file_mapping m_file(StringFromDataObjectId(id), read_only);
    mapped_region region(m_file, read_only);
    region.flush() ;   
    file_mapping::remove(StringFromDataObjectId(id)); 
    /*Erase from cache before releasing lock*/
    cache->object_list->erase(cache->object_list->begin());  /* LRU */ 
    lock.unlock() ;  
    named_mutex::remove(StringFromDataObjectIdMut(id));
    cache_lock->unlock(); 

}

/* From Remote TCP */
//TODO: not very efficient, shouldn't have to write to disk first, THEN
// map the file
bool Cache::write_object_to_cache(const ObtainObjectMessage&  msg) {
    
    DataObjectID_t id = msg.uuid() ; 
    const string& data = msg.data() ;
    size_t size = msg.size() ; 
    ofstream os;
    os.open (StringFromDataObjectId(id), ios::binary ); 
    os.write(data.c_str(),size);
    os.close();
    
    return write_object_to_cache(id) ; 
    
    
}

/* From File on disk */
bool Cache::write_object_to_cache(DataObjectID_t id) { 
    
     VLOG(1) << "Inserting in cache Object ID: " << id ; 
     
     ReferenceDescriptor* rd = store->GetReference(id); 
     
     size_t size = rd->size(); 
     //TODO concurrency control 
     
     cache_lock->lock(); 
     if (cache->capacity>=size) { 
        
         cache->capacity-=size;
         
         /* Map File Read Only */
         file_mapping m_file(StringFromDataObjectId(id), read_only);
         mapped_region region(m_file, read_only);
         
         /* Create mutex */
         named_mutex mutex(open_or_create, StringFromDataObjectIdMut(id) );
         
         cache->object_list->push_back(id); 
         
         return true ; 
     }
    cache_lock->unlock(); 

     make_space_in_cache() ; 
     return false ; 
     
    
  
}

/* From File on disk */
void Cache::write_object_to_disk(DataObjectID_t id, const char* data, size_t size) {
    
    ofstream os(StringFromDataObjectId(id));
    os.write (data,size);
    os.close();
    
    
  
}


void Cache::print_cache()
{
    VLOG(1) << "Cache contents : "  ; 
    SharedVector_t* list_objects = cache->object_list ; 
    for (SharedVector_t::iterator it = list_objects->begin() ; it!=list_objects->end() ; ++it) { 
        VLOG(1) << *it << endl ; 
    }
}

/* create a special (managed) shared memory segment, 
 * declare a Boost.Interprocess allocator and 
 * construct the map in shared memory just if it 
 * was any other object.*/
        

void Cache::create_cache(const char* cache_name) {
    
        /* Ensure no existing cache exists */
    
    shared_memory_object::remove(cache_name); 
    
   managed_shared_memory segment_(create_only ,cache_name  , size);       
      
   segment = &segment_; 
   
   const SharedmemAllocator_t alloc_inst(segment->get_segment_manager());
 
   SharedVector_t *vec = segment->construct<SharedVector_t>("objects")(alloc_inst);   
   size_t* capac = segment->construct<size_t>("size")(size); 


   named_mutex mutex_(open_or_create, cache_name);
   mutex = &mutex_ ; 
   
   WriteLock_t cache_lock_(*mutex);

   cache_lock = &cache_lock_ ; 
   cache->capacity = *capac ; 
   cache->object_list  = vec ; 
}
        
void Cache::clearCache() { 
    
    while (cache->object_list->empty()) { 
        make_space_in_cache(); 
    }
}



 

} //store

 } // firmament
