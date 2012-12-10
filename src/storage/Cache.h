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

namespace firmament { 
    namespace store {

class Cache {
public:
    Cache(ObjectStoreInterface* obj, size_t size_, const string& cache_name_);
    Cache(const Cache& orig);
    virtual ~Cache();
    
    /* For the moment, find out size from object table */
    void remove_from_cache(DataObjectID_t id) ; 
    
    bool write_object_to_cache(const ObtainObjectMessage&  msg);
    bool write_object_to_cache(DataObjectID_t id);
    
    inline size_t capacity() { return capacity ; } 
    inline const size_t getSize() { return size ; } 

    void print_cache() ; 
private:
        const string& cache_name ; 
        Cache_t* cache ; 
        volatile size_t capacity ; 
        const size_t size ; 
        ObjectStoreInterface* store ; 
        
        /* Currently doing LRU but could do anything else*/
        void make_space_in_cache() ;
        
        Cache_t* create_cache() ;
        
        void write_object_to_disk(DataObject_t id, void* data, size_t size);
        
        void clearCache() ; 
        
        
        
        
};

} //store
} // firmament
#endif	/* CACHE_H */

