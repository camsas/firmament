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

namespace firmament { 
    namespace store {

class Cache {
public:
    Cache(ObjectStoreInterface* obj);
    Cache(const Cache& orig);
    virtual ~Cache();
    
    /* For the moment, find out size from object table */
    bool insert_in_cache(DataObjectID_t id, void* data) ; 
    void remove_from_cache(DataObjectID_t id) ; 
    
    
    inline size_t capacity() { return capacity ; } 
    inline const size_t getSize() { return size ; } 

    void print_cache() ; 
private:
        Cache_t* cache ; 
        volatile size_t capacity ; 
        const size_t size ; 
        ObjectStoreInterface* store ; 
        
        /* Currently doing LRU but could do anything else*/
        void make_space_in_cache() ;
        
        Cache_t* create_cache() ; 

        
        
        
        
};

} //store
} // firmament
#endif	/* CACHE_H */

