/* 
 * File:   Cache.cpp
 * Author: nscc2
 * 
 * Created on 07 December 2012, 13:46
 */

#include "simple_object_store.h"

#include "Cache.h"
#include "concrete_reference.h"

namespace firmament {
  namespace store {

    Cache::Cache(SimpleObjectStore* obj, size_t size_, string cache_name_) : cache_name(cache_name_), size(size_), cache(new Cache_t(size_)), store(obj) {
      VLOG(1) << "Setting up Cache of size: " << size_ << endl;
      create_cache(cache_name.c_str());

    }

    Cache::~Cache() {
      //      if (cache!= NULL)    { 
      //        clearCache();
      //        delete(cache);
      //        named_mutex::remove(cache_name.c_str());
      //        shared_memory_object::remove(cache_name.c_str());
      //      }
    }

    /* Currently using LRU*/
    void Cache::make_space_in_cache() {
      VLOG(3) << "Make Space in Cache ";
      /* Identify object to remove */
      SharedVector_t::iterator it = cache->object_list->begin();
      bool cleared = false;
      try {
        while (cleared != true || it != cache->object_list->end()) {
          DataObjectID_t id = *it;
          ReferenceDescriptor* rd = store->GetReference(id);
          size_t size = rd->size();
          cache_lock->lock();

          VLOG(3) << "Trying to remove Object UUID: " << id;

          /* TODO: delete other files if this one is locked
           Right now will only block */

          string file_name = boost::lexical_cast<string>(id); 
          string mutex_name = file_name + "mut"; 
          named_upgradable_mutex mut(open_only, mutex_name.c_str());

          WriteLock_t lock(mut, defer_lock);

          if (!(cleared = lock.try_lock())) {
            VLOG(3) << "Removal failed - Object in use";
            it++;
            continue;
          }

          file_mapping m_file(file_name.c_str(), read_only);
          mapped_region region(m_file, read_only);
          region.flush();
      //    file_mapping::remove(file_name.c_str());

          cache->object_list->erase(cache->object_list->begin()); /* LRU */
          lock.unlock();
          
          named_mutex::remove(mutex_name.c_str());

          cache->capacity += size;
          cache_lock->unlock();
          VLOG(3) << "Object Successfully Removed";
        }
      } catch (interprocess_exception& e) {
        VLOG(1) << "Error: make_space_in_cache" << endl;
      }
    }

    /* From Remote TCP */
    //TODO: not very efficient, shouldn't have to write to disk first, THEN
    // map the file

    bool Cache::write_object_to_cache(const ObtainObjectMessage& msg) {
      DataObjectID_t id = msg.uuid();
      VLOG(3) << "Writing Object " << id << " to Cache from TCP";
      const string& data = msg.data();
      size_t size = msg.size();
      ofstream os;
       string file_name =  boost::lexical_cast<string>(id); 

      os.open(file_name.c_str(), ios::binary|ios::out);
      /* TODO change to more efficient way */
      os.write(data.c_str(), size);
      os.close();

      return write_object_to_cache(id);


    }

    /* From File on disk */
    bool Cache::write_object_to_cache(DataObjectID_t id) {

      VLOG(3) << "Writing Object " << id << " to Cache from disk";

      try {
        ReferenceDescriptor* rd = store->GetReference(id);

        size_t size = rd->size();
        //TODO concurrency control 

        cache_lock->lock();
        if (cache->capacity >= size) {

          cache->capacity -= size;

          /* Map File Read Only */

          string mut_name_s =  boost::lexical_cast<string>(id); 

                    
          file_mapping m_file(mut_name_s.c_str(), read_only);
          mapped_region region(m_file, read_only);

          /* Create mutex */
          mut_name_s+="mut" ;
          named_mutex mutex(open_or_create, mut_name_s.c_str());

          cache->object_list->push_back(id);

          return true;
        }
        cache_lock->unlock();

        make_space_in_cache();

      } catch (interprocess_exception& e) {
        VLOG(1) << "Writing Object " << endl;
      }

      return false;



    }

    /* From File on disk */
    void Cache::write_object_to_disk(DataObjectID_t id, const char* data, size_t size) {
      VLOG(3) << "Writing Object " << id << " to disk ";
      string file_name = boost::lexical_cast<string>(id); 
      ofstream os(file_name.c_str(), std::ios::binary | std::ios::out);
      os.write(data, size);
      os.close();



    }

    void Cache::print_cache() {
      VLOG(1) << "Cache contents : ";
      SharedVector_t* list_objects = cache->object_list;
      for (SharedVector_t::iterator it = list_objects->begin(); it != list_objects->end(); ++it) {
        VLOG(1) << *it << endl;
      }
    }

    /* create a special (managed) shared memory segment, 
     * declare a Boost.Interprocess allocator and 
     * construct the map in shared memory just if it 
     * was any other object.*/


    void Cache::create_cache(const char* cache_name) {
      VLOG(3) << "Creating cache " << cache_name << endl;
      /* Ensure no existing cache exists */

      try {
        shared_memory_object::remove(cache_name);

        size_t cache_n = 1024; /* This is not the size of the cache
                                   * but the number of max items we allow
                                   TODO: add it as parameter
                                   */
        size_t cache_size = sizeof (ReferenceNotification_t) + sizeof (size_t) +
                sizeof (vector<DataObjectID_t>) + sizeof (DataObjectID_t) * cache_n;
      
        managed_shared_memory segment_(create_only, cache_name, cache_size);

        segment = &segment_;

        const SharedmemAllocator_t alloc_inst(segment->get_segment_manager());

        SharedVector_t *vec =
                segment->construct<SharedVector_t > ("objects")(alloc_inst);
        size_t* capac =
                segment->construct<size_t > ("capacity")(size);
        
        size_t* size_ =  segment->construct<size_t >("size")(size);




        /* Temporary Hack until I implement cleaner memory channel */

        ReferenceNotification_t* reference_not_t =
                segment->construct<ReferenceNotification_t > ("refnot")();
        reference_not_t = new ReferenceNotification_t();

        //      boost::thread t(
        //        boost::bind(
        //        boost::mem_fn(&Cache::handle_notification_references), this, _1)(
        //              reference_not_t));

        boost::thread t(&Cache::handle_notification_references, *this, reference_not_t);

        /* End of Hack */

        VLOG(3) << "Acquiring Cache name  mutex" << endl;

        named_mutex mutex_(open_or_create, cache_name);

        VLOG(3) << "Acquired Cache name mutex " << endl;

        mutex = &mutex_;

        scoped_lock<named_mutex> cache_lock_(mutex_, defer_lock);
        
        VLOG(3) << "Created Cache lock " << endl ; 
        
        cache_lock = &cache_lock_;
        cache->capacity = *capac;
        cache->size = *size_ ; 
        cache->object_list = vec;
      } catch (interprocess_exception& e) {
        VLOG(1) << "Error: creating cache " << endl;
      }

      VLOG(3) << "Cache created successfully " << cache_name << endl;
    }

    void Cache::clearCache() {
      VLOG(3) << "Clearing Cache ";
      if (cache->object_list != NULL) {
        while (cache->object_list->empty()) {
          make_space_in_cache();
        }
      }
    }

    /* Temporary - Move to channel abstraction in future */
    void Cache::handle_notification_references(ReferenceNotification_t* ref) {

      VLOG(3) << "Setting up thread to handle notifications references";

      try {
      while (true) {
        VLOG(3) << "Acquiring lock" << endl;
        scoped_lock<interprocess_mutex> lock(ref->mutex);
        VLOG(3) << "Acquired lock" << endl;
        while (ref->writable) {
          VLOG(3) << "Waiting for reference to become readable " << endl;
          ref->cond_added.wait(lock);
        }
        DataObjectID_t id = ref->id;
        ReferenceDescriptor* rd = store->GetReference(id);
        switch (rd->type()) {
          case (ReferenceDescriptor::CONCRETE):
          {
            /* Was already concrete. Add 
             this location to reference
             Adding listening interface directly
             rather than ResourceID for now*/
            rd->add_location(store->get_listening_interface());
          }
            break;

          case (ReferenceDescriptor::FUTURE):
          {
            /* Was future. Make Concrete now*/
            VLOG(3) << "Reference was a future. Making concrete ";
            set<string> loc;
            loc.insert(store->get_listening_interface());
            ConcreteReference* conc_ref = new ConcreteReference(id, ref->size, loc);
            rd = new ReferenceDescriptor(conc_ref->desc());
          }
            break;

          default:
          {
            VLOG(1) << "Unimplemented ";
          }
            break;

        }
        ref->writable = true;
        lock.unlock();
        ref->cond_read.notify_one(); /* Only notify one given only one 
                                               * will be able to write */

      }
      } catch (interprocess_exception& e) { 
          VLOG(1) << "Handling Notification Reference Error " << endl ; 
        }

    }


  } //store

} // firmament
