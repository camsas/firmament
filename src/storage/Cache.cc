// The Firmament project
// Copyright (c) 2012-2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Simple in-memory cache implementation for object store.

#include "storage/simple_object_store.h"

#include <set>
#include <string>
#include <vector>

#include "storage/Cache.h"
#include "storage/concrete_reference.h"

namespace firmament {
namespace store {

Cache::Cache(SimpleObjectStore* obj, size_t size_, string cache_name_)
    : cache_name(cache_name_), size(size_), cache(new Cache_t(size_)),
      store(obj) {
  VLOG(1) << "Setting up Cache of size: " << size_ << endl;
  create_cache(cache_name.c_str());
}

Cache::~Cache() {
// if (cache!= NULL)    {
//   clearCache();
//   named_mutex::remove(cache_name.c_str());
//   shared_memory_object::remove(cache_name.c_str());
// }
}

// Currently using LRU
void Cache::make_space_in_cache() {
  VLOG(3) << "Make Space in Cache ";
  // Identify object to remove
  SharedVector_t::iterator it = cache->object_list->begin();
  bool cleared = false;
  try {
    while (cleared != true || it != cache->object_list->end()) {
      DataObjectID_t id = *it;
      ReferenceDescriptor* rd = store->GetReference(id);
      size_t size = rd->size();
      cache_lock->lock();

      VLOG(3) << "Trying to remove Object UUID: " << id;

      // TODO(tach): delete other files if this one is locked
      // Right now will only block
      string file_name = boost::lexical_cast<string > (id);
      string mutex_name = file_name + "mut";
      named_upgradable_mutex mut(open_only, mutex_name.c_str());

      WriteLock_t lock(mut, boost::interprocess::defer_lock);

      if (!(cleared = lock.try_lock())) {
        VLOG(3) << "Removal failed - Object in use";
        it++;
        continue;
      }

      file_mapping m_file(file_name.c_str(), read_only);
      mapped_region region(m_file, read_only);
      region.flush();
      // file_mapping::remove(file_name.c_str());

      cache->object_list->erase(cache->object_list->begin()); // LRU
      lock.unlock();

      named_mutex::remove(mutex_name.c_str());

      cache->capacity += size;
      cache_lock->unlock();
      VLOG(3) << "Object Successfully Removed";
    }
  } catch (const interprocess_exception& e) {  // NOLINT
    VLOG(1) << "Error: make_space_in_cache" << endl;
  }
}

// From Remote. Using a write through strategy for now
bool Cache::write_object_to_cache(const ObtainObjectMessage& msg) {
  DataObjectID_t id(msg.object_id());
  VLOG(3) << "Writing Object " << id << " to Cache from remote";
  const string& data = msg.data();
  size_t size = msg.size();
  ofstream os;
  string file_name = boost::lexical_cast<string > (id);

  os.open(file_name.c_str(), ios::binary | ios::out);
  // TODO(tach) change to more efficient way
  os.write(data.c_str(), size);
  os.close();

  return write_object_to_cache(id);
}

// From File on disk
bool Cache::write_object_to_cache(const DataObjectID_t& id) {
  VLOG(3) << "Writing Object " << id << " to Cache from disk";

  try {
    ReferenceDescriptor* rd = store->GetReference(id);

    size_t size = rd->size();
    // TODO(tach) concurrency control

    cache_lock->lock();
    if (cache->capacity >= size) {
      cache->capacity -= size;

      /* Map File Read Only */
      string mut_name_s = boost::lexical_cast<string > (id);

      file_mapping m_file(mut_name_s.c_str(), read_only);
      mapped_region region(m_file, read_only);

      /* Create mutex */
      mut_name_s += "mut";
      named_mutex mutex(open_or_create, mut_name_s.c_str());

      cache->object_list->push_back(id);

      return true;
    }
    cache_lock->unlock();
    make_space_in_cache();
  } catch (const interprocess_exception& e) {  // NOLINT
    VLOG(1) << "Writing Object " << endl;
  }
  return false;
}

/* From File on disk */
void Cache::write_object_to_disk(DataObjectID_t id, const char* data,
                                 size_t size) {
  VLOG(3) << "Writing Object " << id << " to disk ";
  string file_name = boost::lexical_cast<string > (id);
  ofstream os(file_name.c_str(), std::ios::binary | std::ios::out);
  os.write(data, size);
  os.close();
}

void Cache::print_cache() {
  VLOG(1) << "Cache contents : ";
  SharedVector_t* list_objects = cache->object_list;
  for (SharedVector_t::iterator it = list_objects->begin();
       it != list_objects->end();
       ++it) {
    VLOG(1) << *it << endl;
  }
}

// create a special (managed) shared memory segment,
// declare a Boost.Interprocess allocator and
// construct the map in shared memory just if it
// was any other object.
void Cache::create_cache(const char* cache_name) {
  VLOG(3) << "Creating cache " << cache_name << endl;
  try {
    // Ensure no existing cache exists
    shared_memory_object::remove(cache_name);
    named_mutex::remove(cache_name);

    size_t cache_n = 1024; // This is not the size of the cache
                           // but the number of max items we allow
                           // TODO(tach): add it as parameter
    size_t cache_size = sizeof(ReferenceNotification_t) + sizeof(size_t) +
        sizeof(vector<DataObjectID_t>) + sizeof(DataObjectID_t) * cache_n;

    managed_shared_memory* segment =
        new managed_shared_memory(create_only, cache_name, cache_size);

    const SharedmemAllocator_t alloc_inst(segment->get_segment_manager());

    SharedVector_t *vec =
            segment->construct<SharedVector_t > ("objects")(alloc_inst);
    size_t* capac =
            segment->construct<size_t > ("capacity")(size);

    size_t* size_ = segment->construct<size_t > ("size")(size);

    /* Temporary Hack until I implement cleaner memory channel */
    ReferenceNotification_t* reference_not_t =
            segment->construct<ReferenceNotification_t>("refnot")();
    reference_not_t = new ReferenceNotification_t();

    boost::thread t(&Cache::handle_notification_references, *this,
                    reference_not_t);
    /* End of Hack */

    VLOG(3) << "Acquiring Cache name mutex" << endl;
    mutex = new named_mutex(open_or_create, cache_name);
    VLOG(3) << "Acquired Cache name mutex " << endl;
    cache_lock = new  scoped_lock<named_mutex>(*mutex,
                                               boost::interprocess::defer_lock);
    VLOG(3) << "Created Cache lock " << endl;

    cache->capacity = *capac;
    cache->size = *size_;
    cache->object_list = vec;
  } catch (const interprocess_exception& e) {  // NOLINT
    VLOG(1) << "Error: creating cache ";
  }
  VLOG(3) << "Cache created successfully " << cache_name;
}

void Cache::clearCache() {
// VLOG(3) << "Clearing Cache ";
// if (cache->object_list != NULL) {
//   while (cache->object_list->empty()) {
//     make_space_in_cache();
//   }
// }
}

bool Cache::obtain_object(const DataObjectID_t& id) {
  VLOG(1) << "ObtainObject: Unimplemented" << endl;
  /* Contact object store  */
  store->obtain_object_remotely(id);
  return false;
}

// Temporary - Move to channel abstraction in future
void Cache::handle_notification_references(ReferenceNotification_t* ref) {
  VLOG(3) << "Setting up thread to handle notifications references";
  try {
    while (true) {
      VLOG(3) << "Acquiring lock";
      scoped_lock<interprocess_mutex> lock(ref->mutex);
      VLOG(3) << "Acquired lock";
      while (ref->writable) {
        VLOG(3) << "Waiting for reference to become readable ";
        ref->cond_added.wait(lock);
      }
      VLOG(3) << "Cache: message received. Processing ";
      switch (ref->request_type) {
        case PUT_OBJECT:
        {
          VLOG(3) << "Type of message: PUT_OBJECT";
          ReferenceDescriptor* rd = store->GetReference(*ref->id);
          switch (rd->type()) {
            case ReferenceDescriptor::CONCRETE:
            {
              // Was already concrete. Add this location to reference
              // Adding resource ID directly rather than listening interface for
              // now
              rd->add_location(lexical_cast<string>(store->get_resource_id()));
              ref->request_type = FREE;
              rd->set_is_modified(true);
            }
              break;
            case ReferenceDescriptor::FUTURE:
            {
              // Was future. Make Concrete now
              VLOG(3) << "Reference was a future. Making concrete ";
              set<string> loc;
              loc.insert(store->get_listening_interface());
              ConcreteReference* conc_ref =
                  new ConcreteReference(*ref->id, ref->size, loc);
              rd = new ReferenceDescriptor(conc_ref->desc());
              ref->request_type = FREE;
              rd->set_is_modified(true);
            }
              break;
            default:
            {
              VLOG(1) << "Unimplemented ";
            }
          }
        }
          break;
        case GET_OBJECT:
        {
          // TODO(tach): Make asynchronous
          VLOG(3) << "Type of message: GE_OBJECT";
          ref->success = obtain_object(*ref->id);
        }
          break;
        case FREE:
          // Fall through
        default:
          VLOG(1) << "Request not recognised, ignored";
          break;
      }

      ref->writable = true;
//          lock.unlock();
      VLOG(3) << "Finished processing message ";
      ref->cond_read.notify_all(); // Only notify one given only onei
                                   // will be able to write
    }
  } catch (const interprocess_exception& e) {  // NOLINT
    VLOG(1) << "Handling Notification Reference Error " << endl;
  }
}

} // namespace store
} // namespace firmament
