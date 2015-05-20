// The Firmament project
// Copyright (c) 2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The interface assumed by object stores.

#ifndef FIRMAMENT_STORAGE_OBJECT_STORE_INTERFACE_H
#define FIRMAMENT_STORAGE_OBJECT_STORE_INTERFACE_H

#include <set>
#include <string>

#include "base/common.h"
#include "base/reference_desc.pb.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/printable_interface.h"
#include "messages/storage_registration_message.pb.h"
#include "storage/types.h"
#include "storage/reference_interface.h"
#include "storage/reference_utils.h"

namespace firmament {
namespace store {

class ObjectStoreInterface : public PrintableInterface {
 public:
  virtual ostream& ToString(ostream* stream) const = 0;
  bool AddReference(const DataObjectID_t& id, ReferenceDescriptor* rd) {
    // Check if we have an entry for this reference already
    set<ReferenceInterface*>* held_set = FindOrNull(*object_table_, id);
    if (!held_set) {
      // New reference -- insert a new set
      set<ReferenceInterface*> set;
      CHECK(InsertIfNotPresent(object_table_.get(), id, set));
      held_set = FindOrNull(*object_table_, id);
    }
    ReferenceInterface* ref = ReferenceFromDescriptor(*rd);
    bool ret = held_set->insert(ref).second;
    return !ret;
  }
  set<ReferenceInterface*>* GetReferences(const DataObjectID_t& id) {
    set<ReferenceInterface*>* rd = FindOrNull(*object_table_, id);
    if (rd!= NULL)
      return rd;
    else
      return NULL;
  }
  void Flush() {
    VLOG(1) << "Flushing all objects from store " << *this;
    object_table_->clear();
  }
  void DumpObjectTableContents() {
    VLOG(1) << "OBJECT TABLE (" << object_table_->size() << " elements):";
    for (DataObjectMap_t::const_iterator it = object_table_->begin();
         it != object_table_->end();
         ++it) {
      VLOG(1) << it->first << ": " << it->second.size() << " refs";
      for (set<ReferenceInterface*>::const_iterator rd_iter =
           it->second.begin();
           rd_iter != it->second.end();
           ++rd_iter)
        VLOG(1) << "+ " << (*rd_iter)->desc().DebugString();
    }
  }
  inline uint64_t NumTotalReferences() {
    return object_table_->size();
  }
  inline uint64_t NumReferencesOfType(
      ReferenceDescriptor::ReferenceType) {
    // TODO(malte): stub
    return 0;
  }
  inline string get_listening_interface() {
    return listening_interface_;
  }
  inline ResourceID_t get_resource_id() {
    return uuid;
  }
  // XXX(malte): Hack! This shouldn't be exposed permanently.
  inline boost::shared_ptr<DataObjectMap_t> object_table() {
    return object_table_;
  }

 protected:
  string listening_interface_;
  ResourceID_t uuid;
  shared_ptr<DataObjectMap_t> object_table_;
};

} // namespace store
} // namespace firmament

#endif  // FIRMAMENT_STORAGE_OBJECT_STORE_INTERFACE
