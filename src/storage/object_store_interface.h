// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The executor interface assumed by the engine.

#ifndef FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE_H
#define FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE_H

#include "base/common.h"
#include "base/types.h"
#include "misc/printable_interface.h"
#include "storage/reference_interface.h"
#include "messages/storage_registration_message.pb.h"
#include "base/reference_desc.pb.h"
#include "storage/types.h"
#include "misc/map-util.h"

namespace firmament {
namespace store {

class ObjectStoreInterface : public PrintableInterface {
 public:
 
  virtual ostream& ToString(ostream* stream) const = 0;
  virtual void HandleStorageRegistrationRequest(const StorageRegistrationMessage& msg) = 0 ; 
  
  
  bool addReference(DataObjectID_t id, ReferenceDescriptor* rd) { 
            return !(InsertIfNotPresent(object_table_.get(), id, rd)) ; 
        }
  
  
  ReferenceDescriptor* GetReference(DataObjectID_t id) {
            ReferenceDescriptor* rd = *FindOrNull(*object_table_,id); 
            return rd ; 
           
        }


    
 
 inline string get_listening_interface() {
      return listening_interface_ ; 
  }
 
 inline ResourceID_t get_resource_id() {
      return uuid ; 
  }
 
   
 protected:
        string listening_interface_ ; 
        ResourceID_t uuid ; 
        shared_ptr<DataObjectMap_t> object_table_ ; 


};

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE
