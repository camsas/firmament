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

namespace firmament {
namespace store {

class ObjectStoreInterface : public PrintableInterface {
 public:
  virtual void PutObject(DataObjectID_t id, void* data, size_t len) = 0;
  virtual bool GetObject(DataObjectID_t id, void*, size_t* len) = 0;
  virtual ostream& ToString(ostream* stream) const = 0;
  virtual void HandleStorageRegistrationRequest(const StorageRegistrationMessage& msg) ; 
  virtual ReferenceDescriptor*  GetReference(DataObjectID_t id );
  
  
  
 inline string get_listening_interface() {
      return listening_interface_ ; 
  }
 
 inline string get_resource_id() {
      return uuid ; 
  }
 
   
 protected:
        string listening_interface_ ; 
        string uuid ; 

};

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE
