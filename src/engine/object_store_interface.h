// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The executor interface assumed by the engine.

#ifndef FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE_H
#define FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE_H

#include "base/common.h"
#include "base/types.h"
#include "misc/printable_interface.h"

namespace firmament {
namespace store {

class ObjectStoreInterface : public PrintableInterface {
 public:
  virtual ostream& ToString(ostream* stream) const = 0;
 protected:
};

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_OBJECT_STORE_INTERFACE
