// The Firmament project
// Copyright (c) 2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Type definitions used by the object store.

#ifndef FIRMAMENT_STORAGE_TYPES_H
#define FIRMAMENT_STORAGE_TYPES_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <thread_safe_map.h>

#include "storage/reference_interface.h"

namespace firmament {
namespace store {

typedef thread_safe::map<DataObjectID_t, set<ReferenceInterface*> >
    DataObjectMap_t;

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_STORAGE_TYPES_H
