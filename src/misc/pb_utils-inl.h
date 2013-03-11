// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Utility functions for working with protobufs.

#ifndef FIRMAMENT_MISC_PB_UTILS_INL_H
#define FIRMAMENT_MISC_PB_UTILS_INL_H

#include <string>

#include "base/common.h"

namespace firmament {

template <typename T>
bool RepeatedContainsPtr(RepeatedPtrField<T>* pbf, T* item) {
  // N.B.: using GNU-style RTTI
  for (typeof(pbf->pointer_begin()) iter =
       pbf->pointer_begin();
       iter != pbf->pointer_end();
       ++iter) {
    if (*iter == item)
      return true;
  }
  return false;
}

}  // namespace firmament

#endif  // FIRMAMENT_MISC_PB_UTILS_INL_H
