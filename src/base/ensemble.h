// TODO: header

#ifndef FIRMAMENT_BASE_ENSEMBLE_H
#define FIRMAMENT_BASE_ENSEMBLE_H

#include <vector>

#include "base/common.h"
#include "base/resource.h"

namespace firmament {

class Ensemble {
 public:
  Ensemble(const string& name);
  void AddResource(Resource& resource);
 private:
  vector<Resource*> resources_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_ENSEMBLE_H
