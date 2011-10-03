// TODO: header

#include <glog/logging.h>

#include "base/ensemble.h"

namespace firmament {

Ensemble::Ensemble(const string& name) {
  LOG(INFO) << "Ensemble \"" << name << "\" constructed.";
}

void Ensemble::AddResource(Resource& resource) {
  CHECK_GE(resources_.size(), 0);
  resources_.push_back(&resource);
}

}  // namespace firmament
