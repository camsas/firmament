// TODO: header

#ifndef FIRMAMENT_SIM_ENSEMBLE_SIM_H
#define FIRMAMENT_SIM_ENSEMBLE_SIM_H

#include "base/common.h"
#include "base/ensemble.h"
#include "base/resource.h"

namespace firmament {

class EnsembleSim : public Ensemble {
 public:
  EnsembleSim(const string& name);
  void Join(Resource* res);
 private:
  vector<Ensemble*> peered_ensembles_;  // TODO: we may need more detail here
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_ENSEMBLE_SIM_H
