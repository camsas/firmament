// TODO: header

#include "sim/ensemble_sim.h"

namespace firmament {

EnsembleSim::EnsembleSim(const string& name) : Ensemble(name) {

}

void EnsembleSim::Join(Resource* res) {
  AddResource(*res);
}

}  // namespace firmament
