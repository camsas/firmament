// TODO: header

#include "sim/task_sim.h"

namespace firmament {

TaskSim::TaskSim(const string& name, double runtime)
  : Task(name),
    runtime_(runtime) {
  VLOG(1) << "Creating sim task " << name << " with run time " << runtime;
}

}  // namespace firmament
