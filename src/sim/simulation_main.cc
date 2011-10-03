// TODO: header

#include <stdint.h>
#include <iostream>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "base/common.h"
#include "sim/workload_generator.h"
#include "sim/ensemble_sim.h"

using namespace firmament;

DEFINE_double(simulation_runtime, 100.0, "Total duration of simulation.");

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  WorkloadGenerator wl_gen;

  // Our simulated "cluster"
  EnsembleSim cluster;

  LOG(INFO) << "Running for a total simulation time of "
            << FLAGS_simulation_runtime;
  double time = 0.0;

  while (time < FLAGS_simulation_runtime) {
    double time_to_next_arrival = wl_gen.GetNextInterarrivalTime();
    uint64_t job_size = wl_gen.GetNextJobSize();
    VLOG(1) << "New job arriving at " << (time + time_to_next_arrival) << ", "
            << "containing " << job_size << " tasks.";
    time += time_to_next_arrival;
  }
}
