// TODO: header

#ifndef FIRMAMENT_SIM_SIM_COMMON_H
#define FIRMAMENT_SIM_SIM_COMMON_H

#include <gflags/gflags.h>

namespace firmament {

DECLARE_double(simulation_runtime);
DECLARE_double(job_size_lambda);
DECLARE_double(job_arrival_lambda);
DECLARE_double(task_duration_scaling_factor);
DECLARE_double(job_arrival_scaling_factor);

}

#endif  // FIRMAMENT_SIM_SIM_COMMON_H
