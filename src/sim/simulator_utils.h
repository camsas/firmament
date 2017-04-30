/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#ifndef FIRMAMENT_SIM_SIMULATOR_UTILS_H
#define FIRMAMENT_SIM_SIMULATOR_UTILS_H

#include "base/types.h"

namespace firmament {
namespace sim {

/**
 * Computes the new total run time of a task.
 * NOTE: This method differs from the method with the same name in utils
 * because it uses simulated_time rather than task_finish_time. This
 * method should only be used in the simulator.
 */
uint64_t ComputeTaskTotalRunTime(uint64_t current_time_us,
                                 const TaskDescriptor& td);

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMULATOR_UTILS_H
