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

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H

#include <string>
#include <vector>

#include "base/types.h"

#define NUM_CHANGE_TYPES 36

namespace firmament {

// Forward declaration, since DIMACSChange contains a DIMACSChangeStats
// instance itself
class DIMACSChange;

enum DIMACSChangeType {
  ADD_TASK_NODE = 0,
  ADD_RESOURCE_NODE = 1,
  ADD_EQUIV_CLASS_NODE = 2,
  ADD_UNSCHED_JOB_NODE = 3,
  ADD_SINK_NODE = 4,
  ADD_ARC_TASK_TO_EQUIV_CLASS = 5,
  ADD_ARC_TASK_TO_RES = 6,
  ADD_ARC_EQUIV_CLASS_TO_RES = 7,
  ADD_ARC_BETWEEN_EQUIV_CLASS = 8,
  ADD_ARC_BETWEEN_RES = 9,
  ADD_ARC_TO_UNSCHED = 10,
  ADD_ARC_FROM_UNSCHED = 11,
  ADD_ARC_RUNNING_TASK = 12,
  ADD_ARC_RES_TO_SINK = 13,
  DEL_UNSCHED_JOB_NODE = 14,
  DEL_TASK_NODE = 15,
  DEL_RESOURCE_NODE = 16,
  DEL_EQUIV_CLASS_NODE = 17,
  DEL_ARC_EQUIV_CLASS_TO_RES = 18,
  DEL_ARC_RUNNING_TASK = 19,
  DEL_ARC_EVICTED_TASK = 20,
  DEL_ARC_BETWEEN_EQUIV_CLASS = 21,
  DEL_ARC_BETWEEN_RES = 22,
  DEL_ARC_TASK_TO_EQUIV_CLASS = 23,
  DEL_ARC_TASK_TO_RES = 24,
  DEL_ARC_RES_TO_SINK = 25,
  CHG_ARC_EVICTED_TASK = 26,
  CHG_ARC_TO_UNSCHED = 27,
  CHG_ARC_FROM_UNSCHED = 28,
  CHG_ARC_TASK_TO_EQUIV_CLASS = 29,
  CHG_ARC_EQUIV_CLASS_TO_RES = 30,
  CHG_ARC_BETWEEN_EQUIV_CLASS = 31,
  CHG_ARC_BETWEEN_RES = 32,
  CHG_ARC_RUNNING_TASK = 33,
  CHG_ARC_TASK_TO_RES = 34,
  CHG_ARC_RES_TO_SINK = 35,
};

struct DIMACSChangeStats {
  uint64_t nodes_added_;
  uint64_t nodes_removed_;
  uint64_t arcs_added_;
  uint64_t arcs_changed_;
  uint64_t arcs_removed_;
  uint64_t num_changes_of_type_[NUM_CHANGE_TYPES];
  DIMACSChangeStats();
  ~DIMACSChangeStats();
  string GetStatsString() const;
  void ResetStats();
  void UpdateStats(DIMACSChangeType change_type);
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H
