// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H

#include <string>
#include <vector>

#include "base/types.h"

#define NUM_CHANGE_TYPES 30

namespace firmament {

// Forward declaration, since DIMACSChange contains a DIMACSChangeStats
// instance itself
class DIMACSChange;

enum ChangeType {
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
  DEL_UNSCHED_JOB_NODE = 13,
  DEL_TASK_NODE = 14,
  DEL_RESOURCE_NODE = 15,
  DEL_EQUIV_CLASS_NODE = 16,
  DEL_ARC_EQUIV_CLASS_TO_RES = 17,
  DEL_ARC_RUNNING_TASK = 18,
  DEL_ARC_EVICTED_TASK = 19,
  DEL_ARC_BETWEEN_EQUIV_CLASS = 20,
  CHG_ARC_EVICTED_TASK = 21,
  CHG_ARC_TO_UNSCHED = 22,
  CHG_ARC_FROM_UNSCHED = 23,
  CHG_ARC_TASK_TO_EQUIV_CLASS = 24,
  CHG_ARC_EQUIV_CLASS_TO_RES = 25,
  CHG_ARC_BETWEEN_EQUIV_CLASS = 26,
  CHG_ARC_BETWEEN_RES = 27,
  CHG_ARC_RUNNING_TASK = 28,
  CHG_ARC_TASK_TO_RES = 29,
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
  void UpdateStats(ChangeType change_type);
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H
