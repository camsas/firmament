// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/dimacs_change_stats.h"

#include <boost/lexical_cast.hpp>
#include <string>

namespace firmament {

DIMACSChangeStats::DIMACSChangeStats() {
  nodes_added_ = 0;
  nodes_removed_ = 0;
  arcs_added_ = 0;
  arcs_changed_ = 0;
  arcs_removed_ = 0;
  for (uint32_t chg_index = 0; chg_index <= NUM_CHANGE_TYPES; ++chg_index) {
    num_changes_of_type_[chg_index] = 0;
  }
}

DIMACSChangeStats::~DIMACSChangeStats() {
}

string DIMACSChangeStats::GetStatsString() const {
  string stats = boost::lexical_cast<string>(nodes_added_) + "," +
    boost::lexical_cast<string>(nodes_removed_) + "," +
    boost::lexical_cast<string>(arcs_added_) + "," +
    boost::lexical_cast<string>(arcs_changed_) + "," +
    boost::lexical_cast<string>(arcs_removed_);
  for (uint32_t index = 0; index <= NUM_CHANGE_TYPES; index++) {
    stats += "," + boost::lexical_cast<string>(num_changes_of_type_[index]);
  }
  return stats;
}

void DIMACSChangeStats::ResetStats() {
  nodes_added_ = 0;
  nodes_removed_ = 0;
  arcs_added_ = 0;
  arcs_changed_ = 0;
  arcs_removed_ = 0;
  for (uint32_t index = 0; index <= NUM_CHANGE_TYPES; index++) {
    num_changes_of_type_[index] = 0;
  }
}

void DIMACSChangeStats::UpdateStats(ChangeType change_type) {
  num_changes_of_type_[change_type]++;
  if (change_type == ADD_TASK_NODE || change_type == ADD_RESOURCE_NODE ||
      change_type == ADD_EQUIV_CLASS_NODE ||
      change_type == ADD_UNSCHED_JOB_NODE ||
      change_type == ADD_SINK_NODE) {
    nodes_added_++;
    return;
  }
  if (change_type == ADD_ARC_TASK_TO_EQUIV_CLASS ||
      change_type == ARC_ARC_TASK_TO_RES ||
      change_type == ADD_ARC_EQUIV_CLASS_TO_RES ||
      change_type == ADD_ARC_BETWEEN_EQUIV_CLASS ||
      change_type == ADD_ARC_BETWEEN_RES || change_type == ADD_ARC_TO_UNSCHED ||
      change_type == ADD_ARC_FROM_UNSCHED || change_type == ADD_ARC_PIN_TASK) {
    arcs_added_++;
    return;
  }
  if (change_type == DEL_UNSCHED_JOB_NODE || change_type == DEL_TASK_NODE ||
      change_type == DEL_RESOURCE_NODE || change_type == DEL_EQUIV_CLASS_NODE) {
    nodes_removed_++;
    return;
  }
  if (change_type == DEL_ARC_EQUIV_CLASS_TO_RES ||
      change_type == DEL_ARC_PIN_TASK || change_type == DEL_ARC_EVICTED_TASK) {
    arcs_removed_++;
    return;
  }
  if (change_type == CHG_ARC_EVICTED_TASK ||
      change_type == CHG_ARC_TO_UNSCHED ||
      change_type == CHG_ARC_FROM_UNSCHED ||
      change_type == CHG_ARC_TASK_TO_EQUIV_CLASS ||
      change_type == CHG_ARC_EQUIV_CLASS_TO_RES ||
      change_type == CHG_ARC_BETWEEN_EQUIV_CLASS ||
      change_type == CHG_ARC_BETWEEN_RES ||
      change_type == CHG_ARC_PIN_TASK) {
    arcs_changed_++;
    return;
  }
  LOG(FATAL) << "Unknown DIMACS change type: " << change_type;
}

} // namespace firmament
