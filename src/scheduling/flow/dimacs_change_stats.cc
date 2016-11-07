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
  for (uint32_t chg_index = 0; chg_index < NUM_CHANGE_TYPES; ++chg_index) {
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
  for (uint32_t index = 0; index < NUM_CHANGE_TYPES; index++) {
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
  for (uint32_t index = 0; index < NUM_CHANGE_TYPES; index++) {
    num_changes_of_type_[index] = 0;
  }
}

void DIMACSChangeStats::UpdateStats(DIMACSChangeType change_type) {
  num_changes_of_type_[change_type]++;
  if (change_type == ADD_TASK_NODE || change_type == ADD_RESOURCE_NODE ||
      change_type == ADD_EQUIV_CLASS_NODE ||
      change_type == ADD_UNSCHED_JOB_NODE ||
      change_type == ADD_SINK_NODE) {
    nodes_added_++;
    return;
  }
  if (change_type == ADD_ARC_TASK_TO_EQUIV_CLASS ||
      change_type == ADD_ARC_TASK_TO_RES ||
      change_type == ADD_ARC_EQUIV_CLASS_TO_RES ||
      change_type == ADD_ARC_BETWEEN_EQUIV_CLASS ||
      change_type == ADD_ARC_BETWEEN_RES || change_type == ADD_ARC_TO_UNSCHED ||
      change_type == ADD_ARC_FROM_UNSCHED ||
      change_type == ADD_ARC_RUNNING_TASK ||
      change_type == ADD_ARC_RES_TO_SINK) {
    arcs_added_++;
    return;
  }
  if (change_type == DEL_UNSCHED_JOB_NODE || change_type == DEL_TASK_NODE ||
      change_type == DEL_RESOURCE_NODE || change_type == DEL_EQUIV_CLASS_NODE) {
    nodes_removed_++;
    return;
  }
  if (change_type == DEL_ARC_EQUIV_CLASS_TO_RES ||
      change_type == DEL_ARC_RUNNING_TASK ||
      change_type == DEL_ARC_EVICTED_TASK ||
      change_type == DEL_ARC_BETWEEN_EQUIV_CLASS ||
      change_type == DEL_ARC_BETWEEN_RES ||
      change_type == DEL_ARC_TASK_TO_EQUIV_CLASS ||
      change_type == DEL_ARC_TASK_TO_RES ||
      change_type == DEL_ARC_RES_TO_SINK) {
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
      change_type == CHG_ARC_TASK_TO_RES ||
      change_type == CHG_ARC_RUNNING_TASK ||
      change_type == CHG_ARC_RES_TO_SINK) {
    arcs_changed_++;
    return;
  }
  LOG(FATAL) << "Unknown DIMACS change type: " << change_type;
}

} // namespace firmament
