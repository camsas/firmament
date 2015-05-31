// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#include "scheduling/flow/dimacs_change_stats.h"

#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"

namespace firmament {

DIMACSChangeStats::DIMACSChangeStats(const vector<DIMACSChange*>& changes) {
  total_ = changes.size();
  nodes_added_ = 0;
  nodes_removed_ = 0;
  arcs_added_ = 0;
  arcs_changed_ = 0;
  arcs_removed_ = 0;
  for (DIMACSChange *chg : changes) {
    nodes_added_ += chg->stats().nodes_added_;
    nodes_removed_ += chg->stats().nodes_removed_;
    arcs_added_ += chg->stats().arcs_added_;
    arcs_changed_ += chg->stats().arcs_changed_;
    arcs_removed_ += chg->stats().arcs_removed_;
  }
}

DIMACSChangeStats::~DIMACSChangeStats() { }

} /* namespace firmament */
