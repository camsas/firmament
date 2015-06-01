// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H

#include <vector>

#include "base/types.h"

namespace firmament {

// Forward declaration, since DIMACSChange contains a DIMACSChangeStats
// instance itself
class DIMACSChange;

struct DIMACSChangeStats {
  unsigned int total_;
  unsigned int nodes_added_;
  unsigned int nodes_removed_;
  unsigned int arcs_added_;
  unsigned int arcs_changed_;
  unsigned int arcs_removed_;

  DIMACSChangeStats() : total_(0), nodes_added_(0), nodes_removed_(0),
      arcs_added_(0), arcs_changed_(0), arcs_removed_(0)
  {}
  explicit DIMACSChangeStats(const vector<DIMACSChange*>& changes);
  virtual ~DIMACSChangeStats();
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_STATS_H
