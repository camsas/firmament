// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef SCHEDULING_DIMACS_CHANGE_STATS_H
#define SCHEDULING_DIMACS_CHANGE_STATS_H

#include <vector>

#include "scheduling/dimacs_change.h"

namespace firmament {

struct DIMACSChangeStats {
 public:
  unsigned int total;
  unsigned int new_node;
  unsigned int remove_node;
  unsigned int new_arc;
  unsigned int change_arc;
  unsigned int remove_arc;

  explicit DIMACSChangeStats(vector<DIMACSChange *> &changes);
  virtual ~DIMACSChangeStats();
};

} // namespace firmament

#endif /* SCHEDULING_DIMACS_CHANGE_STATS_H */
