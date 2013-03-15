// The Firmament project
// Copyright (c) 2011-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simple wrapper to poll information from ProcFS.

#ifndef FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H
#define FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H

#include "platforms/unix/common.h"

#include "base/machine_perf_statistics_sample.pb.h"

namespace firmament {
namespace platform_unix {

typedef struct {
  uint64_t user;
  uint64_t nice;
  uint64_t system;
  uint64_t idle;
  uint64_t iowait;
  uint64_t irq;
  uint64_t soft_irq;
  uint64_t steal;
  uint64_t guest;
  uint64_t guest_nice;
  uint64_t total;
  time_t systime;
} cpu_stats;

typedef struct {
  uint64_t mem_total;
  uint64_t mem_free;
} mem_stats;

class ProcFSMachine {
 public:
  explicit ProcFSMachine();
  const MachinePerfStatisticsSample* CreateStatistics(
      MachinePerfStatisticsSample* stats);

 private:
  vector<cpu_stats> cpu_stats_;
  mem_stats GetMemory();
  vector<cpu_stats> GetCPUStats();
  vector<double> GetCPUUsage();
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H
