// The Firmament project
// Copyright (c) 2011-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simple wrapper to poll information from ProcFS.

#ifndef FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H
#define FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H

#include <vector>

#include "base/machine_perf_statistics_sample.pb.h"
#include "platforms/unix/common.h"

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
} CPUStatistics_t;

typedef struct {
  uint64_t mem_total;
  uint64_t mem_free;
} MemoryStatistics_t;

typedef struct {
  uint64_t send;
  uint64_t recv;
} NetworkStatistics_t;

class ProcFSMachine {
 public:
  ProcFSMachine();
  const MachinePerfStatisticsSample* CreateStatistics(
      MachinePerfStatisticsSample* stats);

 private:
  vector<CPUStatistics_t> GetCPUStats();
  vector<CpuUsage> GetCPUUsage();
  MemoryStatistics_t GetMemory();
  NetworkStatistics_t GetNetwork();

  inline void readunsigned(FILE* input, uint64_t *x) {
    fscanf(input, "%ju ", x);
  }

  vector<CPUStatistics_t> cpu_stats_;
  NetworkStatistics_t net_stats_;
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H
