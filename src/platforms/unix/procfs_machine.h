/*
 * Firmament
 * Copyright (c) Ionel Gog <ionel.gog@cl.cam.ac.uk>
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
  uint64_t read;
  uint64_t write;
} DiskStatistics_t;

typedef struct {
  uint64_t mem_total;
  uint64_t mem_free;
  uint64_t mem_buffers;
  uint64_t mem_pagecache;
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
  void GetMachineCapacity(ResourceVector* cap);

 private:
  vector<CPUStatistics_t> GetCPUStats();
  vector<CpuUsage> GetCPUUsage();
  DiskStatistics_t GetDiskStats();
  MemoryStatistics_t GetMemoryStats();
  NetworkStatistics_t GetNetworkStats();

  inline void readunsigned(FILE* input, uint64_t *x) {
    CHECK_EQ(fscanf(input, "%ju ", x), 1);
  }

  vector<CPUStatistics_t> cpu_stats_;
  DiskStatistics_t disk_stats_;
  NetworkStatistics_t net_stats_;
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_PROCFS_MACHINE_H
