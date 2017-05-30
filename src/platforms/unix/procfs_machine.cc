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

// Simple wrapper to poll information from ProcFS.

#include "platforms/unix/procfs_machine.h"

#include <sys/types.h>
#include <sys/sysinfo.h>

#include <cstdio>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/units.h"
#include "misc/string_utils.h"

DEFINE_string(monitor_netif, "eth0",
              "Network interface on which to monitor traffic statistics.");
DEFINE_string(monitor_blockdev, "sda",
              "Block device on which to monitor I/O statistics.");
DEFINE_int64(monitor_blockdev_maxbw, -1,
             "Maximum read/write bandwidth of monitored block device (in KB).");
DEFINE_uint64(monitor_netif_default_speed, 100,
             "Default network interface speed 100Mb/s.");
DECLARE_uint64(heartbeat_interval);

namespace firmament {
namespace platform_unix {

ProcFSMachine::ProcFSMachine() {
  cpu_stats_ = GetCPUStats();
  disk_stats_ = GetDiskStats();
  net_stats_ = GetNetworkStats();
}

const ResourceStats* ProcFSMachine::CreateStatistics(ResourceStats* stats) {
  // CPU stats
  vector<CpuStats> cpus_stats = GetCPUUsage();
  vector<CpuStats>::iterator it;
  for (it = cpus_stats.begin(); it != cpus_stats.end(); it++) {
      CpuStats* cpu_stats = stats->add_cpus_stats();
      cpu_stats->set_cpu_capacity(it->cpu_capacity());
      cpu_stats->set_cpu_utilization(it->cpu_utilization());
  }
  // RAM stats
  MemoryStatistics_t mem_stats = GetMemoryStats();
  stats->set_mem_capacity(mem_stats.mem_total / BYTES_TO_KB);
  stats->set_mem_utilization(
      static_cast<double>(mem_stats.mem_total - mem_stats.mem_free -
                          mem_stats.mem_buffers - mem_stats.mem_pagecache) /
      static_cast<double>(mem_stats.mem_total));
  // Network I/O stats
  NetworkStatistics_t net_stats = GetNetworkStats();
  // We divide by FLAGS_heartbeat_interval / 1000000, since the samples are
  // taken every FLAGS_heartbeat_interval, and they are in microseconds; we
  // want the bandwidth to be in bytes/second.
  stats->set_net_tx_bw((net_stats.send - net_stats_.send) /
                       (static_cast<double>(FLAGS_heartbeat_interval) /
                        static_cast<double>(SECONDS_TO_MICROSECONDS)) /
                       BYTES_TO_KB);
  stats->set_net_rx_bw((net_stats.recv - net_stats_.recv) /
                       (static_cast<double>(FLAGS_heartbeat_interval) /
                        static_cast<double>(SECONDS_TO_MICROSECONDS)) /
                       BYTES_TO_KB);
  net_stats_ = net_stats;
  // Disk I/O stats
  DiskStatistics_t disk_stats = GetDiskStats();
  stats->set_disk_bw(
      (disk_stats.read - disk_stats_.read +
       disk_stats.write - disk_stats_.write) /
      (static_cast<double>(FLAGS_heartbeat_interval) /
       static_cast<double>(SECONDS_TO_MICROSECONDS)) /
      BYTES_TO_KB);
  disk_stats_ = disk_stats;
  return stats;
}

vector<CPUStatistics_t> ProcFSMachine::GetCPUStats() {
  FILE* proc_stat = fopen("/proc/stat", "r");
  vector<CPUStatistics_t> cpus_now;
  CPUStatistics_t cpu_now;
  int proc_stat_cpu;
  CHECK_NOTNULL(proc_stat);

  for (int cpu_num = -1; ; cpu_num++) {
    string read_expr;
    if (cpu_num >= 0) {
      read_expr = "cpu" + to_string(cpu_num) + " %lld %lld %lld %lld %lld " +
           "%lld %lld %lld %lld %lld\n";
    } else {
      read_expr = "cpu %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld\n";
    }
    proc_stat_cpu = fscanf(proc_stat, read_expr.c_str(),
        &cpu_now.user, &cpu_now.nice, &cpu_now.system, &cpu_now.idle,
        &cpu_now.iowait, &cpu_now.irq, &cpu_now.soft_irq, &cpu_now.steal,
        &cpu_now.guest, &cpu_now.guest_nice);
    if (proc_stat_cpu != 10) {
      break;
    }
    cpu_now.total = cpu_now.user + cpu_now.nice + cpu_now.system +
        cpu_now.idle + cpu_now.iowait + cpu_now.irq + cpu_now.soft_irq +
        cpu_now.steal + cpu_now.guest + cpu_now.guest_nice;
    cpu_now.systime = time(NULL);
    cpus_now.push_back(cpu_now);
  }
  CHECK_EQ(fclose(proc_stat), 0);
  return cpus_now;
}

vector<CpuStats> ProcFSMachine::GetCPUUsage() {
  vector<CpuStats> cpu_stats;
  vector<CPUStatistics_t> cpu_new_stats = GetCPUStats();
  for (vector<CpuStats>::size_type cpu_num = 0; cpu_num < cpu_stats_.size();
       cpu_num++) {
    double idle_diff =
      static_cast<double>(cpu_new_stats[cpu_num].idle -
                          cpu_stats_[cpu_num].idle);
    double total_diff =
      static_cast<double>(cpu_new_stats[cpu_num].total -
                          cpu_stats_[cpu_num].total);
    CpuStats cur_cpu_stats;
    if (total_diff == 0)
      total_diff = 1;  // XXX(malte): ugly hack!
    // Capacity is 1000 millicores
    cur_cpu_stats.set_cpu_capacity(1000);
    cur_cpu_stats.set_cpu_utilization(1.0 - idle_diff / total_diff);
    cpu_stats.push_back(cur_cpu_stats);
  }
  cpu_stats_ = cpu_new_stats;
  return cpu_stats;
}

DiskStatistics_t ProcFSMachine::GetDiskStats() {
  // TODO(malte): This implementation is currently limited to monitoring only
  // one block device, specified in FLAGS_monitor_blockdev. We should extend
  // it with support for multiple interfaces, e.g. as determined from
  // /sys/block/<dev> or 'mount'.
  DiskStatistics_t disk_stats;
  bzero(&disk_stats, sizeof(DiskStatistics_t));
  string dev_path;
  spf(&dev_path, "/sys/class/block/%s/",
      FLAGS_monitor_blockdev.c_str());
  FILE* blockdev_stat_fd = fopen((dev_path + "/stat").c_str(), "r");
  if (blockdev_stat_fd) {
    uint64_t tmp_value;
    for (uint64_t i = 0; i < 11; i++) {
      CHECK_EQ(readunsigned(blockdev_stat_fd, &tmp_value), 0);
      if (i == 2)
        // read sector count
        disk_stats.read = tmp_value * 512;
      if (i == 6)
        // write sector count
        disk_stats.write = tmp_value * 512;
    }
    CHECK_EQ(fclose(blockdev_stat_fd), 0);
  }
  return disk_stats;
}

void ProcFSMachine::GetMachineCapacity(ResourceVector* cap) {
  // Extract the total available resource capacities on this machine
  MemoryStatistics_t mem_stats = GetMemoryStats();
  cap->set_ram_cap(mem_stats.mem_total / BYTES_TO_KB);
  vector<CPUStatistics_t> cpu_stats = GetCPUStats();
  // Subtract one as we have an additional element for the overall CPU load
  // across all cores
  cap->set_cpu_cores(cpu_stats.size() - 1);
  // Get network interface speed from ProcFS
  string nic_speed_path;
  spf(&nic_speed_path, "/sys/class/net/%s/speed",
      FLAGS_monitor_netif.c_str());
  FILE* nic_speed_fd = fopen(nic_speed_path.c_str(), "r");
  uint64_t speed = 0;
  if (nic_speed_fd) {
    int valid_nic_speed = readunsigned(nic_speed_fd, &speed);
    if (valid_nic_speed != 0) {
       // TODO(shiv): How to determine a common default values? 100Mb/s is assumed for now.
       speed = FLAGS_monitor_netif_default_speed;
    }
    CHECK_EQ(fclose(nic_speed_fd), 0);
  }
  if (speed == 0)
    LOG(WARNING) << "Failed to determinate network interface speed for "
                 << FLAGS_monitor_netif;
  cap->set_net_tx_bw(speed / 8 / BYTES_TO_KB);
  cap->set_net_rx_bw(speed / 8 / BYTES_TO_KB);
  // Get disk read/write speed
  if (FLAGS_monitor_blockdev_maxbw == -1) {
    // XXX(malte): we use a hack here -- if the disk is not rotational, we
    // assume it's an SSD and return a generic SSD-level throughput limit.
    string disk_type_path;
    spf(&disk_type_path, "/sys/class/block/%s/queue/rotational",
        FLAGS_monitor_blockdev.c_str());
    FILE* disk_type_fd = fopen(disk_type_path.c_str(), "r");
    uint64_t disk_is_rotational = 1;  // HDD is the default
    if (disk_type_fd) {
      CHECK_EQ(readunsigned(disk_type_fd, &disk_is_rotational), 0);
      CHECK_EQ(fclose(disk_type_fd), 0);
    }
    if (disk_is_rotational) {
      // Legacy HDD, so return low bandwidth
      cap->set_disk_bw(51200);  // 50 MB/s, a medium estimate
    } else {
      // SSD, so go faster
      cap->set_disk_bw(307200); // 300 MB/s, a medium estimate
    }
  } else {
    cap->set_disk_bw(FLAGS_monitor_blockdev_maxbw);
  }
}

MemoryStatistics_t ProcFSMachine::GetMemoryStats() {
  MemoryStatistics_t mem_stats;
  FILE* mem_stat_fd = fopen("/proc/meminfo", "r");
  CHECK_NOTNULL(mem_stat_fd);
  while (!feof(mem_stat_fd)) {
    char label[100];
    uint64_t val = 0;
    // Ignore invalid lines
    if (fscanf(mem_stat_fd, "%s", label) <= 0)
      continue;
    if (fscanf(mem_stat_fd, "%ju", &val) <= 0)
      continue;
    if (strncmp(label, "MemTotal:", 100) == 0) {
      mem_stats.mem_total = val * 1024;
    } else if (strncmp(label, "MemFree:", 100) == 0) {
      mem_stats.mem_free = val * 1024;
    } else if (strncmp(label, "Buffers:", 100) == 0) {
      mem_stats.mem_buffers = val * 1024;
    } else if (strncmp(label, "Cached:", 100) == 0) {
      mem_stats.mem_pagecache = val * 1024;
    }
  }
  CHECK_EQ(fclose(mem_stat_fd), 0);
  return mem_stats;
}

NetworkStatistics_t ProcFSMachine::GetNetworkStats() {
  // TODO(malte): This implementation is currently limited to monitoring only
  // one network interface, specified in FLAGS_monitor_netif. We should extend
  // it with support for multiple interfaces, e.g. as determined from
  // /proc/net/dev.
  NetworkStatistics_t net_stats;
  bzero(&net_stats, sizeof(NetworkStatistics_t));
  string interface_path;
  spf(&interface_path, "/sys/class/net/%s/statistics/",
      FLAGS_monitor_netif.c_str());

  // Send
  FILE* tx_stat_fd = fopen((interface_path + "/tx_bytes").c_str(), "r");
  if (tx_stat_fd) {
    CHECK_EQ(readunsigned(tx_stat_fd, &net_stats.send), 0);
    CHECK_EQ(fclose(tx_stat_fd), 0);
  }
  // Recv
  FILE* rx_stat_fd = fopen((interface_path + "/rx_bytes").c_str(), "r");
  if (rx_stat_fd) {
    CHECK_EQ(readunsigned(rx_stat_fd, &net_stats.recv), 0);
    CHECK_EQ(fclose(rx_stat_fd), 0);
  }
  return net_stats;
}

}  // namespace platform_unix
}  // namespace firmament
