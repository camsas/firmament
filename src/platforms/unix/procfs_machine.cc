// The Firmament project
// Copyright (c) 2011-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simple wrapper to poll information from ProcFS.

#include "platforms/unix/procfs_machine.h"

#include <sys/types.h>
#include <sys/sysinfo.h>

#include <cstdio>
#include <string>
#include <vector>

#include "base/common.h"
#include "misc/string_utils.h"

DEFINE_string(monitor_netif, "eth0",
              "Network interface on which to monitor traffic statistics.");
DEFINE_string(monitor_blockdev, "sda",
              "Block device on which to monitor I/O statistics.");
DECLARE_uint64(heartbeat_interval);

namespace firmament {
namespace platform_unix {

ProcFSMachine::ProcFSMachine() {
  cpu_stats_ = GetCPUStats();
  disk_stats_ = GetDisk();
  net_stats_ = GetNetwork();
}

const MachinePerfStatisticsSample* ProcFSMachine::CreateStatistics(
    MachinePerfStatisticsSample* stats) {
  // CPU stats
  vector<CpuUsage> cpus_usage = GetCPUUsage();
  vector<CpuUsage>::iterator it;
  for (it = cpus_usage.begin(); it != cpus_usage.end(); it++) {
      CpuUsage* cpu_usage = stats->add_cpus_usage();
      cpu_usage->set_user(it->user());
      cpu_usage->set_nice(it->nice());
      cpu_usage->set_system(it->system());
      cpu_usage->set_idle(it->idle());
      cpu_usage->set_iowait(it->iowait());
      cpu_usage->set_irq(it->irq());
      cpu_usage->set_soft_irq(it->soft_irq());
      cpu_usage->set_steal(it->steal());
      cpu_usage->set_guest(it->guest());
      cpu_usage->set_guest_nice(it->guest_nice());
  }
  // RAM stats
  MemoryStatistics_t mem_stats = GetMemory();
  stats->set_total_ram(mem_stats.mem_total);
  stats->set_free_ram(mem_stats.mem_free);
  // Network I/O stats
  NetworkStatistics_t net_stats = GetNetwork();
  // We divide by FLAGS_heartbeat_interval / 1000000, since the samples are
  // taken every FLAGS_heartbeat_interval, and they are in microseconds; we
  // want the bandwidth to be in bytes/second.
  stats->set_net_bw(
      ((net_stats.send - net_stats_.send) + (net_stats.recv - net_stats_.recv))
      / (FLAGS_heartbeat_interval / 1000000));
  net_stats_ = net_stats;
  // Disk I/O stats
  DiskStatistics_t disk_stats = GetDisk();
  stats->set_disk_bw((disk_stats.read - disk_stats_.read) +
                     (disk_stats.write - disk_stats_.write) /
                     (FLAGS_heartbeat_interval / 1000000));
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
  fclose(proc_stat);
  return cpus_now;
}

vector<CpuUsage> ProcFSMachine::GetCPUUsage() {
  vector<CpuUsage> cpu_usage;
  vector<CPUStatistics_t> cpu_new_stats = GetCPUStats();
  for (vector<CpuUsage>::size_type cpu_num = 0; cpu_num < cpu_stats_.size();
       cpu_num++) {
    double user_diff =
      static_cast<double>(cpu_new_stats[cpu_num].user -
                          cpu_stats_[cpu_num].user);
    double nice_diff =
      static_cast<double>(cpu_new_stats[cpu_num].nice -
                          cpu_stats_[cpu_num].nice);
    double system_diff =
      static_cast<double>(cpu_new_stats[cpu_num].system -
                          cpu_stats_[cpu_num].system);
    double idle_diff =
      static_cast<double>(cpu_new_stats[cpu_num].idle -
                          cpu_stats_[cpu_num].idle);
    double iowait_diff =
      static_cast<double>(cpu_new_stats[cpu_num].iowait -
                          cpu_stats_[cpu_num].iowait);
    double irq_diff =
      static_cast<double>(cpu_new_stats[cpu_num].irq -
                          cpu_stats_[cpu_num].irq);
    double soft_irq_diff =
      static_cast<double>(cpu_new_stats[cpu_num].soft_irq -
                          cpu_stats_[cpu_num].soft_irq);
    double steal_diff =
      static_cast<double>(cpu_new_stats[cpu_num].steal -
                          cpu_stats_[cpu_num].steal);
    double guest_diff =
      static_cast<double>(cpu_new_stats[cpu_num].guest -
                          cpu_stats_[cpu_num].guest);
    double guest_nice_diff =
      static_cast<double>(cpu_new_stats[cpu_num].guest_nice -
               cpu_stats_[cpu_num].guest_nice);
    double total_diff =
      static_cast<double>(cpu_new_stats[cpu_num].total -
                          cpu_stats_[cpu_num].total);
    CpuUsage cur_cpu_usage;
    if (total_diff == 0)
      total_diff = 1;  // XXX(malte): ugly hack!
    cur_cpu_usage.set_user(user_diff / total_diff * 100.0);
    cur_cpu_usage.set_nice(nice_diff / total_diff * 100.0);
    cur_cpu_usage.set_system(system_diff / total_diff * 100.0);
    cur_cpu_usage.set_idle(idle_diff / total_diff * 100.0);
    cur_cpu_usage.set_iowait(iowait_diff / total_diff * 100.0);
    cur_cpu_usage.set_irq(irq_diff / total_diff * 100.0);
    cur_cpu_usage.set_soft_irq(soft_irq_diff / total_diff * 100.0);
    cur_cpu_usage.set_steal(steal_diff / total_diff * 100.0);
    cur_cpu_usage.set_guest(guest_diff / total_diff * 100.0);
    cur_cpu_usage.set_guest_nice(guest_nice_diff / total_diff * 100.0);
    cpu_usage.push_back(cur_cpu_usage);
  }
  cpu_stats_ = cpu_new_stats;
  return cpu_usage;
}

DiskStatistics_t ProcFSMachine::GetDisk() {
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
      readunsigned(blockdev_stat_fd, &tmp_value);
      if (i == 2)
        // read sector count
        disk_stats.read = tmp_value * 512;
      if (i == 6)
        // write sector count
        disk_stats.write = tmp_value * 512;
    }
    fclose(blockdev_stat_fd);
  }
  return disk_stats;
}

MemoryStatistics_t ProcFSMachine::GetMemory() {
  MemoryStatistics_t mem_stats;
  struct sysinfo mem_info;
  sysinfo(&mem_info);
  mem_stats.mem_total = mem_info.totalram * mem_info.mem_unit;
  mem_stats.mem_free = mem_info.freeram * mem_info.mem_unit +
                       mem_info.bufferram * mem_info.mem_unit;
  return mem_stats;
}

NetworkStatistics_t ProcFSMachine::GetNetwork() {
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
    readunsigned(tx_stat_fd, &net_stats.send);
    fclose(tx_stat_fd);
  }
  // Recv
  FILE* rx_stat_fd = fopen((interface_path + "/rx_bytes").c_str(), "r");
  if (rx_stat_fd) {
    readunsigned(rx_stat_fd, &net_stats.recv);
    fclose(rx_stat_fd);
  }
  return net_stats;
}

}  // namespace platform_unix
}  // namespace firmament
