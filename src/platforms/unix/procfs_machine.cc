// The Firmament project
// Copyright (c) 2011-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simple wrapper to poll information from ProcFS.

#include "platforms/unix/procfs_machine.h"
#include "sys/types.h"
#include "sys/sysinfo.h"

#include <stdio.h>

#include <vector>

using namespace std;

namespace firmament {
namespace platform_unix {

ProcFSMachine::ProcFSMachine() {
  cpu_stats_ = GetCPUStats();
}

const MachinePerfStatisticsSample* ProcFSMachine::CreateStatistics(
    MachinePerfStatisticsSample* stats) {
  vector<double> cpus_usage = GetCPUUsage();
  vector<double>::iterator v_it;
  for (v_it = cpus_usage.begin(); v_it != cpus_usage.end(); v_it++) {
    stats->add_cpus_usage(*v_it);
  }
  mem_stats mem_stats = GetMemory();
  stats->set_total_ram(mem_stats.mem_total);
  stats->set_free_ram(mem_stats.mem_free);
  return stats;
}

vector<cpu_stats> ProcFSMachine::GetCPUStats() {
  FILE* proc_stat = fopen("/proc/stat", "r");
  vector<cpu_stats> cpus_now;
  cpu_stats cpu_now;
  int proc_stat_cpu;
  CHECK_NOTNULL(proc_stat);

  for (int cpu_num = 0; ; cpu_num++) {
    proc_stat_cpu = fscanf(proc_stat,
        "cpu %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld",
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

vector<double> ProcFSMachine::GetCPUUsage() {
  vector<double> cpu_usage;
  vector<cpu_stats> cpu_new_stats = GetCPUStats();
  for (vector<double>::size_type cpu_num = 0; cpu_num < cpu_stats_.size();
       cpu_num++) {
    // NOTE: We just use idle, but the other information is available here.
    double idle_diff =
      (double)(cpu_new_stats[cpu_num].idle - cpu_stats_[cpu_num].idle);
    double total_diff =
      (double)(cpu_new_stats[cpu_num].total - cpu_stats_[cpu_num].total);
    cpu_usage.push_back(100.0 - idle_diff / total_diff * 100.0);
  }
  cpu_stats_ = cpu_new_stats;
  return cpu_usage;
}

mem_stats ProcFSMachine::GetMemory() {
  mem_stats mem_stats;
  struct sysinfo mem_info;
  sysinfo(&mem_info);
  mem_stats.mem_total = mem_info.totalram * mem_info.mem_unit;
  mem_stats.mem_free = mem_info.freeram * mem_info.mem_unit;
  return mem_stats;
}

}  // namespace platform_unix
}  // namespace firmament
