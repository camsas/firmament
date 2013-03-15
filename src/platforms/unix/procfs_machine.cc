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
  vector<cpu_stats> cpu_new_stats = GetCPUStats();
  for (vector<CpuUsage>::size_type cpu_num = 0; cpu_num < cpu_stats_.size();
       cpu_num++) {
    double user_diff =
      (double)(cpu_new_stats[cpu_num].user - cpu_stats_[cpu_num].user);
    double nice_diff =
      (double)(cpu_new_stats[cpu_num].nice - cpu_stats_[cpu_num].nice);
    double system_diff =
      (double)(cpu_new_stats[cpu_num].system - cpu_stats_[cpu_num].system);
    double idle_diff =
      (double)(cpu_new_stats[cpu_num].idle - cpu_stats_[cpu_num].idle);
    double iowait_diff =
      (double)(cpu_new_stats[cpu_num].iowait - cpu_stats_[cpu_num].iowait);
    double irq_diff =
      (double)(cpu_new_stats[cpu_num].irq - cpu_stats_[cpu_num].irq);
    double soft_irq_diff =
      (double)(cpu_new_stats[cpu_num].soft_irq - cpu_stats_[cpu_num].soft_irq);
    double steal_diff =
      (double)(cpu_new_stats[cpu_num].steal - cpu_stats_[cpu_num].steal);
    double guest_diff =
      (double)(cpu_new_stats[cpu_num].guest - cpu_stats_[cpu_num].guest);
    double guest_nice_diff =
      (double)(cpu_new_stats[cpu_num].guest_nice -
               cpu_stats_[cpu_num].guest_nice);
    double total_diff =
      (double)(cpu_new_stats[cpu_num].total - cpu_stats_[cpu_num].total);
    CpuUsage cur_cpu_usage;
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
