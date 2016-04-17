// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of the simulator's knowledge base.
#include "sim/knowledge_base_simulator.h"

#include <boost/lexical_cast.hpp>

#include <string>
#include <vector>

#include "base/common.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"

#define SIMULATED_CPU_FREQUENCY 2200000000 // 2.2 Ghz

using boost::lexical_cast;

DEFINE_double(rabbit_cpi_threshold, 0.9, "CPI threshold for RABBIT");
DEFINE_double(rabbit_mai_threshold, 0.001, "MAI threshold for RABBIT");
DEFINE_double(devil_mai_threshold, 0.005, "MAI threshold for DEVIL");
DEFINE_double(devil_page_cache_threshold, 0.05,
              "Total page cache threshold for DEVIL");
DEFINE_double(sheep_cpi_threshold, 1.6, "CPI threshold for SHEEP");
DEFINE_double(sheep_mai_threshold, 0.001, "MAI thereshold for SHEEP");

namespace firmament {
namespace sim {

KnowledgeBaseSimulator::KnowledgeBaseSimulator() : KnowledgeBase(NULL) {
}

KnowledgeBaseSimulator::KnowledgeBaseSimulator(
    DataLayerManagerInterface* data_layer_manager)
      : KnowledgeBase(data_layer_manager) {
}

void KnowledgeBaseSimulator::AddMachineSample(
    uint64_t current_simulation_time,
    ResourceDescriptor* rd_ptr,
    const unordered_map<TaskID_t, ResourceDescriptor*>& task_id_to_rd) {
  MachinePerfStatisticsSample machine_stats;
  machine_stats.set_resource_id(rd_ptr->uuid());
  machine_stats.set_timestamp(current_simulation_time);
  uint64_t mem_usage = 0;
  uint64_t num_cores =
    lexical_cast<uint64_t>(rd_ptr->resource_capacity().cpu_cores());
  vector<double> cpus_usage(num_cores, 1.0);
  for (auto& task_id_rd : task_id_to_rd) {
    TraceTaskStats* task_stat = FindOrNull(task_stats_, task_id_rd.first);
    if (!task_stat) {
      // We don't have any stats for the task. Ignore it.
      continue;
    }
    mem_usage += task_stat->avg_canonical_mem_usage_ +
      task_stat->avg_unmapped_page_cache_ -
      task_stat->avg_total_page_cache_;
    string label = task_id_rd.second->friendly_name();
    uint64_t idx = label.find("PU #");
    CHECK_NE(idx, string::npos)
      << "PU label does not contain core id for resource: "
      << task_id_rd.second->uuid();
    string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
    int64_t core_id = strtoll(core_id_substr.c_str(), 0, 10);
    // TODO(ionel): In the Google trace a task might require more than one
    // core. Change the code to handle this case as well.
    // TODO(ionel): This assumes that all the machines in the trace are the
    // same. The reported cpu_usage is relative to the machine type. Fix!
    CHECK_LT(core_id, num_cores);
    cpus_usage[core_id] -= task_stat->avg_mean_cpu_usage_;
  }
  // RAM stats
  machine_stats.set_total_ram(rd_ptr->resource_capacity().ram_cap() *
                              MB_TO_BYTES);
  machine_stats.set_free_ram(
      (rd_ptr->resource_capacity().ram_cap() - mem_usage) * MB_TO_BYTES);
  // CPU stats
  for (auto& usage : cpus_usage) {
    CpuUsage* cpu_usage = machine_stats.add_cpus_usage();
    // Transform to percentage.
    cpu_usage->set_idle(usage * 100.0);
    // We don't have information to fill in the other fields.
  }
  // Disk stats
  // The trace doesn't have information about disk bandwidth.
  machine_stats.set_disk_bw(0);
  // Network stats
  // The trace doesn't have any information about network utilization.
  machine_stats.set_net_bw(0);
  KnowledgeBase::AddMachineSample(machine_stats);
}

void KnowledgeBaseSimulator::EraseTraceTaskStats(TaskID_t task_id) {
  task_stats_.erase(task_id);
}

void KnowledgeBaseSimulator::PopulateTaskFinalReport(TaskDescriptor* td_ptr,
                                                     TaskFinalReport* report) {
  TraceTaskStats* task_stats = FindOrNull(task_stats_, td_ptr->uid());
  if (task_stats && task_stats->avg_cpi_ > COMPARE_EPS) {
    double instructions = (td_ptr->finish_time() - td_ptr->start_time()) /
      task_stats->avg_cpi_ * SIMULATED_CPU_FREQUENCY;
    report->set_instructions(static_cast<uint64_t>(instructions));
    report->set_cycles(
        static_cast<uint64_t>(instructions * task_stats->avg_cpi_));
  } else {
    // We don't have any stats for the task.
    // XXX(ionel): We assume a CPI of 1. Maybe set to the avg_cpi of the
    // entire trace.
    double cpi = 1.0;
    double instructions = (td_ptr->finish_time() - td_ptr->start_time()) *
      SIMULATED_CPU_FREQUENCY / cpi;
    report->set_instructions(static_cast<uint64_t>(instructions));
    report->set_cycles(static_cast<uint64_t>(instructions * cpi));
  }
}

void KnowledgeBaseSimulator::SetTaskType(TaskDescriptor* td_ptr) {
  // The classification works as follows:
  // low CPI, low MAI (lots of compute, but little memory access) => rabbit
  // high MAI, large page cache (lots of memory access, data-intensive) => devil
  // high CPI, high MAI (slow-ish compute, but lots of memory traffic) => sheep
  // else => turtle
  TraceTaskStats* task_stats = FindOrNull(task_stats_, td_ptr->uid());
  if (task_stats) {
    if (task_stats->avg_cpi_ < FLAGS_rabbit_cpi_threshold &&
        task_stats->avg_mai_ < FLAGS_rabbit_mai_threshold) {
      td_ptr->set_task_type(TaskDescriptor::RABBIT);
      return;
    }
    if (task_stats->avg_mai_ > FLAGS_devil_mai_threshold &&
        task_stats->avg_total_page_cache_ > FLAGS_devil_page_cache_threshold) {
      td_ptr->set_task_type(TaskDescriptor::DEVIL);
      return;
    }
    if (task_stats->avg_cpi_ > FLAGS_sheep_cpi_threshold &&
        task_stats->avg_mai_ > FLAGS_sheep_mai_threshold) {
      td_ptr->set_task_type(TaskDescriptor::SHEEP);
      return;
    }
  }
  // We don't have any stats for the task. We assume it's a turtle so
  // that it can be placed anywhere.
  td_ptr->set_task_type(TaskDescriptor::TURTLE);
}

void KnowledgeBaseSimulator::SetTraceTaskStats(
    TaskID_t task_id,
    const TraceTaskStats& task_stats) {
  InsertIfNotPresent(&task_stats_, task_id, task_stats);
}

} // namespace sim
} // namespace firmament
