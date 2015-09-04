// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of the simulator's knowledge base.
#include "sim/trace-extract/knowledge_base_simulator.h"

#include <boost/lexical_cast.hpp>

#include <string>
#include <vector>

#include "base/common.h"
#include "misc/map-util.h"

using boost::lexical_cast;

namespace firmament {
namespace sim {

KnowledgeBaseSimulator::KnowledgeBaseSimulator() {
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
    mem_usage += task_stat->avg_canonical_mem_usage +
      task_stat->avg_unmapped_page_cache -
      task_stat->avg_total_page_cache;
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
    cpus_usage[core_id] -= task_stat->avg_mean_cpu_usage;
  }
  // RAM stats
  machine_stats.set_total_ram(rd_ptr->resource_capacity().ram_cap());
  machine_stats.set_free_ram(rd_ptr->resource_capacity().ram_cap() - mem_usage);
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

void KnowledgeBaseSimulator::SetTraceTaskStats(
    TaskID_t task_id,
    const TraceTaskStats& task_stats) {
  InsertIfNotPresent(&task_stats_, task_id, task_stats);
}

} // namespace sim
} // namespace firmament
