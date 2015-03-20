// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Coordinator knowledege base. This implements data structures and management
// code for the performance and utilization data reported by tasks and other
// coordinators.

#ifndef FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H
#define FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H

#include <deque>
#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/machine_perf_statistics_sample.pb.h"
#include "base/task_perf_statistics_sample.pb.h"
#include "base/task_final_report.pb.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

// Limit stats queues to 1MB each
#define MAX_SAMPLE_QUEUE_CAPACITY 1024*1024

class KnowledgeBase {
 public:
  KnowledgeBase();
  void AddMachineSample(const MachinePerfStatisticsSample& sample);
  void AddTaskSample(const TaskPerfStatisticsSample& sample);
  void DumpMachineStats(const ResourceID_t& res_id) const;
  const deque<MachinePerfStatisticsSample>* GetStatsForMachine(
      ResourceID_t id) const;
  const deque<TaskPerfStatisticsSample>* GetStatsForTask(
      TaskID_t id) const;
  double GetAvgCPIForTEC(TaskEquivClass_t id);
  double GetAvgIPMAForTEC(TaskEquivClass_t id);
  double GetAvgRuntimeForTEC(TaskEquivClass_t id);
  const deque<TaskFinalReport>* GetFinalStatsForTask(TaskID_t task_id) const;
  vector<TaskEquivClass_t>* GetTaskEquivClasses(TaskID_t task_id) const;
  void ProcessTaskFinalReport(const TaskFinalReport& report,
                              TaskID_t task_id);
  void SetCostModel(FlowSchedulingCostModelInterface* cost_model);

 protected:
  map<ResourceID_t, deque<MachinePerfStatisticsSample> > machine_map_;
  // TODO(malte): note that below sample queue has no awareness of time within a
  // task, i.e. it mixes samples from all phases
  unordered_map<TaskID_t, deque<TaskPerfStatisticsSample> > task_map_;
  unordered_map<TaskID_t, deque<TaskFinalReport> > task_exec_reports_;
  boost::mutex kb_lock_;
  FlowSchedulingCostModelInterface* cost_model_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H
