// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Coordinator knowledege base. This implements data structures and management
// code for the performance and utilization data reported by tasks and other
// coordinators.

#ifndef FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H
#define FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H

#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "base/common.h"
#include "base/types.h"
#include "base/machine_perf_statistics_sample.pb.h"
#include "base/task_perf_statistics_sample.pb.h"
#include "base/task_final_report.pb.h"

namespace firmament {

class KnowledgeBase {
 public:
  KnowledgeBase();
  virtual ~KnowledgeBase();
  void AddMachineSample(const MachinePerfStatisticsSample& sample);
  void AddTaskSample(const TaskPerfStatisticsSample& sample);
  void DumpMachineStats(const ResourceID_t& res_id) const;
  bool GetLatestStatsForMachine(ResourceID_t id,
                                MachinePerfStatisticsSample* sample);
  const deque<MachinePerfStatisticsSample> GetStatsForMachine(
      ResourceID_t id);
  const deque<TaskPerfStatisticsSample>* GetStatsForTask(
      TaskID_t id) const;
  virtual double GetAvgCPIForTEC(EquivClass_t id);
  virtual double GetAvgIPMAForTEC(EquivClass_t id);
  virtual double GetAvgPsPIForTEC(EquivClass_t id);
  virtual double GetAvgRuntimeForTEC(EquivClass_t id);
  const deque<TaskFinalReport>* GetFinalReportForTask(TaskID_t task_id) const;
  const deque<TaskFinalReport>* GetFinalReportsForTEC(EquivClass_t ec_id) const;
  void LoadKnowledgeBaseFromFile();
  void ProcessTaskFinalReport(const vector<EquivClass_t>& equiv_classes,
                              const TaskFinalReport& report);

 protected:
  unordered_map<ResourceID_t, deque<MachinePerfStatisticsSample>,
      boost::hash<boost::uuids::uuid> > machine_map_;
  // TODO(malte): note that below sample queue has no awareness of time within a
  // task, i.e. it mixes samples from all phases
  unordered_map<TaskID_t, deque<TaskPerfStatisticsSample> > task_map_;
  unordered_map<TaskID_t, deque<TaskFinalReport> > task_exec_reports_;
  boost::upgrade_mutex kb_lock_;

 private:
  fstream serial_machine_samples_;
  fstream serial_task_samples_;
  ::google::protobuf::io::ZeroCopyOutputStream* raw_machine_output_;
  ::google::protobuf::io::CodedOutputStream* coded_machine_output_;
  ::google::protobuf::io::ZeroCopyOutputStream* raw_task_output_;
  ::google::protobuf::io::CodedOutputStream* coded_task_output_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H
