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
#include "base/resource_stats.pb.h"
#include "base/task_final_report.pb.h"
#include "base/task_stats.pb.h"
#include "scheduling/data_layer_manager_interface.h"

namespace firmament {

class KnowledgeBase {
 public:
  KnowledgeBase();
  KnowledgeBase(DataLayerManagerInterface* data_layer_manager);
  virtual ~KnowledgeBase();
  void AddMachineSample(const ResourceStats& sample);
  void AddTaskStatsSample(const TaskStats& stats_sample);
  void DumpMachineStats(const ResourceID_t& res_id) const;
  bool GetLatestStatsForMachine(ResourceID_t id, ResourceStats* sample);
  const deque<ResourceStats> GetStatsForMachine(ResourceID_t id);
  const deque<TaskStats>* GetStatsForTask(TaskID_t id) const;
  virtual double GetAvgCPIForTEC(EquivClass_t id);
  virtual double GetAvgIPMAForTEC(EquivClass_t id);
  virtual double GetAvgPsPIForTEC(EquivClass_t id);
  virtual double GetAvgRuntimeForTEC(EquivClass_t id);
  const deque<TaskFinalReport>* GetFinalReportForTask(TaskID_t task_id) const;
  const deque<TaskFinalReport>* GetFinalReportsForTEC(EquivClass_t ec_id) const;
  virtual uint64_t GetRuntimeForTask(TaskID_t task_id);
  void LoadKnowledgeBaseFromFile();
  void ProcessTaskFinalReport(const vector<EquivClass_t>& equiv_classes,
                              const TaskFinalReport& report);
  inline const DataLayerManagerInterface& data_layer_manager() {
    CHECK_NOTNULL(data_layer_manager_);
    return *data_layer_manager_;
  }
  inline DataLayerManagerInterface* mutable_data_layer_manager() {
    CHECK_NOTNULL(data_layer_manager_);
    return data_layer_manager_;
  }

 protected:
  unordered_map<ResourceID_t, deque<ResourceStats>,
      boost::hash<boost::uuids::uuid> > machine_map_;
  // TODO(malte): note that below sample queue has no awareness of time within a
  // task, i.e. it mixes samples from all phases
  unordered_map<TaskID_t, deque<TaskStats> > task_map_;
  unordered_map<TaskID_t, deque<TaskFinalReport> > task_exec_reports_;
  boost::upgrade_mutex kb_lock_;

 private:
  fstream serial_machine_samples_;
  fstream serial_task_samples_;
  ::google::protobuf::io::ZeroCopyOutputStream* raw_machine_output_;
  ::google::protobuf::io::CodedOutputStream* coded_machine_output_;
  ::google::protobuf::io::ZeroCopyOutputStream* raw_task_output_;
  ::google::protobuf::io::CodedOutputStream* coded_task_output_;
  DataLayerManagerInterface* data_layer_manager_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_H
