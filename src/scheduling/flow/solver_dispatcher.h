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

#ifndef FIRMAMENT_SCHEDULING_FLOW_SOLVER_DISPATCHER_H
#define FIRMAMENT_SCHEDULING_FLOW_SOLVER_DISPATCHER_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/flow/dimacs_exporter.h"
#include "scheduling/flow/json_exporter.h"
#include "scheduling/flow/flow_graph_manager.h"

namespace firmament {
namespace scheduler {

class SolverDispatcher {
 public:
  SolverDispatcher(shared_ptr<FlowGraphManager> flow_graph_manager,
                   bool solver_ran_once);
  ~SolverDispatcher();

  void ExportJSON(string* output) const;
  multimap<uint64_t, uint64_t>* Run(SchedulerStats* scheduler_stats);

  uint64_t seq_num() const {
    return debug_seq_num_;
  }

 private:
  void ExportGraph(FILE* stream);
  multimap<uint64_t, uint64_t>* GetMappings(
      vector<unordered_map<uint64_t, uint64_t>>* extracted_flow,
      unordered_set<uint64_t> leaves, uint64_t sink);
  multimap<uint64_t, uint64_t>* ReadOutput(uint64_t* algorithm_runtime);
  vector<unordered_map<uint64_t, uint64_t>>* ReadFlowGraph(
      FILE* fptr,
      uint64_t* algorithm_runtime,
      uint64_t num_vertices);
  multimap<uint64_t, uint64_t>* ReadTaskMappingChanges(
      FILE* fptr,
      uint64_t* algorithm_runtime);
  void SolverConfiguration(const string& solver, string* binary,
                           vector<string> *args);
  friend void *ExportToSolver(void *x);

  shared_ptr<FlowGraphManager> flow_graph_manager_;
  // DIMACS exporter for interfacing to the solver
  DIMACSExporter dimacs_exporter_;
  // JSON exporter for debug and visualisation
  JSONExporter json_exporter_;
  // Boolean that indicates if the solver has knowledge of the flow graph (i.e.
  // it is set after the initial from scratch run of the solver).
  bool solver_ran_once_;
  // Debug sequence number (for solver input/output files written to /tmp)
  uint64_t debug_seq_num_;

  // FDs used to communicate with the solver.
  int errfd_[2];
  int outfd_[2];
  int infd_[2];
  FILE* to_solver_;
  FILE* from_solver_;
  FILE* from_solver_stderr_;
};

} // namespace scheduler
} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_SOLVER_DISPATCHER_H
