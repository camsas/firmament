// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
#ifndef FIRMAMENT_SCHEDULING_FLOW_SOLVER_DISPATCHER_H
#define FIRMAMENT_SCHEDULING_FLOW_SOLVER_DISPATCHER_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/scheduling_delta.pb.h"
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
  void NodeBindingToSchedulingDelta(
      const TaskDescriptor& task, const ResourceDescriptor& res,
      unordered_map<TaskID_t, ResourceID_t>* task_bindings,
      vector<SchedulingDelta*>* deltas);
  multimap<uint64_t, uint64_t>* Run(SchedulerStats* scheduler_stats);

  uint64_t seq_num() const {
    return debug_seq_num_;
  }

 private:
  uint64_t AssignNode(vector< map< uint64_t, uint64_t > >* extracted_flow,
                      uint64_t node);
  multimap<uint64_t, uint64_t>* GetMappings(
      vector< map< uint64_t, uint64_t > >* extracted_flow,
      unordered_set<uint64_t> leaves, uint64_t sink);
  multimap<uint64_t, uint64_t>* ReadOutput(uint64_t* algorithm_runtime);
  vector< map< uint64_t, uint64_t> >* ReadFlowGraph(FILE* fptr,
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
