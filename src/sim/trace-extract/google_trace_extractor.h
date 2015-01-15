// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Google cluster trace extractor tool.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H

#include <fstream>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/flow_graph.h"
#include "scheduling/quincy_cost_model.h"
#include "dataset_parser.h"

namespace firmament {
namespace sim {

class GoogleTraceExtractor {
 public:
  explicit GoogleTraceExtractor(string& trace_path);
  void Run();
 private:
  // parameters
  int64_t max_machines_, max_jobs_;
  string trace_path_;

  // CSV parsers
  MachineParser machine_parser_;
  JobParser job_parser_;
  TaskParser task_parser_;

  // state
  ResourceTopologyNodeDescriptor machine_tmpl;
  unordered_map<string, string> uuid_conversion_map_;
  unordered_map<uint64_t, JobID_t> job_id_conversion_map_;

  void AddMachineToTopology(const ResourceTopologyNodeDescriptor& machine_tmpl,
                            uint64_t machine_id,
                            ResourceTopologyNodeDescriptor& rtn_root);
  void AddNewTask(FlowGraph* flow_graph, QuincyCostModel* cost_model, uint64_t job_id,
                  uint64_t task_id, ofstream& out_file);
  void BinTasks(ofstream& out_file);
  ResourceTopologyNodeDescriptor& LoadInitialTopology();
  void LoadInitialMachines(ResourceTopologyNodeDescriptor &);
  unordered_map<uint64_t, JobDescriptor*>& LoadInitialJobs();
  void LoadInitalTasks(
      const unordered_map<uint64_t, JobDescriptor*>& initial_jobs);

  void PopulateJob(JobDescriptor* jd, uint64_t job_id);

  uint64_t ReadJobsFile(vector<uint64_t>* jobs);
  uint64_t ReadMachinesFile(vector<uint64_t>* machines);
  uint64_t ReadInitialTasksFile(const unordered_map<uint64_t, JobDescriptor*>& jobs);

  void ReplayTrace(FlowGraph* flow_graph, QuincyCostModel* cost_model,
                   const string& file_base);

  void reset_uuid(ResourceTopologyNodeDescriptor* rtnd, const string& hostname,
      const string& root_uuid);
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
