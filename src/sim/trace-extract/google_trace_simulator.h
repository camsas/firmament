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

struct TaskIdentifier {                                                                                          
  uint64_t job_id;                                                                                               
  uint64_t task_index;                                                                                           
                                                                                                                 
  bool operator==(const TaskIdentifier& other) const {                                                           
    return job_id == other.job_id && task_index == other.task_index;                                             
  }                                                                                                              
};                                                                                                               
                                                                                                                 
struct TaskIdentifierHasher {                                                                                    
  size_t operator()(const TaskIdentifier& key) const {                                                           
    return hash<uint64_t>()(key.job_id) * 17 + hash<uint64_t>()(key.task_index);                                 
  }                                                                                                              
};                                                                                                               
                                                                                                                 
class GoogleTraceSimulator {
 public:
  explicit GoogleTraceSimulator(string& trace_path);
  void Run();
 private:
  void AddMachineToTopologyAndResetUuid(
  		  const ResourceTopologyNodeDescriptor& machine_tmpl, uint64_t machine_id,
        ResourceTopologyNodeDescriptor& rtn_root);
  ResourceTopologyNodeDescriptor& LoadInitialTopology();

  void PopulateJob(JobDescriptor* jd, uint64_t job_id);

  // Loads all the task runtimes and returns map task_identifier -> runtime.                                 
  unordered_map<TaskIdentifier, uint64_t,
	              TaskIdentifierHasher>& LoadTasksRunningTime();

  uint64_t ReplayMachineEvents(ResourceTopologyNodeDescriptor& root,
  		                         uint64_t max_runtime);
  uint64_t ReplayJobEvents(ResourceTopologyNodeDescriptor& root,
  		                     uint64_t max_runtime);
  uint64_t ReplayTaskEvents(ResourceTopologyNodeDescriptor& root,
                            uint64_t max_runtime);
  void ReplayTrace(ResourceTopologyNodeDescriptor& root, uint64_t max_runtime);

  void ResetUuid(ResourceTopologyNodeDescriptor* rtnd, const string& hostname,
      const string& root_uuid);

  // parameters
  //int64_t max_machines_;
  string trace_path_;

  // CSV parsers
  MachineParser machine_parser_;
  JobParser job_parser_;
  TaskParser task_parser_;

  // state
  uint64_t current_time_, num_machines_seen_;
  ResourceTopologyNodeDescriptor machine_tmpl_;
  unordered_map<uint64_t, JobDescriptor*> jobs_;
  unordered_map<uint64_t, unordered_map<uint32_t, TaskDescriptor*>> tasks_;
  unordered_map<string, string> uuid_conversion_map_;
  unordered_map<uint64_t, JobID_t> job_id_conversion_map_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
