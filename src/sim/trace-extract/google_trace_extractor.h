// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Google cluster trace extractor tool.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/utils.h"
#include "misc/map-util.h"

namespace firmament {
namespace sim {

class GoogleTraceExtractor {
 public:
  explicit GoogleTraceExtractor(string& trace_path);
  void Run();
 private:
  void AddMachineToTopology(const ResourceTopologyNodeDescriptor& machine_tmpl,
                            ResourceTopologyNodeDescriptor* rtn_root);

  ResourceTopologyNodeDescriptor& LoadInitialMachines(int64_t max_num);
  unordered_map<uint64_t, JobDescriptor*>& LoadInitialJobs(int64_t max_jobs);
  void LoadInitalTasks(
      const unordered_map<uint64_t, JobDescriptor*>& initial_jobs);

  void PopulateJob(JobDescriptor* jd, uint64_t job_id);

  uint64_t ReadJobsFile(vector<uint64_t>* jobs, int64_t num_jobs);
  uint64_t ReadMachinesFile(vector<uint64_t>* machines, int64_t num_jobs);
  uint64_t ReadTasksFile(const unordered_map<uint64_t, JobDescriptor*>& jobs);

  void reset_uuid(ResourceTopologyNodeDescriptor* rtnd);

  map<string, string> uuid_conversion_map_;
  string trace_path_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
