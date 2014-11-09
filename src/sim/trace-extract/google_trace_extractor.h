// TODO

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/utils.h"

namespace firmament {
namespace sim {

class GoogleTraceExtractor {
 public:
  explicit GoogleTraceExtractor(string& trace_path);
  void Run();
 private:
  ResourceTopologyNodeDescriptor& LoadInitialMachines(int32_t max_num);
  void LoadInitialJobs();
  void LoadInitalTasks();

  string trace_path_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EXTRACTOR_H
