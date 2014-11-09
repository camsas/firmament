// TODO

#include <cstdio>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <sys/stat.h>
#include <fcntl.h>

#include "sim/trace-extract/google_trace_extractor.h"
#include "scheduling/flow_graph.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/trivial_cost_model.h"
#include "misc/utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_on;

namespace firmament {
namespace sim {

#define MACHINE_TMPL_FILE "../../../tests/testdata/machine_topo.pbin"

DEFINE_int64(num_machines, -1, "Number of machines to extract; -1 for all.");
DEFINE_int32(runtime, -1, "Time to extract data for (from start of trace, in "
             "seconds); -1 for everything.");
DEFINE_string(output_dir, "", "Directory for output flow graphs.");

GoogleTraceExtractor::GoogleTraceExtractor(string& trace_path) :
    trace_path_(trace_path) {
}

ResourceTopologyNodeDescriptor&
GoogleTraceExtractor::LoadInitialMachines(int32_t max_num) {
  vector<uint64_t> machines;

  char line[200];
  vector<string> vals;
  FILE* fptr = NULL;
  string fname = trace_path_ + "/machine_events/part-00000-of-00001.csv"; 
  if ((fptr = fopen(fname.c_str(), "r")) == NULL) {
    LOG(ERROR) << "Failed to open trace for reading of machine events.";
  }
  int64_t l = 0;
  while (!feof(fptr)) {
    if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
      VLOG(2) << "Processing line " << l << ": " << line;
      boost::split(vals, line, is_any_of(","), token_compress_on);
      uint64_t time = lexical_cast<uint64_t>(vals[0]);
      if (time > 0 || (FLAGS_num_machines >= 0 && l >= FLAGS_num_machines)) {
        // We only care about the initial machines here
        break;
      }
      if (vals.size() != 6) {
        LOG(ERROR) << "Unexpected structure of machine event row";
      } else {
        uint64_t machine_id = lexical_cast<uint64_t>(vals[1]);
        uint64_t event_type = lexical_cast<uint64_t>(vals[2]);
        if (event_type == 0)
          machines.push_back(machine_id);
      }
    }
    l++;
  }
  LOG(INFO) << "Loaded " << machines.size() << " machines!";

  // Test resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open(MACHINE_TMPL_FILE, O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);
  // Create the machines
  ResourceTopologyNodeDescriptor* rtn_root = new ResourceTopologyNodeDescriptor();
  ResourceID_t root_uuid = GenerateUUID();
  rtn_root->mutable_resource_desc()->set_uuid(to_string(root_uuid));
  /*InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                     to_string(root_uuid));*/
  // Create each machine and add it to the graph
  for (vector<uint64_t>::const_iterator iter = machines.begin();
       iter != machines.end();
       ++iter) {
    ResourceTopologyNodeDescriptor* child = rtn_root->add_children();
    child->CopyFrom(machine_tmpl);
    child->set_parent_id(rtn_root->resource_desc().uuid());
    /*TraverseResourceProtobufTreeReturnRTND(
        child, boost::bind(&DIMACSExporterTest::reset_uuid, this, _1));*/
  }
  VLOG(1) << "Added " << machines.size() << " machines.";

  return *rtn_root;
}

void GoogleTraceExtractor::LoadInitialJobs() {
}

void GoogleTraceExtractor::LoadInitalTasks() {
}

void GoogleTraceExtractor::Run() {
  // command line argument sanity checking
  if (trace_path_.empty()) {
    LOG(FATAL) << "Please specify a path to the Google trace!";
  }

  LOG(INFO) << "Starting Google Trace extraction!";
  LOG(INFO) << "Number of machines to extract: " << FLAGS_num_machines;
  LOG(INFO) << "Time to extract for: " << FLAGS_runtime << " seconds.";

  ResourceTopologyNodeDescriptor& resource_topology =
      LoadInitialMachines(FLAGS_num_machines);
  LoadInitialJobs();
  LoadInitalTasks();

  FlowGraph g(new TrivialCostModel());
  // Add resources and job to flow graph
  g.AddResourceTopology(resource_topology);
  // Export
  DIMACSExporter exp;
  exp.Export(g);
  string outname = FLAGS_output_dir + "/test.dm";
  VLOG(1) << "Output written to " << outname;
  exp.Flush(outname);
}

}  // namespace sim
}  // namespace firmament
