// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Google cluster trace extractor tool.

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
#include "misc/pb_utils.h"
#include "misc/string_utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

namespace firmament {
namespace sim {

#define MACHINE_TMPL_FILE "../../../tests/testdata/machine_topo.pbin"
//#define MACHINE_TMPL_FILE "/tmp/mach_test.pbin"

DEFINE_int64(num_machines, -1, "Number of machines to extract; -1 for all.");
DEFINE_int64(num_jobs, -1, "Number of initial jobs to extract; -1 for all.");
DEFINE_int32(runtime, -1, "Time to extract data for (from start of trace, in "
             "seconds); -1 for everything.");
DEFINE_string(output_dir, "", "Directory for output flow graphs.");

void GoogleTraceExtractor::reset_uuid(ResourceTopologyNodeDescriptor* rtnd) {
  string old_parent_id = rtnd->parent_id();
  rtnd->set_parent_id(*FindOrNull(uuid_conversion_map_, rtnd->parent_id()));
  string new_uuid = to_string(GenerateUUID());
  VLOG(2) << "Resetting UUID for " << rtnd->resource_desc().uuid()
          << " to " << new_uuid << ", parent is " << rtnd->parent_id()
          << ", was " << old_parent_id;
  InsertOrUpdate(&uuid_conversion_map_, rtnd->resource_desc().uuid(),
                 new_uuid);
  rtnd->mutable_resource_desc()->set_uuid(new_uuid);
}


GoogleTraceExtractor::GoogleTraceExtractor(string& trace_path) :
    trace_path_(trace_path) {
}

uint64_t GoogleTraceExtractor::ReadMachinesFile(vector<uint64_t>* machines,
                                                int64_t num_machines) {
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
      boost::split(vals, line, is_any_of(","), token_compress_off);
      uint64_t time = lexical_cast<uint64_t>(vals[0]);
      if (time > 0 || (num_machines >= 0 && l >= num_machines)) {
        // We only care about the initial machines here
        break;
      }
      if (vals.size() != 6) {
        LOG(ERROR) << "Unexpected structure of machine event row";
      } else {
        uint64_t machine_id = lexical_cast<uint64_t>(vals[1]);
        uint64_t event_type = lexical_cast<uint64_t>(vals[2]);
        if (event_type == 0)
          machines->push_back(machine_id);
      }
    }
    l++;
  }
  return l;
}

uint64_t GoogleTraceExtractor::ReadJobsFile(vector<uint64_t>* jobs,
                                            int64_t num_jobs) {
  bool done = false;
  char line[200];
  vector<string> vals;
  FILE* fptr = NULL;
  int64_t j = 0;
  for (uint64_t f = 0; f < 500; f++) {
    string fname;
    spf(&fname, "%s/job_events/part-%05ld-of-00500.csv",
        trace_path_.c_str(), f);
    if ((fptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(ERROR) << "Failed to open trace for reading of job events.";
    }
    int64_t l = 0;
    while (!feof(fptr)) {
      if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
        VLOG(2) << "Processing line " << l << ": " << line;
        boost::split(vals, line, is_any_of(","), token_compress_off);
        uint64_t time = lexical_cast<uint64_t>(vals[0]);
        if (time > 0 || (num_jobs >= 0 && l >= num_jobs)) {
          // We only care about the initial jobs here, so break once
          // the time is non-zero
          done = true;
          break;
        }
        if (vals.size() != 8) {
          LOG(ERROR) << "Unexpected structure of job event row: found "
                     << vals.size() << " columns.";
        } else {
          uint64_t job_id = lexical_cast<uint64_t>(vals[2]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[3]);
          if (event_type == 0) {
            jobs->push_back(job_id);
            j++;
          }
        }
      }
      l++;
    }
    if (done)
      return j;
  }
  return j;
}

uint64_t GoogleTraceExtractor::ReadTasksFile(
    const unordered_map<uint64_t, JobDescriptor*>& jobs) {
  bool done = false;
  char line[200];
  vector<string> vals;
  FILE* fptr = NULL;
  int64_t t = 0;
  for (uint64_t f = 0; f < 500; f++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05ld-of-00500.csv",
        trace_path_.c_str(), f);
    if ((fptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(ERROR) << "Failed to open trace for reading of task events.";
    }
    int64_t l = 0;
    while (!feof(fptr)) {
      if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
        VLOG(2) << "Processing line " << l << ": " << line;
        boost::split(vals, line, is_any_of(","), token_compress_off);
        uint64_t time = lexical_cast<uint64_t>(vals[0]);
        if (time > 0) {
          // We only care about the initial tasks here, so break once
          // the time is non-zero
          done = true;
          break;
        }
        if (vals.size() != 13) {
          LOG(ERROR) << "Unexpected structure of task event row: found "
                     << vals.size() << " columns.";
        } else {
          uint64_t job_id = lexical_cast<uint64_t>(vals[2]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);
          if (event_type == 0) {
            if (JobDescriptor* jd = FindPtrOrNull(jobs, job_id)) {
              // This is a job we're interested in
              CHECK_NOTNULL(jd);
              TaskDescriptor* root_task = jd->mutable_root_task();
              TaskDescriptor* new_task = root_task->add_spawned();
              new_task->set_uid(GenerateTaskID(*root_task));
              new_task->set_state(TaskDescriptor::RUNNABLE);
              t++;
            }
          }
        }
      }
      l++;
    }
    if (done)
      return t;
  }
  return t;
}

void GoogleTraceExtractor::PopulateJob(JobDescriptor* jd, uint64_t job_id) {
  // XXX(malte): job_id argument is discareded and replaced by randomly
  // generated ID at the moment.
  jd->set_uuid(to_string(GenerateJobID()));
  TaskDescriptor* rt = jd->mutable_root_task();
  string bin;
  // XXX(malte): hack, should use logical job name
  spf(&bin, "%jd", job_id);
  rt->set_binary(bin);
  rt->set_uid(GenerateRootTaskID(*jd));
  rt->set_state(TaskDescriptor::RUNNABLE);
}

void GoogleTraceExtractor::AddMachineToTopology(
    const ResourceTopologyNodeDescriptor& machine_tmpl,
    ResourceTopologyNodeDescriptor* rtn_root) {
  ResourceTopologyNodeDescriptor* child = rtn_root->add_children();
  child->CopyFrom(machine_tmpl);
  child->set_parent_id(rtn_root->resource_desc().uuid());
  TraverseResourceProtobufTreeReturnRTND(
      child, boost::bind(&GoogleTraceExtractor::reset_uuid, this, _1));
}

ResourceTopologyNodeDescriptor&
GoogleTraceExtractor::LoadInitialMachines(int64_t max_num) {
  vector<uint64_t> machines;

  // Read the initial machine events from trace
  uint64_t num_machines = ReadMachinesFile(&machines, max_num);
  LOG(INFO) << "Loaded " << num_machines << " machines!";

  // Import a fictional machine resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open(MACHINE_TMPL_FILE, O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);

  // Create the machines
  ResourceTopologyNodeDescriptor* rtn_root = new 
      ResourceTopologyNodeDescriptor();
  ResourceID_t root_uuid = GenerateUUID();
  rtn_root->mutable_resource_desc()->set_uuid(to_string(root_uuid));
  InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                     to_string(root_uuid));
  // Create each machine and add it to the graph
  for (vector<uint64_t>::const_iterator iter = machines.begin();
       iter != machines.end();
       ++iter) {
    AddMachineToTopology(machine_tmpl, rtn_root);
  }
  LOG(INFO) << "Added " << machines.size() << " machines.";

  return *rtn_root;
}

unordered_map<uint64_t, JobDescriptor*>& GoogleTraceExtractor::LoadInitialJobs(
    int64_t max_jobs) {
  vector<uint64_t> job_ids;
  unordered_map<uint64_t, JobDescriptor*>* jobs;

  // Read the initial machine events from trace
  uint64_t num_jobs_read = ReadJobsFile(&job_ids, max_jobs);
  LOG(INFO) << "Read " << num_jobs_read << " initial jobs.";
  jobs = new unordered_map<uint64_t, JobDescriptor*>();

  // Add jobs
  for (vector<uint64_t>::const_iterator iter = job_ids.begin();
       iter != job_ids.end();
       ++iter) {
    JobDescriptor* jd = new JobDescriptor();
    PopulateJob(jd, *iter);
    CHECK(InsertOrUpdate(jobs, *iter, jd));
  }
  return *jobs;
}

void GoogleTraceExtractor::LoadInitalTasks(
    const unordered_map<uint64_t, JobDescriptor*>& initial_jobs) {
  // Read initial tasks from trace
  // and add tasks to jobs in initial_jobs
  uint64_t num_tasks = ReadTasksFile(initial_jobs);
  LOG(INFO) << "Added " << num_tasks << " initial tasks to "
            << initial_jobs.size() << " initial jobs.";
}

void GoogleTraceExtractor::Run() {
  // command line argument sanity checking
  if (trace_path_.empty()) {
    LOG(FATAL) << "Please specify a path to the Google trace!";
  }

  LOG(INFO) << "Starting Google Trace extraction!";
  LOG(INFO) << "Number of machines to extract: " << FLAGS_num_machines;
  LOG(INFO) << "Time to extract for: " << FLAGS_runtime << " seconds.";

  ResourceTopologyNodeDescriptor& initial_resource_topology =
      LoadInitialMachines(FLAGS_num_machines);
  unordered_map<uint64_t, JobDescriptor*>& initial_jobs =
      LoadInitialJobs(FLAGS_num_jobs);
  LoadInitalTasks(initial_jobs);

  FlowGraph g(new TrivialCostModel());
  // Add resources and job to flow graph
  g.AddResourceTopology(initial_resource_topology);
  // Add initial jobs
  for (unordered_map<uint64_t, JobDescriptor*>::const_iterator
       iter = initial_jobs.begin();
       iter != initial_jobs.end();
       ++iter) {
    VLOG(1) << "Add job with " << iter->second->root_task().spawned_size()
            << " child tasks of root task";
    g.AddJobNodes(iter->second);
  }
  // Export initial graph
  DIMACSExporter exp;
  exp.Export(g);
  string outname = FLAGS_output_dir + "/test.dm";
  VLOG(1) << "Output written to " << outname;
  exp.Flush(outname);
}

}  // namespace sim
}  // namespace firmament
