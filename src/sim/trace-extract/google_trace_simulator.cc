// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#include <cstdio>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <sys/stat.h>
#include <fcntl.h>

#include "sim/trace-extract/google_trace_simulator.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/flow_graph.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"
#include "scheduling/quincy_cost_model.h"
#include "scheduling/quincy_dispatcher.h"
#include "misc/utils.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

namespace firmament {
namespace sim {

#define SUBMIT_EVENT 0
#define SCHEDULE_EVENT 1
#define EVICT_EVENT 2
#define FAIL_EVENT 3
#define FINISH_EVENT 4
#define KILL_EVENT 5
#define LOST_EVENT 6

#define MACHINE_ADD 0
#define MACHINE_REMOVE 1
#define MACHINE_UPDATE 2

#define MACHINE_TMPL_FILE "../../../tests/testdata/machine_topo.pbin"
//#define MACHINE_TMPL_FILE "/tmp/mach_test.pbin"

DEFINE_int64(num_machines, -1, "Number of machines to extract; -1 for all.");
DEFINE_int64(num_jobs, -1, "Number of initial jobs to extract; -1 for all.");
DEFINE_uint64(runtime, -1, "Time to extract data for (from start of trace, in "
             "seconds); -1 for everything.");
DEFINE_string(output_dir, "", "Directory for output flow graphs.");
DEFINE_bool(tasks_preemption_bins, false, "Compute bins of number of preempted tasks.");
DEFINE_uint64(bin_time_duration, 10, "Bin size in seconds.");
DEFINE_string(task_bins_output, "bins.out", "The file in which the task bins are written.");
DEFINE_bool(run_incremental_scheduler, false, "Run the Flowlessly incremental scheduler.");

GoogleTraceSimulator::GoogleTraceSimulator(string& trace_path) : trace_path_(trace_path) {
}

ResourceID_t GoogleTraceSimulator::AddMachineToTopologyAndResetUuid(
    const ResourceTopologyNodeDescriptor& machine_tmpl, uint64_t machine_id,
    ResourceTopologyNodeDescriptor* new_machine) {
  new_machine = rtn_root_.add_children();
  new_machine->CopyFrom(machine_tmpl);
  const string& root_uuid = rtn_root_.resource_desc().uuid();
  char hn[100];
  sprintf(hn, "h%ju", machine_id);
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&GoogleTraceSimulator::ResetUuid, this, _1, string(hn), root_uuid));
  new_machine->mutable_resource_desc()->set_friendly_name(hn);
  return ResourceIDFromString(new_machine->resource_desc().uuid());
}

TaskDescriptor* GoogleTraceSimulator::AddTaskToJob(JobDescriptor* jd_ptr) {
  CHECK_NOTNULL(jd_ptr);
  TaskDescriptor* root_task = jd_ptr->mutable_root_task();
  TaskDescriptor* new_task = root_task->add_spawned();
  new_task->set_uid(GenerateTaskID(*root_task));
  new_task->set_state(TaskDescriptor::RUNNABLE);
  // TODO(ionel): Task doesn't get added to the flow graph.
  return new_task;
}

TaskDescriptor* GoogleTraceSimulator::AddNewTask(
    FlowGraph* flow_graph, TaskIdentifier task_identifier,
    unordered_map<uint64_t, TaskIdentifier>* flow_id_to_task_id) {
  JobDescriptor** jdpp = FindOrNull(job_id_to_jd_, task_identifier.job_id);
  JobDescriptor* jd_ptr;
  if (!jdpp) {
    // Add new job to the graph
    jd_ptr = new JobDescriptor();
    PopulateJob(jd_ptr, task_identifier.job_id);
    jdpp = FindOrNull(job_id_to_jd_, task_identifier.job_id);
    CHECK_NOTNULL(jdpp);
  } else {
    jd_ptr = *jdpp;
  }
  TaskDescriptor* td_ptr = AddTaskToJob(jd_ptr);
  flow_graph->AddOrUpdateJobNodes(jd_ptr);
  InsertOrUpdate(flow_id_to_task_id, td_ptr->uid(), task_identifier);
  return td_ptr;
}

void GoogleTraceSimulator::BinTasksByEventType(uint64_t event, ofstream& out_file) {
  char line[200];
  vector<string> vals;
  FILE* fptr = NULL;
  uint64_t time_interval_bound = FLAGS_bin_time_duration;
  uint64_t num_tasks = 0;
  for (uint64_t file_num = 0; file_num < 500; file_num++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05ld-of-00500.csv", trace_path_.c_str(), file_num);
    if ((fptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(ERROR) << "Failed to open trace for reading of task events.";
    }
    while (!feof(fptr)) {
      if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
        boost::split(vals, line, is_any_of(","), token_compress_off);
        if (vals.size() != 13) {
          LOG(ERROR) << "Unexpected structure of task event row: found "
                     << vals.size() << " columns.";
        } else {
          uint64_t task_time = lexical_cast<uint64_t>(vals[0]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);
          if (event_type == event) {
            if (task_time <= time_interval_bound) {
              num_tasks++;
            } else {
              out_file << "(" << time_interval_bound - FLAGS_bin_time_duration << ", " <<
                time_interval_bound << "]: " << num_tasks << "\n";
              time_interval_bound += FLAGS_bin_time_duration;
              while (time_interval_bound < task_time) {
                out_file << "(" << time_interval_bound - FLAGS_bin_time_duration << ", " <<
                  time_interval_bound << "]: 0\n";
                time_interval_bound += FLAGS_bin_time_duration;
              }
              num_tasks = 1;
            }
          }
        }
      }
    }
  }
  out_file << "(" << time_interval_bound - FLAGS_bin_time_duration << ", " <<
    time_interval_bound << "]: " << num_tasks << "\n";
}

void GoogleTraceSimulator::LoadInitialJobs(int64_t max_jobs) {
  set<uint64_t> job_ids;

  // Read the initial job events from trace
  uint64_t num_jobs_read = ReadJobsFile(&job_ids, max_jobs);
  LOG(INFO) << "Loaded " << num_jobs_read << " initial jobs.";

  // Add jobs
  for (set<uint64_t>::const_iterator iter = job_ids.begin(); iter != job_ids.end(); ++iter) {
    JobDescriptor* jd = new JobDescriptor();
    PopulateJob(jd, *iter);
    CHECK(InsertOrUpdate(&job_id_to_jd_, *iter, jd));
  }
}

void GoogleTraceSimulator::LoadInitialMachines(
    int64_t max_num_machines) {
  set<uint64_t> machines;

  // Read the initial machine events from trace
  uint64_t num_machines = ReadMachinesFile(&machines, max_num_machines);
  LOG(INFO) << "Loaded " << num_machines << " machines.";

  // Import a fictional machine resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open(MACHINE_TMPL_FILE, O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);

  // Create the machines
  ResourceID_t root_uuid = GenerateRootResourceID("XXXgoogleXXX");
  rtn_root_.mutable_resource_desc()->set_uuid(to_string(root_uuid));
  LOG(INFO) << "Root res ID is " << to_string(root_uuid);
  InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid), to_string(root_uuid));
  // Create each machine and add it to the graph
  for (set<uint64_t>::const_iterator iter = machines.begin(); iter != machines.end(); ++iter) {
    ResourceTopologyNodeDescriptor* new_machine = NULL;
    ResourceID_t res_id = AddMachineToTopologyAndResetUuid(machine_tmpl, *iter, new_machine);
    CHECK(InsertOrUpdate(&machine_id_to_res_id_, *iter, res_id));
  }
  LOG(INFO) << "Added " << machines.size() << " machines.";
}

void GoogleTraceSimulator::LoadInitialTasks() {
  // Read initial tasks from trace and add tasks to jobs in initial_jobs
  uint64_t num_tasks = ReadInitialTasksFile();
  LOG(INFO) << "Added " << num_tasks << " initial tasks to "
            << job_id_to_jd_.size() << " initial jobs.";
}

EventDescriptor_EventType GoogleTraceSimulator::TranslateMachineEventToEventType(int32_t machine_event) {
  if (machine_event == MACHINE_ADD) {
    return EventDescriptor::ADD_MACHINE;
  } else if (machine_event == MACHINE_REMOVE) {
    return EventDescriptor::REMOVE_MACHINE;
  } else if (machine_event == MACHINE_UPDATE) {
    return EventDescriptor::UPDATE_MACHINE;
  } else {
    LOG(FATAL) << "Unexpected machine event type: " << machine_event;
  }
}

void GoogleTraceSimulator::LoadMachineEvents() {
  char line[200];
  vector<string> cols;
  FILE* machines_file;
  string machines_file_name = trace_path_ + "/machine_events/part-00001-of-00500.csv";
  if ((machines_file = fopen(machines_file_name.c_str(), "r")) == NULL) {
    LOG(ERROR) << "Failed to open trace for reading machine events.";
  }
  int64_t num_line = 1;
  while (!feof(machines_file)) {
    if (fscanf(machines_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(","), token_compress_off);
      if (cols.size() != 6) {
        LOG(ERROR) << "Unexpected structure of machine events on line " << num_line << ": found "
                   << cols.size() << " columns.";
      } else {
        // row schema: (timestamp, machine_id, event_type, platform, CPUs, Memory)
        EventDescriptor* event_desc = new EventDescriptor();
        event_desc->set_machine_id(lexical_cast<uint64_t>(cols[1]));
        event_desc->set_type(TranslateMachineEventToEventType(lexical_cast<int32_t>(cols[2])));
        uint64_t timestamp = lexical_cast<uint64_t>(cols[0]);
        if (event_desc->type() == EventDescriptor::REMOVE_MACHINE ||
            event_desc->type() == EventDescriptor::ADD_MACHINE) {
          events_.insert(pair<uint64_t, EventDescriptor*>(timestamp, event_desc));
        } else {
          // TODO(ionel): Handle machine update events.
        }
      }
    }
    num_line++;
  }
}

unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>& GoogleTraceSimulator::LoadTasksRunningTime() {
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher> *task_runtime =
    new unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>();
  char line[200];
  vector<string> cols;
  FILE* tasks_file = NULL;
  string tasks_file_name = trace_path_ + "/task_runtime_events/task_runtime_events.csv";
  if ((tasks_file = fopen(tasks_file_name.c_str(), "r")) == NULL) {
    LOG(ERROR) << "Failed to open trace runtime events file.";
  }
  int64_t num_line = 1;
  while (!feof(tasks_file)) {
    if (fscanf(tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
      if (cols.size() != 13) {
        LOG(ERROR) << "Unexpected structure of task runtime row on line: " << num_line;
      } else {
        TaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);
        uint64_t runtime = lexical_cast<uint64_t>(cols[4]);
        if (!InsertIfNotPresent(task_runtime, task_id, runtime)) {
          LOG(ERROR) << "There should not be more than an entry for job " << task_id.job_id <<
            ", task " << task_id.task_index;
        }
      }
    }
    num_line++;
  }
  return *task_runtime;
}

uint64_t GoogleTraceSimulator::ReadJobsFile(set<uint64_t>* jobs, int64_t num_jobs) {
  bool done = false;
  char line[200];
  vector<string> vals;
  FILE* fptr = NULL;
  int64_t j = 0;
  for (uint64_t f = 0; f < 500; f++) {
    string fname;
    spf(&fname, "%s/job_events/part-%05ld-of-00500.csv", trace_path_.c_str(), f);
    if ((fptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(ERROR) << "Failed to open trace for reading of job events.";
    }
    int64_t l = 0;
    while (!feof(fptr)) {
      if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
        VLOG(3) << "Processing line " << l << ": " << line;
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
          if (event_type == SUBMIT_EVENT) {
            jobs->insert(job_id);
            j++;
          } else if (event_type == KILL_EVENT) {
            if (jobs->erase(job_id)) {
              j--;
            }
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

uint64_t GoogleTraceSimulator::ReadMachinesFile(set<uint64_t>* machines, int64_t num_machines) {
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
      VLOG(3) << "Processing line " << l << ": " << line;
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
        if (event_type == MACHINE_ADD) {
          machines->insert(machine_id);
        } else if (event_type == MACHINE_REMOVE) {
          machines->erase(machine_id);
        } else if (event_type == MACHINE_UPDATE) {
          // TODO(ionel): Handle machine update event.
        }
      }
    }
    l++;
  }
  return l;
}

uint64_t GoogleTraceSimulator::ReadInitialTasksFile() {
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
        VLOG(3) << "Processing line " << l << ": " << line;
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
          uint64_t task_index = lexical_cast<uint64_t>(vals[3]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);
          if (event_type == SUBMIT_EVENT) {
            if (JobDescriptor* jd = FindPtrOrNull(job_id_to_jd_, job_id)) {
              // This is a job we're interested in
              TaskDescriptor* new_task = AddTaskToJob(jd);
              TaskIdentifier task_id;
              task_id.job_id = job_id;
              task_id.task_index = task_index;
              InsertIfNotPresent(&task_id_to_td_, task_id, new_task);
              t++;
            }
          }
          // TODO(ionel): Can there be a situation in which a task is
          // submitted and killed at timestamp 0?
        }
      }
      l++;
    }
    if (done)
      return t;
  }
  return t;
}

void GoogleTraceSimulator::PopulateJob(JobDescriptor* jd, uint64_t job_id) {
  // Generate a hash out of the trace job_id.
  JobID_t new_job_id = GenerateJobID(job_id);
  // Maintain a mapping between the trace job_id and the generated job_id.
  jd->set_uuid(to_string(new_job_id));
  InsertOrUpdate(&job_id_to_jd_, job_id, jd);
  TaskDescriptor* rt = jd->mutable_root_task();
  string bin;
  // XXX(malte): hack, should use logical job name
  spf(&bin, "%jd", job_id);
  rt->set_binary(bin);
  rt->set_uid(GenerateRootTaskID(*jd));
  rt->set_state(TaskDescriptor::RUNNABLE);
}

void GoogleTraceSimulator::ResetUuid(ResourceTopologyNodeDescriptor* rtnd,
    const string& hostname, const string& root_uuid) {
  string new_uuid;
  if (rtnd->has_parent_id()) {
    // This is an intermediate node, so translate the parent UUID via the lookup table
    const string& old_parent_id = rtnd->parent_id();
    string* new_parent_id = FindOrNull(uuid_conversion_map_, rtnd->parent_id());
    CHECK_NOTNULL(new_parent_id);
    VLOG(2) << "Resetting parent UUID for " << rtnd->resource_desc().uuid()
            << ", parent was " << old_parent_id
            << ", is now " << *new_parent_id;
    rtnd->set_parent_id(*new_parent_id);
    // Grab a new UUID for the node itself
    new_uuid = to_string(GenerateResourceID());
  } else {
    // This is the top of a machine topology, so generate a first UUID for its
    // topology based on its hostname and link it into the root
    rtnd->set_parent_id(root_uuid);
    new_uuid = to_string(GenerateRootResourceID(hostname));
  }
  VLOG(2) << "Resetting UUID for " << rtnd->resource_desc().uuid() << " to " << new_uuid;
  InsertOrUpdate(&uuid_conversion_map_, rtnd->resource_desc().uuid(), new_uuid);
  rtnd->mutable_resource_desc()->set_uuid(new_uuid);
}

void GoogleTraceSimulator::ReplayTrace(FlowGraph* flow_graph) {
  // Load all the machine events.
  LoadMachineEvents();
  // Load tasks' runtime.
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>& task_runtime =
    LoadTasksRunningTime();
  // Multimap from timestamp to task. The key represents the timestamp at which the task ends.
  multimap<uint64_t, TaskDescriptor*> task_end_runtime;
  // Mapping from flow graph task uid to trace task id.
  unordered_map<uint64_t, TaskIdentifier> flow_id_to_task_id;

  // Import a fictional machine resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open(MACHINE_TMPL_FILE, O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);

  scheduler::QuincyDispatcher quincy_dispatcher =
    scheduler::QuincyDispatcher(shared_ptr<FlowGraph>(flow_graph), false);
  // The first time the solver runs from scratch.
  map<uint64_t, uint64_t>* task_mappings = quincy_dispatcher.Run();
  UpdateFlowGraph(flow_graph, task_mappings, task_runtime, task_end_runtime);

  char line[200];
  vector<string> vals;
  FILE* f_task_events_ptr = NULL;
  uint64_t time_interval_bound = FLAGS_bin_time_duration;
  uint64_t last_timestamp = 0;
  for (uint64_t file_num = 0; file_num < 500; file_num++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05ld-of-00500.csv", trace_path_.c_str(), file_num);
    if ((f_task_events_ptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(ERROR) << "Failed to open trace for reading of task events.";
    }
    while (!feof(f_task_events_ptr)) {
      if (fscanf(f_task_events_ptr, "%[^\n]%*[\n]", &line[0]) > 0) {
        boost::split(vals, line, is_any_of(","), token_compress_off);
        if (vals.size() != 13) {
          LOG(ERROR) << "Unexpected structure of task event row: found "
                     << vals.size() << " columns.";
        } else {
          TaskIdentifier task_identifier;
          uint64_t task_time = lexical_cast<uint64_t>(vals[0]);
          task_identifier.job_id = lexical_cast<uint64_t>(vals[2]);
          task_identifier.task_index = lexical_cast<uint64_t>(vals[3]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);

          if (task_time == 0) {
            // The task is already present in the graph.
            continue;
          }

          // We only run for the first FLAGS_runtime seconds.
          if (FLAGS_runtime < task_time) {
            LOG(INFO) << "Terminating at : " << task_time;
            return;
          }

          if (task_time > last_timestamp) {
            // Apply all the machine events.
            ApplyMachineEvents(last_timestamp, task_time, flow_graph, machine_tmpl);
            last_timestamp = task_time;
          }

          if (event_type == SUBMIT_EVENT) {
            if (task_time <= time_interval_bound) {
              AddNewTask(flow_graph, task_identifier, &flow_id_to_task_id);
            } else {
              task_mappings = quincy_dispatcher.Run();
              UpdateFlowGraph(flow_graph, task_mappings, task_runtime, task_end_runtime);

              // Update current time.
              time_interval_bound += FLAGS_bin_time_duration;
              while (time_interval_bound < task_time) {
                time_interval_bound += FLAGS_bin_time_duration;
              }

              AddNewTask(flow_graph, task_identifier, &flow_id_to_task_id);
            }
          }

        }
      }
    }
  }
}

void GoogleTraceSimulator::Run() {
  // command line argument sanity checking
  if (trace_path_.empty()) {
    LOG(FATAL) << "Please specify a path to the Google trace!";
  }

  LOG(INFO) << "Starting Google Trace extraction!";
  LOG(INFO) << "Number of machines to extract: " << FLAGS_num_machines;
  LOG(INFO) << "Time to extract for: " << FLAGS_runtime << " seconds.";
  LoadInitialMachines(FLAGS_num_machines);
  LoadInitialJobs(FLAGS_num_jobs);
  LoadInitialTasks();

  QuincyCostModel* cost_model = new QuincyCostModel();
  FlowGraph g(cost_model);
  // Add resources and job to flow graph
  g.AddResourceTopology(rtn_root_);
  // Add initial jobs
  for (unordered_map<uint64_t, JobDescriptor*>::const_iterator iter = job_id_to_jd_.begin();
       iter != job_id_to_jd_.end(); ++iter) {
    VLOG(1) << "Add job with " << iter->second->root_task().spawned_size()
            << " child tasks of root task";
    g.AddOrUpdateJobNodes(iter->second);
  }
  if (FLAGS_tasks_preemption_bins) {
    ofstream out_file(FLAGS_output_dir + "/" + FLAGS_task_bins_output);
    if (out_file.is_open()) {
      BinTasksByEventType(EVICT_EVENT, out_file);
      out_file.close();
    } else {
      LOG(ERROR) << "Could not open bin output file.";
    }
  }
  if (FLAGS_run_incremental_scheduler) {
    ReplayTrace(&g);
  }
}

void GoogleTraceSimulator::UpdateFlowGraph(
    FlowGraph* flow_graph, map<uint64_t, uint64_t>* task_mappings,
    unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>& task_runtime,
    multimap<uint64_t, TaskDescriptor*>& task_end_runtime) {
  // TODO(ionel): Implement.
}

void GoogleTraceSimulator::ApplyMachineEvents(
    uint64_t last_time, uint64_t cur_time, FlowGraph* flow_graph,
    const ResourceTopologyNodeDescriptor& machine_tmpl) {
  for (; last_time <= cur_time; last_time++) {
    pair<multimap<uint64_t, EventDescriptor*>::iterator,
         multimap<uint64_t, EventDescriptor*>::iterator> range_events =
      events_.equal_range(last_time);
    for (multimap<uint64_t, EventDescriptor*>::iterator it = range_events.first;
         it != range_events.second; ++it) {
      if (it->second->type() == EventDescriptor::ADD_MACHINE) {
        ResourceID_t* res_id_ptr = FindOrNull(machine_id_to_res_id_, it->second->machine_id());
        if (res_id_ptr) {
          LOG(ERROR) << "Already added machine " << it->second->machine_id();
          continue;
        }
        // Create a new machine topology descriptor.
        ResourceTopologyNodeDescriptor* new_machine = NULL;
        ResourceID_t res_id =
          AddMachineToTopologyAndResetUuid(machine_tmpl, it->second->machine_id(), new_machine);
        // Add mapping from machine_id to the new resource_id.
        InsertOrUpdate(&machine_id_to_res_id_, it->second->machine_id(), res_id);
        // Add the new machine to the flow graph.
        flow_graph->AddResourceNode(new_machine);
      } else if (it->second->type() == EventDescriptor::REMOVE_MACHINE) {
        ResourceID_t* res_id_ptr = FindOrNull(machine_id_to_res_id_, it->second->machine_id());
        CHECK_NOTNULL(res_id_ptr);
        // Delete the machine from the flow graph.
        flow_graph->DeleteResourceNode(*res_id_ptr);
        machine_id_to_res_id_.erase(it->second->machine_id());
      } else if (it->second->type() == EventDescriptor::UPDATE_MACHINE) {
        // TODO(ionel): Handle machine update event.
      } else {
        LOG(ERROR) << "Unexpected machine event";
      }
    }
    events_.erase(last_time);
  }
}

}  // namespace sim
}  // namespace firmament
