// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>

#include <sys/stat.h>
#include <fcntl.h>

#include "../ext/spooky_hash/SpookyV2.h"

#include "scheduling/dimacs_change_stats.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/flow_graph.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"
#include "scheduling/quincy_dispatcher.h"
#include "sim/dfs/simulated_dfs.h"
#include "sim/trace-extract/google_trace_simulator.h"
#include "misc/utils.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

DEFINE_string(machine_tmpl_file, "../../../tests/testdata/machine_topo.pbin",
              "File specifying machine topology.");

DEFINE_bool(tasks_preemption_bins, false,
            "Compute bins of number of preempted tasks.");
DEFINE_uint64(bin_time_duration, 10, "Bin size in microseconds.");
DEFINE_string(task_bins_output, "bins.out",
              "The file in which the task bins are written.");

DEFINE_int32(num_files_to_process, 500, "Number of files to process.");
DEFINE_uint64(runtime, 9223372036854775807,
              "Maximum time in microsec to extract data for"
              "(from start of trace)");
DEFINE_uint64(max_events, UINT64_MAX,
              "Maximum number of task events to process.");
DEFINE_uint64(max_scheduling_rounds, UINT64_MAX,
              "Maximum number of scheduling rounds to run for.");
DEFINE_double(percentage, 100.0, "Percentage of events to retain.");
DEFINE_uint64(batch_step, 0, "Batch mode: time interval to run solver at.");
DEFINE_double(online_factor, 0.0, "Online mode: speed at which to run at. "
              "Factor of 1 corresponds to real-time. Larger to include"
              " overheads elsewhere in Firmament etc., smaller to simulate"
              " solver running faster.");
DEFINE_double(online_max_time, 100000000.0, "Online mode: cap on time solver "
              "takes to run, in seconds. If unspecified, no cap imposed."
              " Only use with incremental solver, in which case it should"
              " be set to the worst case runtime of the full solver.");

DEFINE_string(stats_file, "", "File to write CSV of statistics.");
DEFINE_string(graph_output_file, "",
              "File to write incremental DIMACS export.");
DEFINE_string(output_dir, "", "Directory for output flow graphs.");
DEFINE_bool(graph_output_events, true, "If -graph_output_file specified: "
                                       "export simulator events as comments?");

DEFINE_string(solver, "flowlessly", "Solver to use: flowlessly | cs2 | custom.");
DEFINE_uint64(solver_timeout, UINT64_MAX,
              "Timeout: terminate after waiting this number of seconds");
DEFINE_bool(run_incremental_scheduler, false,
            "Run the Flowlessly incremental scheduler.");
DEFINE_int32(flow_scheduling_cost_model, 0,
             "Flow scheduler cost model to use. "
             "Values: 0 = TRIVIAL, 1 = RANDOM, 2 = SJF, 3 = QUINCY, "
             "4 = WHARE, 5 = COCO, 6 = OCTOPUS, 7 = VOID, 8 = SIMULATED QUINCY");

// Racks contain "between 29 and 31 computers" in Quincy test setup
DEFINE_uint64(simulated_quincy_machines_per_rack, 30,
                           "Machines are binned into racks of specified size.");
// Defaults from Quincy paper
DEFINE_double(simulated_quincy_delta_preferred_machine, 0.1,
    "Threshold of proportion of data stored on machine for it to be on preferred list.");
DEFINE_double(simulated_quincy_delta_preferred_rack, 0.1,
    "Threshold of proportion of data stored on rack for it to be on preferred list.");
DEFINE_uint64(simulated_quincy_tor_transfer_cost, 1,
    "Cost per unit of data transferred in core switch.");
// Cost was 2 for most experiments, 20 for constrained network experiments
DEFINE_uint64(simulated_quincy_core_transfer_cost, 2,
    "Cost per unit of data transferred in core switch.");
// Distributed filesystem options
DEFINE_uint64(simulated_quincy_blocks_per_machine, 98304,
   "Number of 64 MB blocks each machine stores. Defaults to 98304, i.e. 6 TB.");
DEFINE_uint64(simulated_quincy_replication_factor, 3,
    "The number of times each block should be replicated.");
// See google_runtime_distribution.h for explanation of these defaults
DEFINE_double(simulated_quincy_runtime_factor, 0.298,
                           "Runtime power law distribution: factor parameter.");
DEFINE_double(simulated_quincy_runtime_power, -0.2627,
                           "Runtime power law distribution: factor parameter.");
// Input size distribution. See Evaluation Plan for derivation of defaults.
DEFINE_uint64(simulated_quincy_input_percent_min, 50,
                         "Percentage of input files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_input_min_blocks, 1, "Minimum # of blocks in input file.");
DEFINE_double(simulated_quincy_input_max_blocks, 320, "Maximum # of blocks in input file.");
DEFINE_uint64(simulated_quincy_input_percent_over_tolerance, 50,
               "Percentage # of blocks allowed to exceed the value predicted.");
// File size distribution. See Evaluation Plan for derivation of defaults.
DEFINE_uint64(simulated_quincy_file_percent_min, 20,
                         "Percentage of files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_file_min_blocks, 1, "Minimum # of blocks in file.");
DEFINE_double(simulated_quincy_file_max_blocks, 160, "Maximum # of blocks in file.");
// Random seed
DEFINE_uint64(simulated_quincy_random_seed, 42, "Seed for random generators.");

namespace firmament {
namespace sim {

#define SUBMIT_EVENT 0
#define SCHEDULE_EVENT 1
#define EVICT_EVENT 2
#define FAIL_EVENT 3
#define FINISH_EVENT 4
#define KILL_EVENT 5
#define LOST_EVENT 6
#define UPDATE_PENDING_EVENT 7
#define UPDATE_RUNNING_EVENT 8

#define MACHINE_ADD 0
#define MACHINE_REMOVE 1
#define MACHINE_UPDATE 2

const static uint64_t SEED = 0;

#define EPS 0.00001

// It varies a little over time, but relatively constant. Used for calculation
// of how much storage space we have.
const static uint64_t MACHINES_IN_TRACE = 10000;

ofstream *timeout_file;
void alarm_handler(int sig) {
  signal(SIGALRM, SIG_IGN);
  if (timeout_file) {
    *timeout_file << "0,Timeout,Timeout,Timeout,Timeout";
    if (FLAGS_batch_step == 0) {
      // online
      *timeout_file << ",Timeout";
    }
    *timeout_file << ",,,,," << std::endl;
   }
   // N.B. Don't use LOG(FATAL) since we want a successful return code
   LOG(ERROR) << "Timeout after waiting for solver for "
              << FLAGS_solver_timeout << " seconds.";
   exit(0);
}

GoogleTraceSimulator::GoogleTraceSimulator(const string& trace_path) :
  job_map_(new JobMap_t), task_map_(new TaskMap_t),
  resource_map_(new ResourceMap_t), trace_path_(trace_path),
  knowledge_base_(new KnowledgeBaseSimulator) {
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>();
  switch (FLAGS_flow_scheduling_cost_model) {
  case FlowSchedulingCostModelType::COST_MODEL_TRIVIAL:
    cost_model_ = new TrivialCostModel(task_map_, leaf_res_ids);
    VLOG(1) << "Using the trivial cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_RANDOM:
    cost_model_ = new RandomCostModel(task_map_, leaf_res_ids);
    VLOG(1) << "Using the random cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_COCO:
    cost_model_ = new CocoCostModel(resource_map_, rtn_root_, task_map_,
                                    leaf_res_ids, knowledge_base_);
    VLOG(1) << "Using the coco cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_SJF:
    cost_model_ = new SJFCostModel(task_map_, leaf_res_ids, knowledge_base_);
    VLOG(1) << "Using the SJF cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_QUINCY:
    cost_model_ =
      new QuincyCostModel(resource_map_, job_map_, task_map_,
                          &task_bindings_, leaf_res_ids,
                          knowledge_base_);
    VLOG(1) << "Using the Quincy cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_WHARE:
    cost_model_ = new WhareMapCostModel(resource_map_, task_map_,
                                        knowledge_base_);
    VLOG(1) << "Using the Whare-Map cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_OCTOPUS:
    cost_model_ = new OctopusCostModel(resource_map_);
    VLOG(1) << "Using the octopus cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_VOID:
    cost_model_ = new VoidCostModel();
    VLOG(1) << "Using the void cost model";
    break;
  case FlowSchedulingCostModelType::COST_MODEL_SIMULATED_QUINCY:
    cost_model_ = SetupSimulatedQuincyCostModel(leaf_res_ids);
    VLOG(1) << "Using the simulated Quincy cost model";
    break;
  default:
  LOG(FATAL) << "Unknown flow scheduling cost model specificed "
             << "(" << FLAGS_flow_scheduling_cost_model << ")";
  }
  flow_graph_.reset(new FlowGraph(cost_model_, leaf_res_ids));
  cost_model_->SetFlowGraph(flow_graph_);
  knowledge_base_->SetCostModel(cost_model_);
  quincy_dispatcher_ =
    new scheduler::QuincyDispatcher(shared_ptr<FlowGraph>(flow_graph_), false);
}

SimulatedQuincyCostModel *GoogleTraceSimulator::SetupSimulatedQuincyCostModel(
   unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids) {
  GoogleBlockDistribution *input_block_distn = new GoogleBlockDistribution(
                                      FLAGS_simulated_quincy_input_percent_min,
                                      FLAGS_simulated_quincy_input_min_blocks,
                                      FLAGS_simulated_quincy_input_max_blocks);
  GoogleBlockDistribution *file_block_distn = new GoogleBlockDistribution(
                                        FLAGS_simulated_quincy_file_percent_min,
                                        FLAGS_simulated_quincy_file_min_blocks,
                                        FLAGS_simulated_quincy_file_max_blocks);
  GoogleRuntimeDistribution *runtime_distn = new GoogleRuntimeDistribution(
                                        FLAGS_simulated_quincy_runtime_factor,
                                        FLAGS_simulated_quincy_runtime_power);

  uint64_t num_machines = std::round(MACHINES_IN_TRACE * FLAGS_percentage / 100.0);
  LOG(INFO) << "Assuming " << num_machines << " machines in cluster.";
  SimulatedDFS *dfs = new SimulatedDFS(num_machines,
                                   FLAGS_simulated_quincy_blocks_per_machine,
                                   FLAGS_simulated_quincy_replication_factor,
                                   file_block_distn,
                                   FLAGS_simulated_quincy_random_seed);
  return new SimulatedQuincyCostModel(
      resource_map_, job_map_, task_map_, &task_bindings_, leaf_res_ids,
      knowledge_base_, dfs, runtime_distn, input_block_distn,
      FLAGS_simulated_quincy_delta_preferred_machine,
      FLAGS_simulated_quincy_delta_preferred_rack,
      FLAGS_simulated_quincy_core_transfer_cost,
      FLAGS_simulated_quincy_tor_transfer_cost,
      FLAGS_simulated_quincy_input_percent_over_tolerance,
      FLAGS_simulated_quincy_machines_per_rack);
}

GoogleTraceSimulator::~GoogleTraceSimulator() {
  for (ResourceMap_t::iterator it = resource_map_->begin();
       it != resource_map_->end(); ) {
    ResourceMap_t::iterator it_tmp = it;
    ++it;
    delete it_tmp->second;
  }
  delete quincy_dispatcher_;
  delete knowledge_base_;
}

void GoogleTraceSimulator::Run() {
  FLAGS_add_root_task_to_graph = false;
  FLAGS_flow_scheduling_strict = true;

  const uint32_t MAX_VALUE = UINT32_MAX;
  proportion_to_retain_ = (FLAGS_percentage / 100.0) * MAX_VALUE;
  VLOG(2) << "Retaining events with hash < " << proportion_to_retain_;

  CHECK(FLAGS_batch_step != 0 || FLAGS_online_factor != 0.0)
                        << "must specify one of -batch_step or -online_factor.";
  CHECK(FLAGS_batch_step == 0 || FLAGS_online_factor == 0.0)
                       << "cannot specify both -batch_step and -online_factor.";

  if (!FLAGS_solver.compare("flowlessly")) {
    FLAGS_incremental_flow = FLAGS_run_incremental_scheduler;
    FLAGS_flow_scheduling_solver = "flowlessly";
    FLAGS_only_read_assignment_changes = true;
    FLAGS_flow_scheduling_binary =
        SOLVER_DIR "/flowlessly-git/run_fast_cost_scaling";
  } else if (!FLAGS_solver.compare("cs2")) {
    FLAGS_incremental_flow = false;
    FLAGS_flow_scheduling_solver = "cs2";
    FLAGS_only_read_assignment_changes = false;
    FLAGS_flow_scheduling_binary = SOLVER_DIR "/cs2-4.6/cs2.exe";
  } else if (!FLAGS_solver.compare("custom")) {
    FLAGS_flow_scheduling_solver = "custom";
    FLAGS_flow_scheduling_time_reported = true;
  } else {
    LOG(FATAL) << "Unknown solver type: " << FLAGS_solver;
  }

  // command line argument sanity checking
  if (trace_path_.empty()) {
    LOG(FATAL) << "Please specify a path to the Google trace!";
  }

  LOG(INFO) << "Starting Google trace simulator!";
  LOG(INFO) << "Time to simulate for: " << FLAGS_runtime << " microseconds.";
  LOG(INFO) << "Number of events to process: " << FLAGS_max_events;
  LOG(INFO) << "Number of scheduling rounds: " << FLAGS_max_scheduling_rounds;

  CreateRootResource();

  if (FLAGS_tasks_preemption_bins) {
    ofstream out_file(FLAGS_output_dir + "/" + FLAGS_task_bins_output);
    if (out_file.is_open()) {
      BinTasksByEventType(EVICT_EVENT, out_file);
      out_file.close();
    } else {
      LOG(ERROR) << "Could not open bin output file.";
    }
  } else {
    ofstream *stats_file = NULL;
    if (!FLAGS_stats_file.empty()) {
      stats_file = new ofstream(FLAGS_stats_file);
      if (stats_file->fail()) {
        LOG(FATAL) << "Could not open for writing stats file "
                   << FLAGS_stats_file;
      }
    }

    graph_output_ = NULL;
    if (!FLAGS_graph_output_file.empty()) {
      graph_output_ = fopen(FLAGS_graph_output_file.c_str(), "w");
      if (!graph_output_) {
        LOG(FATAL) << "Could not open for writing graph file "
                   << FLAGS_graph_output_file
                   << ", error: " << strerror(errno);
      }
    }

    ReplayTrace(stats_file);

    if (graph_output_) {
      fclose(graph_output_);
    }
    if (stats_file) {
      delete stats_file;
    }
  }
}

ResourceDescriptor* GoogleTraceSimulator::AddMachine(
    const ResourceTopologyNodeDescriptor& machine_tmpl, uint64_t machine_id) {
  // Create a new machine topology descriptor.
  ResourceTopologyNodeDescriptor* new_machine = rtn_root_.add_children();
  new_machine->CopyFrom(machine_tmpl);
  const string& root_uuid = rtn_root_.resource_desc().uuid();
  char hn[100];
  snprintf(hn, sizeof(hn), "h%ju", machine_id);
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&GoogleTraceSimulator::ResetUuidAndAddResource,
                               this, _1, string(hn), root_uuid));
  ResourceDescriptor* rd = new_machine->mutable_resource_desc();
  rd->set_friendly_name(hn);
  // Add the node to the flow graph.
  flow_graph_->AddMachine(new_machine);
  // Add resource to the google machine_id to ResourceDescriptor* map.
  CHECK(InsertIfNotPresent(&machine_id_to_rd_, machine_id, rd));
  CHECK(InsertIfNotPresent(&machine_id_to_rtnd_, machine_id, new_machine));
  return rd;
}

TaskDescriptor* GoogleTraceSimulator::AddNewTask(
    const TaskIdentifier& task_identifier,
    unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* task_runtime) {
  JobDescriptor** jdpp = FindOrNull(job_id_to_jd_, task_identifier.job_id);
  JobDescriptor* jd_ptr;
  if (!jdpp) {
    // Add new job to the graph
    jd_ptr = PopulateJob(task_identifier.job_id);
    CHECK_NOTNULL(jd_ptr);
  } else {
    jd_ptr = *jdpp;
  }
  // Ignore task if it has already been added.
  // TODO(ionel): We should not ignore it.
  TaskDescriptor* td_ptr = NULL;
  if (FindOrNull(task_id_to_td_, task_identifier) == NULL) {
    td_ptr = AddTaskToJob(jd_ptr);
    td_ptr->set_binary(lexical_cast<string>(task_identifier.job_id));
    if (InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr)) {
      CHECK(InsertIfNotPresent(&task_id_to_identifier_,
                               td_ptr->uid(), task_identifier));
      // Add task to the google (job_id, task_index) to TaskDescriptor* map.
      CHECK(InsertIfNotPresent(&task_id_to_td_, task_identifier, td_ptr));
      // Update statistics used by cost models. This must be done PRIOR
      // to AddOrUpdateJobNodes, as costs computed in that step.
      AddTaskStats(task_identifier, task_runtime);
      // Update the job in the flow graph. This method also adds the new task to
      // the flow graph.
      flow_graph_->AddOrUpdateJobNodes(jd_ptr);
    } else {
      // TODO(ionel): We should handle duplicate task ids.
      LOG(WARNING) << "Duplicate task id: " << td_ptr->uid() << " for task "
                   << task_identifier.job_id << " "
                   << task_identifier.task_index;
     return NULL;
    }
  }
  return td_ptr;
}

void GoogleTraceSimulator::AddTaskEndEvent(
    uint64_t cur_timestamp,
    const TaskID_t& task_id,
    TaskIdentifier task_identifier,
    unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* task_rnt) {
  uint64_t* runtime_ptr = FindOrNull(*task_rnt, task_identifier);
  EventDescriptor event_desc;
  event_desc.set_job_id(task_identifier.job_id);
  event_desc.set_task_index(task_identifier.task_index);
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  if (runtime_ptr != NULL) {
    // We can approximate the duration of the task.
    events_.insert(pair<uint64_t, EventDescriptor>(cur_timestamp + *runtime_ptr,
                                                   event_desc));
    CHECK(InsertIfNotPresent(&task_id_to_end_time_, task_id,
                             cur_timestamp + *runtime_ptr));
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // last timestamp of the simulation.
    events_.insert(pair<uint64_t, EventDescriptor>(FLAGS_runtime, event_desc));
    CHECK(InsertIfNotPresent(&task_id_to_end_time_, task_id, FLAGS_runtime));
  }
}

void GoogleTraceSimulator::AddTaskStats(
    TaskIdentifier task_identifier,
    unordered_map<TaskIdentifier, uint64_t,
      TaskIdentifierHasher>* task_runtime) {
  TaskStats* task_stats = FindOrNull(task_id_to_stats_, task_identifier);
  if (task_stats == NULL) {
    // Already added stats.
    VLOG(2) << "Skipping setting runtime for "
            << task_identifier.job_id << "/" << task_identifier.task_index;
    return;
  }
  TaskDescriptor* td_ptr = FindPtrOrNull(task_id_to_td_, task_identifier);
  CHECK_NOTNULL(td_ptr);
  uint64_t* task_runtime_ptr = FindOrNull(*task_runtime, task_identifier);
  double runtime = 0.0;
  if (task_runtime_ptr != NULL) {
    // knowledge base has runtime in ms, but we get it in micros
    runtime = *task_runtime_ptr / 1000.0;
  } else {
    // We don't have information about this task's runtime.
    // Set its runtime to max which means it's a service task.
    // TODO(adam): or float infinity?
    runtime = numeric_limits<uint64_t>::max();
  }
  vector<EquivClass_t>* task_equiv_classes =
    cost_model_->GetTaskEquivClasses(td_ptr->uid());
  CHECK_NOTNULL(task_equiv_classes);
  // XXX(malte): This uses the task ID as an additional implicit EC, so that
  // statistics are recorded on a per-task basis. Instead, what we want to do is
  // to record them for each of the task's equivalence classes, plus possibly
  // for the task ID, if the cost model requires per-task record keeping.
  //
  // Note that we might also need support for recording statistics under
  // different equivalence classes than we use in the flow graph. This isn't
  // currently supported.
  EquivClass_t bogus_equiv_class = static_cast<EquivClass_t>(td_ptr->uid());
  task_equiv_classes->push_back(bogus_equiv_class);
  // Add statistics to all relevant ECs
  for (vector<EquivClass_t>::iterator it = task_equiv_classes->begin();
       it != task_equiv_classes->end();
       ++it) {
    VLOG(2) << "Setting runtime of " << runtime << " for "
            << task_identifier.job_id << "/" << task_identifier.task_index;
    knowledge_base_->SetAvgRuntimeForTEC(*it, runtime);
    if (fabs(task_stats->avg_mean_cpu_usage + 1.0) > EPS) {
      knowledge_base_->SetAvgMeanCpuUsage(*it, task_stats->avg_mean_cpu_usage);
    }
    if (fabs(task_stats->avg_canonical_mem_usage + 1.0) > EPS) {
      knowledge_base_->SetAvgCanonicalMemUsage(
          *it, task_stats->avg_canonical_mem_usage);
    }
    if (fabs(task_stats->avg_assigned_mem_usage + 1.0) > EPS) {
      knowledge_base_->SetAvgAssignedMemUsage(
          *it, task_stats->avg_assigned_mem_usage);
    }
    if (fabs(task_stats->avg_unmapped_page_cache + 1.0) > EPS) {
      knowledge_base_->SetAvgUnmappedPageCache(
          *it, task_stats->avg_unmapped_page_cache);
    }
    if (fabs(task_stats->avg_total_page_cache + 1.0) > EPS) {
      knowledge_base_->SetAvgTotalPageCache(
          *it, task_stats->avg_total_page_cache);
    }
    if (fabs(task_stats->avg_mean_disk_io_time + 1.0) > EPS) {
      knowledge_base_->SetAvgMeanDiskIOTime(
          *it, task_stats->avg_mean_disk_io_time);
    }
    if (fabs(task_stats->avg_mean_local_disk_used + 1.0) > EPS) {
      knowledge_base_->SetAvgMeanLocalDiskUsed(
          *it, task_stats->avg_mean_local_disk_used);
    }
    if (fabs(task_stats->avg_cpi + 1.0) > EPS) {
      knowledge_base_->SetAvgCPIForTEC(*it, task_stats->avg_cpi);
    }
    if (fabs(task_stats->avg_mai + 1.0) > EPS) {
      knowledge_base_->SetAvgIPMAForTEC(*it, 1.0 / task_stats->avg_mai);
    }
    task_id_to_stats_.erase(task_identifier);
  }
}

void GoogleTraceSimulator::RemoveTaskStats(TaskID_t task_id) {
  vector<EquivClass_t>* task_equiv_classes =
    cost_model_->GetTaskEquivClasses(task_id);
  CHECK_NOTNULL(task_equiv_classes);
  // XXX(malte): This uses the task ID as an additional implicit EC, so that
  // statistics are recorded on a per-task basis. Instead, what we want to do is
  // to record them for each of the task's equivalence classes, plus possibly
  // for the task ID, if the cost model requires per-task record keeping.
  //
  // Note that we might also need support for recording statistics under
  // different equivalence classes than we use in the flow graph. This isn't
  // currently supported.
  EquivClass_t bogus_equiv_class = static_cast<EquivClass_t>(task_id);
  task_equiv_classes->push_back(bogus_equiv_class);
  // Add statistics to all relevant ECs
  for (vector<EquivClass_t>::iterator it = task_equiv_classes->begin();
       it != task_equiv_classes->end();
       ++it) {
    knowledge_base_->EraseStats(*it);
  }
}

TaskDescriptor* GoogleTraceSimulator::AddTaskToJob(JobDescriptor* jd_ptr) {
  CHECK_NOTNULL(jd_ptr);
  TaskDescriptor* root_task = jd_ptr->mutable_root_task();
  TaskDescriptor* new_task = root_task->add_spawned();
  new_task->set_uid(GenerateTaskID(*root_task));
  new_task->set_state(TaskDescriptor::RUNNABLE);
  new_task->set_job_id(jd_ptr->uuid());
  return new_task;
}

void GoogleTraceSimulator::BinTasksByEventType(uint64_t event,
                                               ofstream& out_file) {
  char line[200];
  vector<string> vals;
  FILE* fptr = NULL;
  uint64_t time_interval_bound = FLAGS_bin_time_duration;
  uint64_t num_tasks = 0;
  for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
       file_num++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05d-of-00500.csv",
        trace_path_.c_str(), file_num);
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
              out_file << "(" << time_interval_bound - FLAGS_bin_time_duration
                       << ", " << time_interval_bound << "]: "
                       << num_tasks << "\n";
              time_interval_bound += FLAGS_bin_time_duration;
              while (time_interval_bound < task_time) {
                out_file << "(" << time_interval_bound - FLAGS_bin_time_duration
                         << ", " << time_interval_bound << "]: 0\n";
                time_interval_bound += FLAGS_bin_time_duration;
              }
              num_tasks = 1;
            }
          }
        }
      }
    }
    fclose(fptr);
  }
  out_file << "(" << time_interval_bound - FLAGS_bin_time_duration << ", " <<
    time_interval_bound << "]: " << num_tasks << "\n";
}

void GoogleTraceSimulator::CreateRootResource() {
  ResourceID_t root_uuid = GenerateRootResourceID("XXXgoogleXXX");
  ResourceDescriptor* rd = rtn_root_.mutable_resource_desc();
  rd->set_uuid(to_string(root_uuid));
  LOG(INFO) << "Root res ID is " << to_string(root_uuid);
  CHECK(InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                           to_string(root_uuid)));

  // Add resources and job to flow graph
  flow_graph_->AddResourceTopology(&rtn_root_);
  CHECK(InsertIfNotPresent(resource_map_.get(), root_uuid,
                           new ResourceStatus(rd, &rtn_root_, "endpoint_uri",
                                              GetCurrentTimestamp())));
}

void GoogleTraceSimulator::JobCompleted(uint64_t simulator_job_id,
                                        JobID_t job_id) {
  flow_graph_->JobCompleted(job_id);
  // This call frees the JobDescriptor* as well.
  job_map_->erase(job_id);
  job_id_to_jd_.erase(simulator_job_id);
  job_num_tasks_.erase(simulator_job_id);
}

void GoogleTraceSimulator::LoadJobsNumTasks() {
  char line[200];
  vector<string> cols;
  FILE* jobs_tasks_file = NULL;
  string jobs_tasks_file_name = trace_path_ +
    "/jobs_num_tasks/jobs_num_tasks.csv";
  if ((jobs_tasks_file = fopen(jobs_tasks_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open jobs num tasks file.";
  }
  int64_t num_line = 1;
  while (!feof(jobs_tasks_file)) {
    if (fscanf(jobs_tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
      if (cols.size() != 2) {
        LOG(ERROR) << "Unexpected structure of jobs num tasks row on line: "
                   << num_line;
      } else {
        uint64_t job_id = lexical_cast<uint64_t>(cols[0]);
        uint64_t num_tasks = lexical_cast<uint64_t>(cols[1]);
        CHECK(InsertIfNotPresent(&job_num_tasks_, job_id, num_tasks));
      }
    }
    num_line++;
  }
  fclose(jobs_tasks_file);
}

void GoogleTraceSimulator::LoadMachineEvents() {
  char line[200];
  vector<string> cols;
  FILE* machines_file;
  string machines_file_name = trace_path_ +
    "/machine_events/part-00000-of-00001.csv";
  if ((machines_file = fopen(machines_file_name.c_str(), "r")) == NULL) {
    LOG(ERROR) << "Failed to open trace for reading machine events.";
  }

  int64_t num_line = 1;
  while (!feof(machines_file)) {
    if (fscanf(machines_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(","), token_compress_off);
      if (cols.size() != 6) {
        LOG(ERROR) << "Unexpected structure of machine events on line "
                   << num_line << ": found " << cols.size() << " columns.";
      } else {
      	uint64_t timestamp = lexical_cast<uint64_t>(cols[0]);
      	if (timestamp > FLAGS_runtime) {
      		// only load the events that we need
      		break;
      	}

        // schema: (timestamp, machine_id, event_type, platform, CPUs, Memory)
        uint64_t machine_id = lexical_cast<uint64_t>(cols[1]);
        // Sub-sample the trace if we only retain < 100% of machines.
        if (SpookyHash::Hash32(&machine_id, sizeof(machine_id), SEED)
            > proportion_to_retain_) {
          // skip event
          continue;
        }

        EventDescriptor event_desc;
        event_desc.set_machine_id(lexical_cast<uint64_t>(cols[1]));
        event_desc.set_type(TranslateMachineEvent(
            lexical_cast<int32_t>(cols[2])));
        if (event_desc.type() == EventDescriptor::REMOVE_MACHINE ||
            event_desc.type() == EventDescriptor::ADD_MACHINE) {
          events_.insert(pair<uint64_t, EventDescriptor>(timestamp,
                                                         event_desc));
        } else {
          // TODO(ionel): Handle machine update events.
        }
      }
    }
    num_line++;
  }
  fclose(machines_file);
}

void GoogleTraceSimulator::LoadTaskRuntimeStats() {
  char line[1000];
  vector<string> cols;
  FILE* usage_file = NULL;
  string usage_file_name = trace_path_ +
    "/task_usage_stat/task_usage_stat.csv";
  if ((usage_file = fopen(usage_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace task runtime stats file.";
  }
  int64_t num_line = 1;
  while (!feof(usage_file)) {
    if (fscanf(usage_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
      if (cols.size() != 38) {
        LOG(WARNING) << "Malformed task usage, " << cols.size()
                     << " != 38 columns at line " << num_line;
      } else {
        TaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);
        TaskStats task_stats;
        task_stats.avg_mean_cpu_usage = lexical_cast<double>(cols[4]);
        task_stats.avg_canonical_mem_usage = lexical_cast<double>(cols[8]);
        task_stats.avg_assigned_mem_usage = lexical_cast<double>(cols[12]);
        task_stats.avg_unmapped_page_cache = lexical_cast<double>(cols[16]);
        task_stats.avg_total_page_cache = lexical_cast<double>(cols[20]);
        task_stats.avg_mean_disk_io_time = lexical_cast<double>(cols[24]);
        task_stats.avg_mean_local_disk_used = lexical_cast<double>(cols[28]);
        task_stats.avg_cpi = lexical_cast<double>(cols[32]);
        task_stats.avg_mai = lexical_cast<double>(cols[36]);

        if (!InsertIfNotPresent(&task_id_to_stats_, task_id, task_stats) &&
            VLOG_IS_ON(1)) {
          LOG(ERROR) << "LoadTaskRuntimeStats: There should not be more than an "
                     << "entry for job " << task_id.job_id
                     << ", task " << task_id.task_index;
        } else {
          VLOG(2) << "Loaded stats for "
                  << task_id.job_id << "/" << task_id.task_index;
        }

        // double min_mean_cpu_usage = lexical_cast<double>(cols[2]);
        // double max_mean_cpu_usage = lexical_cast<double>(cols[3]);
        // double sd_mean_cpu_usage = lexical_cast<double>(cols[5]);
        // double min_canonical_mem_usage = lexical_cast<double>(cols[6]);
        // double max_canonical_mem_usage = lexical_cast<double>(cols[7]);
        // double sd_canonical_mem_usage = lexical_cast<double>(cols[9]);
        // double min_assigned_mem_usage = lexical_cast<double>(cols[10]);
        // double max_assigned_mem_usage = lexical_cast<double>(cols[11]);
        // double sd_assigned_mem_usage = lexical_cast<double>(cols[13]);
        // double min_unmapped_page_cache = lexical_cast<double>(cols[14]);
        // double max_unmapped_page_cache = lexical_cast<double>(cols[15]);
        // double sd_unmapped_page_cache = lexical_cast<double>(cols[17]);
        // double min_total_page_cache = lexical_cast<double>(cols[18]);
        // double max_total_page_cache = lexical_cast<double>(cols[19]);
        // double sd_total_page_cache = lexical_cast<double>(cols[21]);
        // double min_mean_disk_io_time = lexical_cast<double>(cols[22]);
        // double max_mean_disk_io_time = lexical_cast<double>(cols[23]);
        // double sd_mean_disk_io_time = lexical_cast<double>(cols[25]);
        // double min_mean_local_disk_used = lexical_cast<double>(cols[26]);
        // double max_mean_local_disk_used = lexical_cast<double>(cols[27]);
        // double sd_mean_local_disk_used = lexical_cast<double>(cols[29]);
        // double min_cpi = lexical_cast<double>(cols[30]);
        // double max_cpi = lexical_cast<double>(cols[31]);
        // double sd_cpi = lexical_cast<double>(cols[33]);
        // double min_mai = lexical_cast<double>(cols[34]);
        // double max_mai = lexical_cast<double>(cols[35]);
        // double sd_mai = lexical_cast<double>(cols[37]);
      }
    }
    num_line++;
  }
  fclose(usage_file);
}

unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>*
    GoogleTraceSimulator::LoadTasksRunningTime() {
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher> *task_runtime =
    new unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>();
  char line[200];
  vector<string> cols;
  FILE* tasks_file = NULL;
  string tasks_file_name = trace_path_ +
    "/task_runtime_events/task_runtime_events.csv";
  if ((tasks_file = fopen(tasks_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace runtime events file.";
  }

  int64_t num_line = 1;
  while (!feof(tasks_file)) {
    if (fscanf(tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
      if (cols.size() != 13) {
        LOG(ERROR) << "Unexpected structure of task runtime row on line: "
                   << num_line;
      } else {
        TaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);

        // Sub-sample the trace if we only retain < 100% of tasks.
        if (SpookyHash::Hash32(&task_id, sizeof(task_id), SEED) >
            proportion_to_retain_) {
          // skip event
          continue;
        }

        uint64_t runtime = lexical_cast<uint64_t>(cols[4]);
        if (!InsertIfNotPresent(task_runtime, task_id, runtime) &&
            VLOG_IS_ON(1)) {
          LOG(ERROR) << "LoadTasksRunningTime: There should not be more than "
                     << "one entry for job " << task_id.job_id
                     << ", task " << task_id.task_index;
        } else {
          VLOG(2) << "Loaded runtime for "
                  << task_id.job_id << "/" << task_id.task_index;
        }
      }
    }
    num_line++;
  }
  fclose(tasks_file);
  return task_runtime;
}

JobDescriptor* GoogleTraceSimulator::PopulateJob(uint64_t job_id) {
  JobDescriptor jd;
  // Generate a hash out of the trace job_id.
  JobID_t new_job_id = GenerateJobID(job_id);

  CHECK(InsertIfNotPresent(job_map_.get(), new_job_id, jd));
  // Get the new value of the pointer because jd has been copied.
  JobDescriptor* jd_ptr = FindOrNull(*job_map_, new_job_id);

  // Maintain a mapping between the trace job_id and the generated job_id.
  jd_ptr->set_uuid(to_string(new_job_id));
  InsertOrUpdate(&job_id_to_jd_, job_id, jd_ptr);
  TaskDescriptor* rt = jd_ptr->mutable_root_task();
  string bin;
  // XXX(malte): hack, should use logical job name
  spf(&bin, "%jd", job_id);
  rt->set_binary(bin);
  rt->set_uid(GenerateRootTaskID(*jd_ptr));
  rt->set_state(TaskDescriptor::RUNNABLE);
  return jd_ptr;
}

void GoogleTraceSimulator::SeenExogenous(uint64_t time) {
  first_exogenous_event_seen_ = std::min(time, first_exogenous_event_seen_);
}

uint64_t GoogleTraceSimulator::NextSimulatorEvent() {
  multimap<uint64_t, EventDescriptor>::iterator it = events_.begin();
  if (it == events_.end()) {
    // Empty collection.
    return UINT64_MAX;
  } else {
    return it->first;
  }
}

void GoogleTraceSimulator::ProcessSimulatorEvents(uint64_t cur_time,
                           const ResourceTopologyNodeDescriptor& machine_tmpl) {
  while (true) {
    multimap<uint64_t, EventDescriptor>::iterator it = events_.begin();
    if (it == events_.end()) {
      // Empty collection.
      break;
    }
    if (it->first > cur_time) {
      // Processed all events <= cur_time.
      break;
    }

    string log_string;
    if (it->second.type() == EventDescriptor::ADD_MACHINE) {
      spf(&log_string, "ADD_MACHINE %ju @ %ju\n", it->second.machine_id(),
          it->first);
      LogEvent(log_string);
      AddMachine(machine_tmpl, it->second.machine_id());
      SeenExogenous(it->first);
    } else if (it->second.type() == EventDescriptor::REMOVE_MACHINE) {
      spf(&log_string, "REMOVE_MACHINE %ju @ %ju\n", it->second.machine_id(),
          it->first);
      LogEvent(log_string);
      RemoveMachine(it->second.machine_id());
      SeenExogenous(it->first);
    } else if (it->second.type() == EventDescriptor::UPDATE_MACHINE) {
      // TODO(ionel): Handle machine update event.
    } else if (it->second.type() == EventDescriptor::TASK_END_RUNTIME) {
      spf(&log_string, "TASK_END_RUNTIME %s:%ju @ %ju\n", it->second.job_id(),
          it->second.task_index(), it->first);
      LogEvent(log_string);
      // Task has finished.
      TaskIdentifier task_identifier;
      task_identifier.task_index = it->second.task_index();
      task_identifier.job_id = it->second.job_id();
      TaskCompleted(task_identifier);
      SeenExogenous(it->first);
    } else if (it->second.type() == EventDescriptor::TASK_ASSIGNMENT_CHANGED) {
      // no-op: this event is just used to trigger solver re-run
    } else {
      LOG(ERROR) << "Unexpected event type " << it->second.type() << " @ " << it->first;
    }

    events_.erase(it);
  }
}

void GoogleTraceSimulator::ProcessTaskEvent(
    uint64_t cur_time, const TaskIdentifier& task_identifier,
    uint64_t event_type,
    unordered_map<TaskIdentifier, uint64_t,
      TaskIdentifierHasher>* task_runtime) {
  string log_string;
  if (event_type == SUBMIT_EVENT) {
    spf(&log_string, "TASK_SUBMIT_EVENT: ID %s:%ju @ %ju\n",
        task_identifier.job_id, task_identifier.task_index,
        cur_time);
    LogEvent(log_string);
    if (AddNewTask(task_identifier, task_runtime)) {
      SeenExogenous(cur_time);
    } else {
      // duplicate task id -- ignore
    }
  }
}

void GoogleTraceSimulator::RemoveMachine(uint64_t machine_id) {
  machine_id_to_rd_.erase(machine_id);
  ResourceTopologyNodeDescriptor* rtnd_ptr =
    FindPtrOrNull(machine_id_to_rtnd_, machine_id);
  CHECK_NOTNULL(rtnd_ptr);
  // Traverse the resource topology tree in order to evict tasks and
  // remove resources from resource_map.
  DFSTraverseResourceProtobufTreeReturnRTND(
      rtnd_ptr, boost::bind(&GoogleTraceSimulator::RemoveResource, this, _1));
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  flow_graph_->RemoveMachine(res_id);
  if (rtnd_ptr->has_parent_id()) {
    if (rtnd_ptr->parent_id().compare(rtn_root_.resource_desc().uuid()) == 0) {
      RepeatedPtrField<ResourceTopologyNodeDescriptor>* parent_children =
        rtn_root_.mutable_children();
      int32_t index = 0;
      for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator it =
             parent_children->begin(); it != parent_children->end();
           ++it, ++index) {
        if (it->resource_desc().uuid()
              .compare(rtnd_ptr->resource_desc().uuid()) == 0) {
          break;
        }
      }
      if (index < parent_children->size()) {
        // Found the node.
        if (index < parent_children->size() - 1) {
          // The node is not the last one.
          parent_children->SwapElements(index, parent_children->size() - 1);
        }
        parent_children->RemoveLast();
      } else {
        LOG(WARNING) << "Could not found the machine in the parent's list";
      }
    } else {
      LOG(ERROR) << "Machine " << machine_id
                 << " is not direclty connected to the root";
    }
  } else {
    LOG(WARNING) << "Machine " << machine_id << " doesn't have a parent";
  }
  machine_id_to_rtnd_.erase(machine_id);
  // We can't delete the node because we haven't removed it from it's parent
  // children list.
  // delete rtnd_ptr;
}

void GoogleTraceSimulator::RemoveResource(
    ResourceTopologyNodeDescriptor* rtnd) {
  ResourceID_t res_id = ResourceIDFromString(rtnd->resource_desc().uuid());
  TaskID_t* task_id = FindOrNull(res_id_to_task_id_, res_id);
  if (task_id != NULL) {
    // Evict the task running on the resource.
    TaskEvicted(*task_id, res_id);
  }
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs_ptr);
  resource_map_->erase(res_id);
  delete rs_ptr;
}

void GoogleTraceSimulator::TaskEvicted(TaskID_t task_id,
                                       const ResourceID_t& res_id) {
  VLOG(2) << "Evict task " << task_id << " from resource " << res_id;
  TaskDescriptor** td_ptr = FindOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // Change the state of the task from running to runnable.
  (*td_ptr)->set_state(TaskDescriptor::RUNNABLE);
  flow_graph_->NodeForTaskID(task_id)->type_.set_type(
      FlowNodeType::UNSCHEDULED_TASK);
  // Remove the running arc and add back arcs to EC and UNSCHED.
  flow_graph_->TaskEvicted(task_id, res_id);

  // Get the Google trace identifier of the task.
  TaskIdentifier* ti_ptr = FindOrNull(task_id_to_identifier_, task_id);
  CHECK_NOTNULL(ti_ptr);
  // Get the end time of the task.
  uint64_t* task_end_time = FindOrNull(task_id_to_end_time_, task_id);
  CHECK_NOTNULL(task_end_time);
  // Remove the task end time event from the simulator events_.
  pair<multimap<uint64_t, EventDescriptor>::iterator,
       multimap<uint64_t, EventDescriptor>::iterator> range_it =
    events_.equal_range(*task_end_time);
  for (; range_it.first != range_it.second; range_it.first++) {
    if (range_it.first->second.type() == EventDescriptor::TASK_END_RUNTIME &&
        range_it.first->second.job_id() == ti_ptr->job_id &&
        range_it.first->second.task_index() == ti_ptr->task_index) {
      break;
    }
  }
  ResourceID_t res_id_tmp = res_id;
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
  rd_ptr->clear_current_running_task();
  task_bindings_.erase(task_id);
  res_id_to_task_id_.erase(res_id_tmp);
  // Remove current task end time.
  task_id_to_end_time_.erase(task_id);
  // We've found the event.
  if (range_it.first != range_it.second) {
    events_.erase(range_it.first);
  }
}

void GoogleTraceSimulator::ResetUuidAndAddResource(
    ResourceTopologyNodeDescriptor* rtnd, const string& hostname,
    const string& root_uuid) {
  string new_uuid;
  if (rtnd->has_parent_id()) {
    // This is an intermediate node, so translate the parent UUID via the
    // lookup table
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
  VLOG(2) << "Resetting UUID for " << rtnd->resource_desc().uuid() << " to "
          << new_uuid;
  InsertOrUpdate(&uuid_conversion_map_, rtnd->resource_desc().uuid(), new_uuid);
  ResourceDescriptor* rd = rtnd->mutable_resource_desc();
  rd->set_uuid(new_uuid);
  // Add the resource node to the map.
  CHECK(InsertIfNotPresent(resource_map_.get(),
                           ResourceIDFromString(rd->uuid()),
                           new ResourceStatus(rd, rtnd, "endpoint_uri",
                                              GetCurrentTimestamp())));
}

const string CHANGE_STATS_HEADER =
             "total_changes,new_node,remove_node,new_arc,change_arc,remove_arc";

void OutputChangeStats(DIMACSChangeStats &stats, ofstream &stats_file) {
  stats_file << stats.total << "," << stats.new_node << ","
             << stats.remove_node << "," << stats.new_arc << ","
             << stats.change_arc << "," << stats.remove_arc
             << std::endl;
}

int get_binary_directory(char *pBuf, ssize_t len) {
  char szTmp[32];
  sprintf(szTmp, "/proc/%d/exe", getpid());
  int bytes = std::min(readlink(szTmp, pBuf, len), len - 1);
  if(bytes >= 0)
    pBuf[bytes] = '\0';
  return bytes;
}

void GoogleTraceSimulator::ReplayTrace(ofstream *stats_file) {
  // Output CSV header
  if (stats_file) {
    if (FLAGS_batch_step == 0) {
      // online
      *stats_file << "cluster_timestamp,scheduling_latency,algorithm_time,"
                  << "flowsolver_time,total_time,";
    } else {
      // batch
      *stats_file << "cluster_timestamp,algorithm_time,flowsolver_time,total_time,";
    }
    *stats_file << CHANGE_STATS_HEADER << std::endl;
  }

  // timing
  boost::timer::cpu_timer timer;
  timeout_file = stats_file;
  signal(SIGALRM, alarm_handler);

  // Load all the machine events.
  LoadMachineEvents();
  // Populate the job_id to number of tasks mapping.
  LoadJobsNumTasks();
  // Load tasks' runtime.
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* task_runtime =
    LoadTasksRunningTime();
  // Populate the knowledge base.
  LoadTaskRuntimeStats();

  // Import a fictional machine resource topology
  boost::filesystem::path machine_tmpl_path(FLAGS_machine_tmpl_file);
  if (machine_tmpl_path.is_relative()) {
    // lookup file relative to directory of binary, not CWD
    char binary_path[1024];
    size_t bytes = get_binary_directory(binary_path, sizeof(binary_path));
    CHECK(bytes < sizeof(binary_path));
    boost::filesystem::path binary_path_boost(binary_path);
    binary_path_boost.remove_filename();

    machine_tmpl_path = binary_path_boost / machine_tmpl_path;
  }

  ResourceTopologyNodeDescriptor machine_tmpl;
  std::string machine_tmpl_fname(machine_tmpl_path.string());
  LOG(INFO) << "Loading machine descriptor from " << machine_tmpl_fname;
  int fd = open(machine_tmpl_fname.c_str(), O_RDONLY);
  if (fd < 0) {
    PLOG(FATAL) << "Could not load " << machine_tmpl_fname;
  }
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);

  char line[200];
  vector<string> vals;
  FILE* f_task_events_ptr = NULL;
  uint64_t time_interval_bound = 0;
  uint64_t num_events = 0;
  uint64_t num_scheduling_rounds = 0;
  first_exogenous_event_seen_ = UINT64_MAX;

  for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
       file_num++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05d-of-00500.csv", trace_path_.c_str(),
        file_num);
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
          TaskIdentifier task_id;
          uint64_t task_time = lexical_cast<uint64_t>(vals[0]);
          task_id.job_id = lexical_cast<uint64_t>(vals[2]);
          task_id.task_index = lexical_cast<uint64_t>(vals[3]);

          // Sub-sample the trace if we only retain < 100% of tasks.
          if (SpookyHash::Hash32(&task_id, sizeof(task_id), SEED)
              > proportion_to_retain_) {
            // skip event
            continue;
          }

          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);

          // We only run for the first FLAGS_runtime microseconds.
          if (FLAGS_runtime < task_time) {
            LOG(INFO) << "Terminating at : " << task_time;
            return;
          }

          num_events++;
          if (num_events > FLAGS_max_events) {
            LOG(INFO) << "Terminating after " << num_events << " events";
            return;
          }

          VLOG(2) << "TASK EVENT @ " << task_time;

          while (task_time > time_interval_bound) {
            ProcessSimulatorEvents(time_interval_bound, machine_tmpl);

            if (!flow_graph_->graph_changes().empty()) {
              // Only run solver if something has actually changed.
              // (Sometimes, all the events we received in a time interval
              // have been ignored, e.g. task submit events with duplicate IDs.)
              VLOG(2) << "Job id size: " << job_id_to_jd_.size();
              VLOG(2) << "Task id size: " << task_id_to_identifier_.size();
              VLOG(2) << "Job num tasks size: " << job_num_tasks_.size();
              VLOG(2) << "Job id to jd size: " << job_map_->size();
              VLOG(2) << "Task id to td size: " << task_map_->size();
              VLOG(2) << "Res id to rd size: " << resource_map_->size();
              VLOG(2) << "Task binding: " << task_bindings_.size();

              LOG(INFO) << "Scheduler run for time: " << time_interval_bound;
              LOG(INFO) << "Nodes: " << flow_graph_->NumNodes()
                        << ", arcs: " << flow_graph_->NumArcs();

              if (graph_output_) {
                fprintf(graph_output_, "c SOI %lu\n", time_interval_bound);
                fflush(graph_output_);
              }
              double algorithm_time, flowsolver_time;

              DIMACSChangeStats change_stats(flow_graph_->graph_changes());
              alarm(FLAGS_solver_timeout);
              multimap<uint64_t, uint64_t>* task_mappings =
                quincy_dispatcher_->Run(&algorithm_time, &flowsolver_time,
                                        graph_output_);
              alarm(0);

              UpdateFlowGraph(time_interval_bound, task_runtime, task_mappings);

              delete task_mappings;

              if (FLAGS_flow_scheduling_cost_model ==
                  FlowSchedulingCostModelType::COST_MODEL_COCO ||
                  FLAGS_flow_scheduling_cost_model ==
                  FlowSchedulingCostModelType::COST_MODEL_OCTOPUS ||
                  FLAGS_flow_scheduling_cost_model ==
                  FlowSchedulingCostModelType::COST_MODEL_WHARE) {
                flow_graph_->ComputeTopologyStatistics(
                    flow_graph_->sink_node(),
                    boost::bind(&FlowSchedulingCostModelInterface::GatherStats,
                                cost_model_, _1, _2));
                flow_graph_->ComputeTopologyStatistics(
                    flow_graph_->sink_node(),
                    boost::bind(&FlowSchedulingCostModelInterface::UpdateStats,
                                cost_model_, _1, _2));
              } else {
                LOG(INFO) << "No resource stats update required";
              }

              // Log stats to CSV file
              if (stats_file) {
                boost::timer::cpu_times total_runtime = timer.elapsed();
                boost::timer::nanosecond_type second = 1000*1000*1000;
                double total_runtime_float = total_runtime.wall;
                total_runtime_float /= second;
                if (FLAGS_batch_step == 0) {
                  // online mode

                  double scheduling_latency = time_interval_bound;
                  scheduling_latency += algorithm_time * 1000 * 1000;
                  scheduling_latency -= first_exogenous_event_seen_;
                  scheduling_latency /= (1000 * 1000);

                  // will be negative if we have not seen any exogeneous event
                  scheduling_latency = std::max(0.0, scheduling_latency);

                  *stats_file << time_interval_bound << ","
                              << scheduling_latency << ","
                              << algorithm_time << ","
                              << flowsolver_time << ","
                              << total_runtime_float << ",";
                } else {
                  // batch mode
                  *stats_file << time_interval_bound << ","
                              << algorithm_time << ","
                              << flowsolver_time << ","
                              << total_runtime_float << ",";
                }

                OutputChangeStats(change_stats, *stats_file);
                stats_file->flush();
              }
              // restart timer; elapsed() returns time from this point
              timer.stop(); timer.start();

              // Update current time.
              if (FLAGS_batch_step == 0) {
                // we're in online mode

                // 1. when we run the solver next depends on how fast we were
                double time_to_solve =
                                std::min(algorithm_time, FLAGS_online_max_time);
                time_to_solve *= 1000 * 1000; // to micro
                // adjust for time warp factor
                time_to_solve *= FLAGS_online_factor;
                time_interval_bound += (uint64_t)time_to_solve;
                // 2. if task assignments changed, then graph will have been
                // modified, even in the absence of any new events.
                // Incremental solvers will want to rerun here, as it reduces
                // latency. But we shouldn't count it as an actual iteration.
                // Full solvers will not want to rerun: no point.
                if (FLAGS_incremental_flow) {
                  EventDescriptor event;
                  event.set_type(EventDescriptor::TASK_ASSIGNMENT_CHANGED);
                  events_.insert(make_pair(time_interval_bound, event));
                }
              } else {
                // we're in batch mode
                time_interval_bound += FLAGS_batch_step;
              }

              num_scheduling_rounds++;
              if (num_scheduling_rounds >= FLAGS_max_scheduling_rounds) {
                LOG(INFO) << "Terminating after " << num_scheduling_rounds
                          << " scheduling rounds.";
                return;
              }
            }

            VLOG(1) << "Updated time interval bound to " << time_interval_bound;
            // skip time until the next event happens
            uint64_t next_event = std::min(task_time, NextSimulatorEvent());
            VLOG(1) << "Next event at " << next_event;
            time_interval_bound = std::max(next_event, time_interval_bound);
            VLOG(1) << "Time interval bound to " << time_interval_bound;

            first_exogenous_event_seen_ = UINT64_MAX;
          }

          ProcessSimulatorEvents(task_time, machine_tmpl);
          ProcessTaskEvent(task_time, task_id, event_type, task_runtime);
          first_exogenous_event_seen_ =
                               std::min(first_exogenous_event_seen_, task_time);
        }
      }
    }
    fclose(f_task_events_ptr);
  }
  delete task_runtime;
}

void GoogleTraceSimulator::TaskCompleted(
    const TaskIdentifier& task_identifier) {
  TaskDescriptor** td_ptr = FindOrNull(task_id_to_td_, task_identifier);
  if (td_ptr == NULL) {
    LOG(ERROR) << "Could not find TaskDescriptor for: "
               << task_identifier.job_id
               << " " << task_identifier.task_index;
    // TODO(ionel): This may have to update the state.
    return;
  }
  TaskID_t task_id = (*td_ptr)->uid();
  JobID_t job_id = JobIDFromString((*td_ptr)->job_id());
  // Remove the task node from the flow graph.
  flow_graph_->TaskCompleted(task_id);
  // Erase from knowledge base
  RemoveTaskStats(task_id);
  // Erase from local state: task_id_to_td_, task_id_to_identifier_, task_map_
  // and task_bindings_, task_id_to_end_time_, res_id_to_task_id_.
  task_id_to_td_.erase(task_identifier);
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, task_id);
  CHECK_NOTNULL(res_id_ptr);
  ResourceID_t res_id_tmp = *res_id_ptr;
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
  res_id_to_task_id_.erase(res_id_tmp);
  rd_ptr->clear_current_running_task();
  task_bindings_.erase(task_id);
  task_map_->erase(task_id);
  task_id_to_identifier_.erase(task_id);
  task_id_to_end_time_.erase(task_id);
  uint64_t* num_tasks = FindOrNull(job_num_tasks_, task_identifier.job_id);
  CHECK_NOTNULL(num_tasks);
  (*num_tasks)--;
  if (*num_tasks == 0) {
    JobCompleted(task_identifier.job_id, job_id);
  }
}

EventDescriptor_EventType GoogleTraceSimulator::TranslateMachineEvent(
    int32_t machine_event) {
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

void GoogleTraceSimulator::UpdateFlowGraph(
    uint64_t scheduling_timestamp,
    unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* task_runtime,
    multimap<uint64_t, uint64_t>* task_mappings) {
  set<ResourceID_t> pus_used;
  vector<SchedulingDelta*> deltas;
  for (multimap<uint64_t, uint64_t>::iterator it = task_mappings->begin();
       it != task_mappings->end(); ++it) {
    // Some sanity checks
    FlowGraphNode* src = flow_graph_->Node(it->first);
    FlowGraphNode* dst = flow_graph_->Node(it->second);
    // Source must be a task node as this point
    CHECK(src->type_.type() == FlowNodeType::SCHEDULED_TASK ||
          src->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
          src->type_.type() == FlowNodeType::ROOT_TASK);
    // Destination must be a PU node
    CHECK(dst->type_.type() == FlowNodeType::PU);
    // XXX: what about unscheduled tasks?
    // Get the TD and RD for the source and destination
    TaskDescriptor* task = FindPtrOrNull(*task_map_, src->task_id_);
    CHECK_NOTNULL(task);
    ResourceStatus* target_res_status =
      FindPtrOrNull(*resource_map_, dst->resource_id_);
    CHECK_NOTNULL(target_res_status);
    const ResourceDescriptor& resource = target_res_status->descriptor();
    // Populate the scheduling delta.
    quincy_dispatcher_->NodeBindingToSchedulingDelta(
        *task, resource, &task_bindings_, &deltas);
  }
  for (vector<SchedulingDelta*>::const_iterator it = deltas.begin();
       it != deltas.end(); ++it) {
    SchedulingDelta* delta = *it;
    if (delta->type() == SchedulingDelta::NOOP) {
      // We don't have to do anything.
      continue;
    } else if (delta->type() == SchedulingDelta::PLACE) {
      // Apply the scheduling delta.
      TaskID_t task_id = delta->task_id();
      ResourceID_t res_id = ResourceIDFromString(delta->resource_id());
      // Mark the task as scheduled
      FlowGraphNode* node = flow_graph_->NodeForTaskID(delta->task_id());
      CHECK_NOTNULL(node);
      node->type_.set_type(FlowNodeType::SCHEDULED_TASK);
      TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
      CHECK_NOTNULL(td);
      CHECK_NOTNULL(rs);
      ResourceDescriptor* rd = rs->mutable_descriptor();
      rd->set_state(ResourceDescriptor::RESOURCE_BUSY);
      pus_used.insert(res_id);
      td->set_state(TaskDescriptor::RUNNING);
      rd->set_current_running_task(task_id);
      CHECK(InsertIfNotPresent(&task_bindings_, task_id, res_id));
      CHECK(InsertIfNotPresent(&res_id_to_task_id_, res_id, task_id));
      // Unless FLAGS_preemption is set, all edges are removed except into
      // the bound resource, disabling preemption and migration
      flow_graph_->TaskScheduled(task_id, res_id);
      TaskIdentifier* task_identifier =
        FindOrNull(task_id_to_identifier_, task_id);
      CHECK_NOTNULL(task_identifier);
      AddTaskEndEvent(scheduling_timestamp, task_id, *task_identifier,
                      task_runtime);
    } else if (delta->type() == SchedulingDelta::PREEMPT) {
      // Apply the scheduling delta.
      TaskID_t old_task_id = delta->task_id();
      ResourceID_t res_id = ResourceIDFromString(delta->resource_id());
      // Mark the old task as unscheduled
      // XXX: Does this do everything?
      TaskEvicted(old_task_id, res_id);
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
      CHECK_NOTNULL(rs);
      ResourceDescriptor* rd = rs->mutable_descriptor();
      rd->set_state(ResourceDescriptor::RESOURCE_IDLE);
    } else if (delta->type() == SchedulingDelta::MIGRATE) {
      // Apply the scheduling delta.
      TaskID_t task_id = delta->task_id();
      ResourceID_t res_id = ResourceIDFromString(delta->resource_id());
      // Mark the task as scheduled
      FlowGraphNode* node = flow_graph_->NodeForTaskID(delta->task_id());
      CHECK_NOTNULL(node);
      node->type_.set_type(FlowNodeType::SCHEDULED_TASK);
      TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
      CHECK_NOTNULL(td);
      CHECK_NOTNULL(rs);
      ResourceDescriptor* rd = rs->mutable_descriptor();
      rd->set_state(ResourceDescriptor::RESOURCE_BUSY);
      pus_used.insert(res_id);
      td->set_state(TaskDescriptor::RUNNING);
      ResourceID_t* old_res_id = FindOrNull(task_bindings_, task_id);
      if (old_res_id) {
        // could be null if we've already processed a PREEMPTION event for this
        if (pus_used.find(*old_res_id) == pus_used.end()) {
          // The resource is now idle.
          ResourceStatus* old_rs = FindPtrOrNull(*resource_map_, *old_res_id);
          CHECK_NOTNULL(old_rs);
          ResourceDescriptor* old_rd = old_rs->mutable_descriptor();
          old_rd->set_state(ResourceDescriptor::RESOURCE_IDLE);
          res_id_to_task_id_.erase(*old_res_id);
        }
      }
      rd->set_current_running_task(task_id);
      InsertOrUpdate(&task_bindings_, task_id, res_id);
      // may already be present if there's another migration/preemption event
      InsertOrUpdate(&res_id_to_task_id_, res_id, task_id);
    } else {
      LOG(FATAL) << "Unhandled scheduling delta case.";
    }
    delete delta;
  }
}

FlowGraphNode* GoogleTraceSimulator::GatherWhareMCStats(
    FlowGraphNode* accumulator, FlowGraphNode* other) {
  if (accumulator->type_.type() == FlowNodeType::ROOT_TASK ||
      accumulator->type_.type() == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_.type() == FlowNodeType::SINK) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    if (accumulator->type_.type() == FlowNodeType::PU) {
      // Base case. We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      CHECK_NOTNULL(rs_ptr);
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      if (rd_ptr->has_current_running_task()) {
        TaskDescriptor* td_ptr =
          FindPtrOrNull(*task_map_, rd_ptr->current_running_task());
        if (td_ptr->has_task_type()) {
          // TODO(ionel): Gather the statistics.
          WhareMapStats* wms_ptr = rd_ptr->mutable_whare_map_stats();
          if (td_ptr->task_type() == TaskDescriptor::DEVIL) {
            wms_ptr->set_num_devils(1);
          } else if (td_ptr->task_type() == TaskDescriptor::RABBIT) {
            wms_ptr->set_num_rabbits(1);
          } else if (td_ptr->task_type() == TaskDescriptor::SHEEP) {
            wms_ptr->set_num_sheep(1);
          } else if (td_ptr->task_type() == TaskDescriptor::TURTLE) {
            wms_ptr->set_num_turtles(1);
          } else {
            LOG(FATAL) << "Unexpected task type";
          }
        } else {
          LOG(WARNING) << "Task " << td_ptr->uid() << " does not have a type";
        }
      }
    }
    return accumulator;
  }
  if (accumulator->type_.type() == FlowNodeType::EQUIVALENCE_CLASS) {
    if (!other->resource_id_.is_nil() &&
        other->type_.type() == FlowNodeType::MACHINE) {
      // If the other node is a machine.
      //    AccumulateWhareMapStats(accumulator, other);
    }
    // TODO(ionel): Update knowledge base.
    return accumulator;
  }
  ResourceStatus* acc_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(acc_rs_ptr);
  WhareMapStats* wms_acc_ptr =
    acc_rs_ptr->mutable_descriptor()->mutable_whare_map_stats();
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  WhareMapStats* wms_other_ptr =
    other_rs_ptr->mutable_descriptor()->mutable_whare_map_stats();
  if (accumulator->type_.type() == FlowNodeType::MACHINE) {
    AccumulateWhareMapStats(wms_acc_ptr, wms_other_ptr);
    // TODO(ionel): Update knowledge base.
    return accumulator;
  }
  AccumulateWhareMapStats(wms_acc_ptr, wms_other_ptr);
  return accumulator;
}

void GoogleTraceSimulator::AccumulateWhareMapStats(
    WhareMapStats* accumulator, WhareMapStats* other) {
  accumulator->set_num_devils(accumulator->num_devils() +
                              other->num_devils());
  accumulator->set_num_rabbits(accumulator->num_rabbits() +
                               other->num_rabbits());
  accumulator->set_num_sheep(accumulator->num_sheep() +
                             other->num_sheep());
  accumulator->set_num_turtles(accumulator->num_turtles() +
                               other->num_turtles());
}

} // namespace sim
} // namespace firmament
