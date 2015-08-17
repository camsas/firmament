// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.
#include "sim/trace-extract/google_trace_simulator.h"

#include <signal.h>
#include <SpookyV2.h>
#include <sys/stat.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>
#include <cstdio>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/flow_graph_node.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "sim/trace-extract/google_trace_loader.h"
#include "sim/trace-extract/simulated_quincy_factory.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

DEFINE_int32(num_files_to_process, 500, "Number of files to process.");
DEFINE_uint64(runtime, 9223372036854775807,
              "Maximum time in microsec to extract data for"
              "(from start of trace)");
DEFINE_uint64(max_events, UINT64_MAX,
              "Maximum number of task events to process.");
DEFINE_uint64(max_scheduling_rounds, UINT64_MAX,
              "Maximum number of scheduling rounds to run for.");
DEFINE_double(events_fraction, 1.0, "Fraction of events to retain.");
DEFINE_uint64(batch_step, 0, "Batch mode: time interval to run solver "
              "at (in microseconds).");
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
DEFINE_bool(graph_output_events, true, "If -graph_output_file specified: "
                                       "export simulator events as comments?");

DEFINE_string(solver, "flowlessly",
              "Solver to use: flowlessly | cs2 | custom.");
DEFINE_uint64(solver_timeout, UINT64_MAX,
              "Timeout: terminate after waiting this number of seconds");
DEFINE_bool(run_incremental_scheduler, false,
            "Run the Flowlessly incremental scheduler.");
DEFINE_int32(flow_scheduling_cost_model, 0,
             "Flow scheduler cost model to use. "
             "Values: 0 = TRIVIAL, 1 = RANDOM, 2 = SJF, 3 = QUINCY, "
             "4 = WHARE, 5 = COCO, 6 = OCTOPUS, 7 = VOID, "
             "8 = SIMULATED QUINCY");

static bool ValidateBatchStep(const char* flagname, uint64_t batch_step) {
  if (batch_step == 0) {
    if (firmament::IsEqual(FLAGS_online_factor, 0.0)) {
      LOG(ERROR) << "must specify one of -batch_step or -online_factor";
      return false;
    }
    return true;
  } else {
    if (!firmament::IsEqual(FLAGS_online_factor, 0.0)) {
      LOG(ERROR) << "cannot specify both -batch_step and -online_factor";
      return false;
    }
    return true;
  }
}

static const bool batch_step_validator =
  google::RegisterFlagValidator(&FLAGS_batch_step, &ValidateBatchStep);

static bool ValidateSolver(const char* flagname, const string& solver) {
  if (solver.compare("cs2") && solver.compare("flowlessly") &&
      solver.compare("custom")) {
    LOG(ERROR) << "Solver can be one of: cs2, flowlessly or custom";
    return false;
  }
  return true;
}

static const bool solver_validator =
  google::RegisterFlagValidator(&FLAGS_solver, &ValidateSolver);

static bool ValidateRunIncremental(const char* flagname, bool run_incremental) {
  if (run_incremental && FLAGS_solver.compare("flowlessly")) {
    LOG(ERROR) << "run_incremental_scheduler can only be set with the "
               << "flowlessly solver";
    return false;
  }
  return true;
}

static const bool run_incremental_validator =
  google::RegisterFlagValidator(&FLAGS_run_incremental_scheduler,
                                &ValidateRunIncremental);

namespace firmament {
namespace sim {

// It varies a little over time, but relatively constant. Used for calculation
// of how much storage space we have.
static const uint64_t MACHINES_IN_TRACE_APPROXIMATION = 10000;

GoogleTraceSimulator::GoogleTraceSimulator(const string& trace_path) :
  job_map_(new JobMap_t), task_map_(new TaskMap_t),
  resource_map_(new ResourceMap_t), trace_path_(trace_path),
  knowledge_base_(new KnowledgeBaseSimulator) {
  InitializeCostModel();
  knowledge_base_->SetCostModel(cost_model_);
  solver_dispatcher_ =
    new scheduler::SolverDispatcher(shared_ptr<FlowGraph>(flow_graph_), false);
  task_runtime_ =
    new unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>();

  graph_output_ = NULL;
  if (!FLAGS_graph_output_file.empty()) {
    graph_output_ = fopen(FLAGS_graph_output_file.c_str(), "w");
    if (!graph_output_) {
      LOG(FATAL) << "Could not open for writing graph file "
                 << FLAGS_graph_output_file << ", error: " << strerror(errno);
    }
  }
  stats_file_ = NULL;
  if (!FLAGS_stats_file.empty()) {
    stats_file_ = fopen(FLAGS_stats_file.c_str(), "w");
    if (!stats_file_) {
      LOG(FATAL) << "Could not open for writing stats file "
                 << FLAGS_stats_file << ", error: " << strerror(errno);
    }
  }
  ResetSchedulingLatencyStats();
}

GoogleTraceSimulator::~GoogleTraceSimulator() {
  if (graph_output_) {
    fclose(graph_output_);
  }
  if (stats_file_) {
    fclose(stats_file_);
  }
  for (ResourceMap_t::iterator it = resource_map_->begin();
       it != resource_map_->end(); ) {
    ResourceMap_t::iterator it_tmp = it;
    ++it;
    delete it_tmp->second;
  }
  delete solver_dispatcher_;
  delete task_runtime_;
  delete knowledge_base_;
  // N.B. We don't have to delete the cost_model_ because it is owned by
  // the flow graph.
  for (auto& job_id_jd : job_id_to_jd_) {
    delete job_id_jd.second;
  }
  for (auto& task_id_td : task_id_to_td_) {
    delete task_id_td.second;
  }
  for (auto& machine_id_rd : machine_id_to_rd_) {
    delete machine_id_rd.second;
  }
}

void GoogleTraceSimulator::Run() {
  FLAGS_add_root_task_to_graph = false;
  // Terminate if flow solving binary fails.
  FLAGS_flow_scheduling_strict = true;

  // We must check if we're retaining all events. If so, we have to manually
  // set max_event_id_to_retain_ because otherwise we might end up overflowing.
  if (IsEqual(FLAGS_events_fraction, 1.0)) {
    max_event_id_to_retain_ = UINT64_MAX;
  } else {
    max_event_id_to_retain_ = FLAGS_events_fraction * UINT64_MAX;
  }
  LOG(INFO) << "Retaining events with hash < " << max_event_id_to_retain_;

  FLAGS_flow_scheduling_solver = FLAGS_solver;
  if (!FLAGS_solver.compare("flowlessly")) {
    FLAGS_incremental_flow = FLAGS_run_incremental_scheduler;
    FLAGS_only_read_assignment_changes = true;
    FLAGS_flow_scheduling_binary =
        SOLVER_DIR "/flowlessly-git/run_fast_cost_scaling";
  } else if (!FLAGS_solver.compare("cs2")) {
    FLAGS_incremental_flow = false;
    FLAGS_only_read_assignment_changes = false;
    FLAGS_flow_scheduling_binary = SOLVER_DIR "/cs2-git/cs2.exe";
  } else if (!FLAGS_solver.compare("custom")) {
    FLAGS_flow_scheduling_time_reported = true;
  }

  LOG(INFO) << "Starting Google trace simulator!";
  LOG(INFO) << "Time to simulate for: " << FLAGS_runtime << " microseconds.";
  LOG(INFO) << "Maximum number of events to process: " << FLAGS_max_events;
  LOG(INFO) << "Maximum number of scheduling rounds: "
            << FLAGS_max_scheduling_rounds;

  CreateRootResource();
  ReplayTrace();
}

void GoogleTraceSimulator::SolverTimeoutHandler(int sig) {
  signal(SIGALRM, SIG_IGN);
  LOG(FATAL) << "Timeout after waiting for solver for "
             << FLAGS_solver_timeout << " seconds.";
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
    unordered_map<TaskIdentifier, uint64_t,
      TaskIdentifierHasher>* task_runtime) {
  JobDescriptor** jdpp = FindOrNull(job_id_to_jd_, task_identifier.job_id);
  JobDescriptor* jd_ptr;
  if (!jdpp) {
    // Add new job to the graph
    jd_ptr = PopulateJob(task_identifier.job_id);
    CHECK_NOTNULL(jd_ptr);
  } else {
    jd_ptr = *jdpp;
  }
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
  } else {
    // Ignore task if it has already been added.
    LOG(WARNING) << "Task already added: " << task_identifier.job_id << " "
                 << task_identifier.task_index;
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
    // Already added stats to knowledge base.
    VLOG(2) << "Already added stats to knowledge base for "
            << task_identifier.job_id << "/" << task_identifier.task_index;
    return;
  }
  uint64_t* task_runtime_ptr = FindOrNull(*task_runtime, task_identifier);
  double runtime = 0.0;
  if (task_runtime_ptr != NULL) {
    // knowledge base has runtime in ms, but we get it in micros
    runtime = *task_runtime_ptr / 1000.0;
  } else {
    // We don't have information about this task's runtime.
    // Set its runtime to max which means it's a service task.
    runtime = numeric_limits<double>::max();
  }
  TaskDescriptor* td_ptr = FindPtrOrNull(task_id_to_td_, task_identifier);
  CHECK_NOTNULL(td_ptr);
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
  AddTaskStatsToKnowledgeBase(task_identifier, runtime, *task_stats,
                        *task_equiv_classes);
}

void GoogleTraceSimulator::AddTaskStatsToKnowledgeBase(
    const TaskIdentifier& task_identifier, double runtime,
    const TaskStats& task_stats,
    const vector<EquivClass_t>& task_equiv_classes) {
  VLOG(2) << "Setting runtime of " << runtime << " for "
          << task_identifier.job_id << "/" << task_identifier.task_index;
  // Add statistics to all relevant ECs
  for (vector<EquivClass_t>::const_iterator it = task_equiv_classes.begin();
       it != task_equiv_classes.end();
       ++it) {
    knowledge_base_->SetAvgRuntimeForTEC(*it, runtime);
    if (!IsEqual(task_stats.avg_mean_cpu_usage, -1.0)) {
      knowledge_base_->SetAvgMeanCpuUsage(*it, task_stats.avg_mean_cpu_usage);
    }
    if (!IsEqual(task_stats.avg_canonical_mem_usage, -1.0)) {
      knowledge_base_->SetAvgCanonicalMemUsage(
          *it, task_stats.avg_canonical_mem_usage);
    }
    if (!IsEqual(task_stats.avg_assigned_mem_usage, -1.0)) {
      knowledge_base_->SetAvgAssignedMemUsage(
          *it, task_stats.avg_assigned_mem_usage);
    }
    if (!IsEqual(task_stats.avg_unmapped_page_cache, -1.0)) {
      knowledge_base_->SetAvgUnmappedPageCache(
          *it, task_stats.avg_unmapped_page_cache);
    }
    if (!IsEqual(task_stats.avg_total_page_cache, -1.0)) {
      knowledge_base_->SetAvgTotalPageCache(
          *it, task_stats.avg_total_page_cache);
    }
    if (!IsEqual(task_stats.avg_mean_disk_io_time, -1.0)) {
      knowledge_base_->SetAvgMeanDiskIOTime(
          *it, task_stats.avg_mean_disk_io_time);
    }
    if (!IsEqual(task_stats.avg_mean_local_disk_used, -1.0)) {
      knowledge_base_->SetAvgMeanLocalDiskUsed(
          *it, task_stats.avg_mean_local_disk_used);
    }
    if (!IsEqual(task_stats.avg_cpi, -1.0)) {
      knowledge_base_->SetAvgCPIForTEC(*it, task_stats.avg_cpi);
    }
    if (!IsEqual(task_stats.avg_mai, -1.0)) {
      knowledge_base_->SetAvgIPMAForTEC(*it, 1.0 / task_stats.avg_mai);
    }
  }
  task_id_to_stats_.erase(task_identifier);
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

bool GoogleTraceSimulator::HasSimulationCompleted(
    uint64_t task_time,
    uint64_t num_events,
    uint64_t num_scheduling_rounds) {
  // We only run for the first FLAGS_runtime microseconds.
  if (FLAGS_runtime < task_time) {
    LOG(INFO) << "Terminating at : " << task_time;
    return true;
  }
  if (num_events > FLAGS_max_events) {
    LOG(INFO) << "Terminating after " << num_events << " events";
    return true;
  }
  if (num_scheduling_rounds >= FLAGS_max_scheduling_rounds) {
    LOG(INFO) << "Terminating after " << num_scheduling_rounds
              << " scheduling rounds.";
    return true;
  }
  return false;
}

void GoogleTraceSimulator::InitializeCostModel() {
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>();
  switch (FLAGS_flow_scheduling_cost_model) {
  case CostModelType::COST_MODEL_TRIVIAL:
    cost_model_ = new TrivialCostModel(task_map_, leaf_res_ids);
    VLOG(1) << "Using the trivial cost model";
    break;
  case CostModelType::COST_MODEL_RANDOM:
    cost_model_ = new RandomCostModel(task_map_, leaf_res_ids);
    VLOG(1) << "Using the random cost model";
    break;
  case CostModelType::COST_MODEL_COCO:
    cost_model_ = new CocoCostModel(resource_map_, rtn_root_, task_map_,
                                    leaf_res_ids, knowledge_base_);
    VLOG(1) << "Using the coco cost model";
    break;
  case CostModelType::COST_MODEL_SJF:
    cost_model_ = new SJFCostModel(task_map_, leaf_res_ids, knowledge_base_);
    VLOG(1) << "Using the SJF cost model";
    break;
  case CostModelType::COST_MODEL_QUINCY:
    cost_model_ =
      new QuincyCostModel(resource_map_, job_map_, task_map_,
                          &task_bindings_, leaf_res_ids,
                          knowledge_base_);
    VLOG(1) << "Using the Quincy cost model";
    break;
  case CostModelType::COST_MODEL_WHARE:
    cost_model_ = new WhareMapCostModel(resource_map_, task_map_,
                                        knowledge_base_);
    VLOG(1) << "Using the Whare-Map cost model";
    break;
  case CostModelType::COST_MODEL_OCTOPUS:
    cost_model_ = new OctopusCostModel(resource_map_, task_map_);
    VLOG(1) << "Using the octopus cost model";
    break;
  case CostModelType::COST_MODEL_VOID:
    cost_model_ = new VoidCostModel(task_map_);
    VLOG(1) << "Using the void cost model";
    break;
  case CostModelType::COST_MODEL_SIMULATED_QUINCY: {
    uint64_t num_machines =
      std::round(MACHINES_IN_TRACE_APPROXIMATION * FLAGS_events_fraction);
    cost_model_ =
      SetupSimulatedQuincyCostModel(resource_map_, job_map_, task_map_,
                                    task_bindings_, knowledge_base_,
                                    num_machines, leaf_res_ids);
    VLOG(1) << "Using the simulated Quincy cost model";
    break;
  }
  default:
  LOG(FATAL) << "Unknown flow scheduling cost model specified "
             << "(" << FLAGS_flow_scheduling_cost_model << ")";
  }
  flow_graph_.reset(new FlowGraph(cost_model_, leaf_res_ids));
  cost_model_->SetFlowGraph(flow_graph_);
}

void GoogleTraceSimulator::JobCompleted(uint64_t simulator_job_id,
                                        JobID_t job_id) {
  flow_graph_->JobCompleted(job_id);
  // This call frees the JobDescriptor* as well.
  job_map_->erase(job_id);
  job_id_to_jd_.erase(simulator_job_id);
  job_num_tasks_.erase(simulator_job_id);
}

void GoogleTraceSimulator::LoadTraceData(
    ResourceTopologyNodeDescriptor* machine_tmpl) {
  GoogleTraceLoader trace_loader(trace_path_);
  // Import a fictional machine resource topology
  trace_loader.LoadMachineTemplate(machine_tmpl);
  // Load all the machine events.
  trace_loader.LoadMachineEvents(max_event_id_to_retain_, &events_);
  // Populate the job_id to number of tasks mapping.
  trace_loader.LoadJobsNumTasks(&job_num_tasks_);
  // Load tasks' runtime.
  trace_loader.LoadTasksRunningTime(max_event_id_to_retain_, task_runtime_);
  // Populate the knowledge base.
  trace_loader.LoadTaskUtilizationStats(&task_id_to_stats_);
}

uint64_t GoogleTraceSimulator::NextRunSolverAt(uint64_t cur_run_solver_at,
                                               double algorithm_time) {
  if (FLAGS_batch_step == 0) {
    // we're in online mode
    // 1. when we run the solver next depends on how fast we were
    double time_to_solve = min(algorithm_time, FLAGS_online_max_time);
    time_to_solve *= 1000 * 1000; // to micro
    // adjust for time warp factor
    time_to_solve *= FLAGS_online_factor;
    cur_run_solver_at += static_cast<uint64_t>(time_to_solve);
    // 2. if task assignments changed, then graph will have been
    // modified, even in the absence of any new events.
    // Incremental solvers will want to rerun here, as it reduces
    // latency. But we shouldn't count it as an actual iteration.
    // Full solvers will not want to rerun: no point.
    if (FLAGS_incremental_flow) {
      EventDescriptor event;
      event.set_type(EventDescriptor::TASK_ASSIGNMENT_CHANGED);
      events_.insert(make_pair(cur_run_solver_at, event));
    }
  } else {
    // we're in batch mode
    cur_run_solver_at += FLAGS_batch_step;
  }
  return cur_run_solver_at;
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

void GoogleTraceSimulator::ProcessSimulatorEvents(
    uint64_t cur_time,
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
      LogEvent(graph_output_, log_string);
      AddMachine(machine_tmpl, it->second.machine_id());
      UpdateSchedulingLatencyStats(it->first);
    } else if (it->second.type() == EventDescriptor::REMOVE_MACHINE) {
      spf(&log_string, "REMOVE_MACHINE %ju @ %ju\n", it->second.machine_id(),
          it->first);
      LogEvent(graph_output_, log_string);
      RemoveMachine(it->second.machine_id());
      UpdateSchedulingLatencyStats(it->first);
    } else if (it->second.type() == EventDescriptor::UPDATE_MACHINE) {
      // TODO(ionel): Handle machine update event.
    } else if (it->second.type() == EventDescriptor::TASK_END_RUNTIME) {
      spf(&log_string, "TASK_END_RUNTIME %ju:%ju @ %ju\n", it->second.job_id(),
          it->second.task_index(), it->first);
      LogEvent(graph_output_, log_string);
      // Task has finished.
      TaskIdentifier task_identifier;
      task_identifier.task_index = it->second.task_index();
      task_identifier.job_id = it->second.job_id();
      TaskCompleted(task_identifier);
      UpdateSchedulingLatencyStats(it->first);
    } else if (it->second.type() == EventDescriptor::TASK_ASSIGNMENT_CHANGED) {
      // no-op: this event is just used to trigger solver re-run
    } else {
      LOG(FATAL) << "Unexpected event type " << it->second.type() << " @ "
                 << it->first;
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
    spf(&log_string, "TASK_SUBMIT_EVENT: ID %ju:%ju @ %ju\n",
        task_identifier.job_id, task_identifier.task_index,
        cur_time);
    LogEvent(graph_output_, log_string);
    if (AddNewTask(task_identifier, task_runtime)) {
      UpdateSchedulingLatencyStats(cur_time);
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
      RemoveResourceNodeFromParentChildrenList(*rtnd_ptr);
    } else {
      LOG(ERROR) << "Machine " << machine_id
                 << " is not direclty connected to the root";
    }
  } else {
    LOG(ERROR) << "Machine " << machine_id << " doesn't have a parent";
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

void GoogleTraceSimulator::RemoveResourceNodeFromParentChildrenList(
    const ResourceTopologyNodeDescriptor& rtnd) {
  // The parent of the node is the topology root.
  RepeatedPtrField<ResourceTopologyNodeDescriptor>* parent_children =
    rtn_root_.mutable_children();
  int32_t index = 0;
  // Find the node in the parent's children list.
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator it =
         parent_children->begin(); it != parent_children->end();
       ++it, ++index) {
    if (it->resource_desc().uuid()
        .compare(rtnd.resource_desc().uuid()) == 0) {
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
    LOG(FATAL) << "Could not found the machine in the parent's list";
  }
}

void GoogleTraceSimulator::ReplayTrace() {
  // Output CSV header
  OutputStatsHeader(stats_file_);

  // Timing facilities
  boost::timer::cpu_timer timer;
  signal(SIGALRM, GoogleTraceSimulator::SolverTimeoutHandler);

  // Load the trace ingredients
  ResourceTopologyNodeDescriptor machine_tmpl;
  LoadTraceData(&machine_tmpl);

  char line[200];
  vector<string> vals;
  FILE* f_task_events_ptr = NULL;
  uint64_t run_solver_at = 0;
  uint64_t num_events = 0;
  uint64_t num_scheduling_rounds = 0;
  ResetSchedulingLatencyStats();

  for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
       file_num++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05d-of-00500.csv", trace_path_.c_str(),
        file_num);
    if ((f_task_events_ptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(FATAL) << "Failed to open trace for reading of task events.";
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
          if (SpookyHash::Hash64(&task_id, sizeof(task_id), kSeed) >
              max_event_id_to_retain_) {
            // skip event
            continue;
          }

          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);
          VLOG(2) << "TASK EVENT @ " << task_time;
          num_events++;
          if (HasSimulationCompleted(task_time, num_events,
                                     num_scheduling_rounds)) {
            return;
          }

          while (task_time > run_solver_at) {
            ProcessSimulatorEvents(run_solver_at, machine_tmpl);

            if (!flow_graph_->graph_changes().empty()) {
              // Only run solver if something has actually changed.
              // (Sometimes, all the events we received in a time interval
              // have been ignored, e.g. task submit events with duplicate IDs.)
              run_solver_at = RunSolver(run_solver_at);

              // We've done another round, check if we should keep going
              num_scheduling_rounds++;
              if (HasSimulationCompleted(task_time, num_events,
                                         num_scheduling_rounds)) {
                return;
              }
            }

            // skip time until the next event happens
            uint64_t next_event = min(task_time, NextSimulatorEvent());
            VLOG(1) << "Next event at " << next_event;
            run_solver_at = max(next_event, run_solver_at);
            VLOG(1) << "Run solver by " << run_solver_at;
            ResetSchedulingLatencyStats();
          }

          ProcessSimulatorEvents(task_time, machine_tmpl);
          ProcessTaskEvent(task_time, task_id, event_type, task_runtime_);
        }
      }
    }
    fclose(f_task_events_ptr);
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

void GoogleTraceSimulator::ResetSchedulingLatencyStats() {
  first_event_in_scheduling_round_ = UINT64_MAX;
  last_event_in_scheduling_round_ = 0;
  num_events_in_scheduling_round_ = 0;
  sum_timestamps_in_scheduling_round_ = 0;
}

uint64_t GoogleTraceSimulator::RunSolver(uint64_t run_solver_at) {
  double algorithm_time;
  double flow_solver_time;
  LogStartOfSolverRun(graph_output_, flow_graph_, run_solver_at);

  DIMACSChangeStats change_stats(flow_graph_->graph_changes());
  // Set a timeout on the solver's run
  alarm(FLAGS_solver_timeout);
  boost::timer::cpu_timer timer;
  multimap<uint64_t, uint64_t>* task_mappings =
    solver_dispatcher_->Run(&algorithm_time, &flow_solver_time,
                            graph_output_);
  alarm(0);

  // We're done, now update the flow graph with the results
  UpdateFlowGraph(run_solver_at, task_runtime_, task_mappings);
  delete task_mappings;
  // Also update any resource statistics, if required
  UpdateResourceStats();

  double avg_event_timestamp_in_scheduling_round;
  // Set timestamp to max if we haven't seen any events. This has an
  // effect on the logic that computes the scheduling latency. It makes sure
  // that the latency is set to 0 in this case.
  if (num_events_in_scheduling_round_ == 0) {
    avg_event_timestamp_in_scheduling_round =
      numeric_limits<double>::max();
  } else {
    avg_event_timestamp_in_scheduling_round =
      static_cast<double>(sum_timestamps_in_scheduling_round_) /
      num_events_in_scheduling_round_;
  }
  // Log stats to CSV file
  LogSolverRunStats(avg_event_timestamp_in_scheduling_round, stats_file_, timer,
                    run_solver_at, algorithm_time, flow_solver_time,
                    change_stats);

  return NextRunSolverAt(run_solver_at, algorithm_time);
}

void GoogleTraceSimulator::TaskEvicted(TaskID_t task_id,
                                       const ResourceID_t& res_id) {
  VLOG(2) << "Evict task " << task_id << " from resource " << res_id;
  TaskDescriptor** td_ptr = FindOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // Change the state of the task from running to runnable.
  (*td_ptr)->set_state(TaskDescriptor::RUNNABLE);
  flow_graph_->NodeForTaskID(task_id)->type_ = FlowNodeType::UNSCHEDULED_TASK;
  // Remove the running arc and add back arcs to EC and UNSCHED.
  flow_graph_->TaskEvicted(task_id, res_id);

  // Get the Google trace identifier of the task.
  TaskIdentifier* ti_ptr = FindOrNull(task_id_to_identifier_, task_id);
  CHECK_NOTNULL(ti_ptr);
  // Get the end time of the task.
  uint64_t* task_end_time = FindOrNull(task_id_to_end_time_, task_id);
  CHECK_NOTNULL(task_end_time);
  TaskEvictedClearSimulatorState(task_id, *task_end_time, res_id, *ti_ptr);
}

void GoogleTraceSimulator::TaskEvictedClearSimulatorState(
    TaskID_t task_id,
    uint64_t task_end_time,
    const ResourceID_t& res_id,
    const TaskIdentifier& task_identifier) {
  // Remove the task end time event from the simulator events_.
  pair<multimap<uint64_t, EventDescriptor>::iterator,
       multimap<uint64_t, EventDescriptor>::iterator> range_it =
    events_.equal_range(task_end_time);
  for (; range_it.first != range_it.second; range_it.first++) {
    if (range_it.first->second.type() == EventDescriptor::TASK_END_RUNTIME &&
        range_it.first->second.job_id() == task_identifier.job_id &&
        range_it.first->second.task_index() == task_identifier.task_index) {
      break;
    }
  }
  // We've found the event.
  if (range_it.first != range_it.second) {
    events_.erase(range_it.first);
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
}

void GoogleTraceSimulator::TaskCompleted(
    const TaskIdentifier& task_identifier) {
  TaskDescriptor** td_ptr = FindOrNull(task_id_to_td_, task_identifier);
  CHECK_NOTNULL(td_ptr);
  TaskID_t task_id = (*td_ptr)->uid();
  JobID_t job_id = JobIDFromString((*td_ptr)->job_id());
  // Remove the task node from the flow graph.
  flow_graph_->TaskCompleted(task_id);
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
    CHECK(src->type_ == FlowNodeType::SCHEDULED_TASK ||
          src->type_ == FlowNodeType::UNSCHEDULED_TASK ||
          src->type_ == FlowNodeType::ROOT_TASK);
    // Destination must be a PU node
    CHECK(dst->type_ == FlowNodeType::PU);
    // XXX: what about unscheduled tasks?
    // Get the TD and RD for the source and destination
    TaskDescriptor* task = FindPtrOrNull(*task_map_, src->task_id_);
    CHECK_NOTNULL(task);
    ResourceStatus* target_res_status =
      FindPtrOrNull(*resource_map_, dst->resource_id_);
    CHECK_NOTNULL(target_res_status);
    const ResourceDescriptor& resource = target_res_status->descriptor();
    // Populate the scheduling delta.
    solver_dispatcher_->NodeBindingToSchedulingDelta(
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
      node->type_ = FlowNodeType::SCHEDULED_TASK;
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
      node->type_ = FlowNodeType::SCHEDULED_TASK;
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

void GoogleTraceSimulator::UpdateResourceStats() {
  if (FLAGS_flow_scheduling_cost_model ==
      CostModelType::COST_MODEL_COCO ||
      FLAGS_flow_scheduling_cost_model ==
      CostModelType::COST_MODEL_OCTOPUS ||
      FLAGS_flow_scheduling_cost_model ==
      CostModelType::COST_MODEL_WHARE) {
    flow_graph_->ComputeTopologyStatistics(
        flow_graph_->sink_node(),
        boost::bind(&CostModelInterface::GatherStats,
                    cost_model_, _1, _2));
    flow_graph_->ComputeTopologyStatistics(
        flow_graph_->sink_node(),
        boost::bind(&CostModelInterface::UpdateStats,
                    cost_model_, _1, _2));
  } else {
    LOG(INFO) << "No resource stats update required";
  }
}

void GoogleTraceSimulator::UpdateSchedulingLatencyStats(uint64_t time) {
  first_event_in_scheduling_round_ =
    min(time, first_event_in_scheduling_round_);
  last_event_in_scheduling_round_ =
    max(time, last_event_in_scheduling_round_);
  sum_timestamps_in_scheduling_round_ += time;
  num_events_in_scheduling_round_++;
}

} // namespace sim
} // namespace firmament
