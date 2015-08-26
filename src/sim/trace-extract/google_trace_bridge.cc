// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
#include "sim/trace-extract/google_trace_bridge.h"

#include <boost/lexical_cast.hpp>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/units.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow/cost_models.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "sim/knowledge_base_simulator.h"
#include "sim/trace-extract/google_trace_loader.h"
#include "sim/trace-extract/simulated_quincy_factory.h"

using boost::lexical_cast;

DEFINE_int32(flow_scheduling_cost_model, 0,
             "Flow scheduler cost model to use. "
             "Values: 0 = TRIVIAL, 1 = RANDOM, 2 = SJF, 3 = QUINCY, "
             "4 = WHARE, 5 = COCO, 6 = OCTOPUS, 7 = VOID, "
             "8 = SIMULATED QUINCY");

namespace firmament {
namespace sim {

GoogleTraceBridge::GoogleTraceBridge(
    const string& trace_path,
    GoogleTraceEventManager* event_manager) :
    event_manager_(event_manager), job_map_(new JobMap_t),
    knowledge_base_(new KnowledgeBaseSimulator),
    resource_map_(new ResourceMap_t), task_map_(new TaskMap_t),
    trace_path_(trace_path) {
  task_runtime_ =
    new unordered_map<TraceTaskIdentifier, uint64_t,
                      TraceTaskIdentifierHasher>();
  InitializeCostModel();
  solver_dispatcher_ = new scheduler::SolverDispatcher(flow_graph_, false);
  knowledge_base_->SetCostModel(cost_model_);
}

GoogleTraceBridge::~GoogleTraceBridge() {
  for (auto& job_id_jd : job_id_to_jd_) {
    delete job_id_jd.second;
  }
  for (auto& ti_td : *task_map_) {
    delete ti_td.second;
  }
  // We don't have to delete the TaskDescriptor* from machine_id_to_td_
  // and trace_task_id_to_td_ because we delete them when we iterate over
  // task_map_.
  for (auto& machine_id_rtnd : trace_machine_id_to_rtnd_) {
    delete machine_id_rtnd.second;
  }
  for (ResourceMap_t::iterator it = resource_map_->begin();
       it != resource_map_->end(); ) {
    ResourceMap_t::iterator it_tmp = it;
    ++it;
    delete it_tmp->second;
  }
  // N.B. We don't have to delete:
  // 1) cost_model_ because it is owned by the flow graph.
  // 2) event_manager_ because it is owned by google_trace_simulator.
  delete knowledge_base_;
  delete solver_dispatcher_;
  delete task_runtime_;
}

ResourceDescriptor* GoogleTraceBridge::AddMachine(
    const ResourceTopologyNodeDescriptor& machine_tmpl, uint64_t machine_id) {
  // Create a new machine topology descriptor.
  ResourceTopologyNodeDescriptor* new_machine = rtn_root_.add_children();
  new_machine->CopyFrom(machine_tmpl);
  const string& root_uuid = rtn_root_.resource_desc().uuid();
  char hn[100];
  snprintf(hn, sizeof(hn), "h%ju", machine_id);
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&GoogleTraceBridge::ResetUuidAndAddResource,
                               this, _1, string(hn), root_uuid));
  ResourceDescriptor* rd = new_machine->mutable_resource_desc();
  rd->set_friendly_name(hn);
  // Add the node to the flow graph.
  flow_graph_->AddMachine(new_machine);
  CHECK(InsertIfNotPresent(&trace_machine_id_to_rtnd_, machine_id,
                           new_machine));
  return rd;
}

TaskDescriptor* GoogleTraceBridge::AddTask(
    const TraceTaskIdentifier& task_identifier) {
  JobDescriptor* jd_ptr = FindPtrOrNull(job_id_to_jd_, task_identifier.job_id);
  if (!jd_ptr) {
    // Add new job to the graph
    jd_ptr = PopulateJob(task_identifier.job_id);
    CHECK_NOTNULL(jd_ptr);
  }
  TaskDescriptor* td_ptr = NULL;
  if (FindOrNull(trace_task_id_to_td_, task_identifier) == NULL) {
    td_ptr = AddTaskToJob(jd_ptr);
    // XXX(ionel): Hack. We're setting the Google trace job id as the binary
    // of the job in order for Firmament to uniquely identify it.
    td_ptr->set_binary(lexical_cast<string>(task_identifier.job_id));
    if (InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr)) {
      CHECK(InsertIfNotPresent(&task_id_to_identifier_,
                               td_ptr->uid(), task_identifier));
      // Add task to the google (job_id, task_index) to TaskDescriptor* map.
      CHECK(InsertIfNotPresent(&trace_task_id_to_td_, task_identifier, td_ptr));
      // Update statistics used by cost models. This must be done PRIOR
      // to AddOrUpdateJobNodes, as costs computed in that step.
      AddTaskStats(task_identifier);
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

void GoogleTraceBridge::AddTaskEndEvent(
    uint64_t cur_timestamp,
    const TraceTaskIdentifier& task_identifier) {
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_identifier);
  EventDescriptor event_desc;
  event_desc.set_job_id(task_identifier.job_id);
  event_desc.set_task_index(task_identifier.task_index);
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  TaskDescriptor* td_ptr = FindPtrOrNull(trace_task_id_to_td_, task_identifier);
  if (runtime_ptr != NULL) {
    // We can approximate the duration of the task.
    event_manager_->AddEvent(cur_timestamp + *runtime_ptr, event_desc);
    td_ptr->set_finish_time(cur_timestamp + *runtime_ptr);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // last timestamp of the simulation.
    event_manager_->AddEvent(FLAGS_runtime, event_desc);
    td_ptr->set_finish_time(FLAGS_runtime);
  }
}

void GoogleTraceBridge::AddTaskStats(
    const TraceTaskIdentifier& task_identifier) {
  TaskStats* task_stats = FindOrNull(trace_task_id_to_stats_, task_identifier);
  if (task_stats == NULL) {
    // Already added stats to knowledge base.
    LOG(WARNING) << "Already added stats to knowledge base for "
                 << task_identifier.job_id << "/" << task_identifier.task_index;
    return;
  }
  uint64_t* task_runtime_ptr = FindOrNull(*task_runtime_, task_identifier);
  double runtime = 0.0;
  if (task_runtime_ptr != NULL) {
    // knowledge base has runtime in ms, but we get it in micros
    runtime = *task_runtime_ptr / MILLISECONDS_TO_MICROSECONDS;
  } else {
    // We don't have information about this task's runtime.
    // Set its runtime to max which means it's a service task.
    runtime = numeric_limits<double>::max();
  }
  TaskDescriptor* td_ptr = FindPtrOrNull(trace_task_id_to_td_, task_identifier);
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

void GoogleTraceBridge::AddTaskStatsToKnowledgeBase(
    const TraceTaskIdentifier& task_identifier, double runtime,
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
  trace_task_id_to_stats_.erase(task_identifier);
}

TaskDescriptor* GoogleTraceBridge::AddTaskToJob(JobDescriptor* jd_ptr) {
  CHECK_NOTNULL(jd_ptr);
  TaskDescriptor* root_task = jd_ptr->mutable_root_task();
  TaskDescriptor* new_task = root_task->add_spawned();
  new_task->set_uid(GenerateTaskID(*root_task));
  new_task->set_state(TaskDescriptor::RUNNABLE);
  new_task->set_job_id(jd_ptr->uuid());
  return new_task;
}

void GoogleTraceBridge::CreateRootResource() {
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

void GoogleTraceBridge::InitializeCostModel() {
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
  case CostModelType::COST_MODEL_SIMULATED_QUINCY:
    cost_model_ =
      SetupSimulatedQuincyCostModel(resource_map_, job_map_, task_map_,
                                    &task_bindings_, knowledge_base_,
                                    leaf_res_ids);
    VLOG(1) << "Using the simulated Quincy cost model";
    break;
  default:
  LOG(FATAL) << "Unknown flow scheduling cost model specified "
             << "(" << FLAGS_flow_scheduling_cost_model << ")";
  }
  flow_graph_.reset(new FlowGraph(cost_model_, leaf_res_ids));
  cost_model_->SetFlowGraph(flow_graph_);
}

void GoogleTraceBridge::JobCompleted(uint64_t simulator_job_id,
                                     JobID_t job_id) {
  flow_graph_->JobCompleted(job_id);
  // This call frees the JobDescriptor* as well.
  job_map_->erase(job_id);
  job_id_to_jd_.erase(simulator_job_id);
  job_num_tasks_.erase(simulator_job_id);
}

void GoogleTraceBridge::LoadTraceData(
    ResourceTopologyNodeDescriptor* machine_tmpl) {
  GoogleTraceLoader trace_loader(trace_path_);
  // Import a fictional machine resource topology
  trace_loader.LoadMachineTemplate(machine_tmpl);
  // Load all the machine events.
  multimap<uint64_t, EventDescriptor> machine_events;
  trace_loader.LoadMachineEvents(MaxEventIdToRetain(), &machine_events);
  for (auto& machine_event : machine_events) {
    event_manager_->AddEvent(machine_event.first, machine_event.second);
  }
  // Populate the job_id to number of tasks mapping.
  trace_loader.LoadJobsNumTasks(&job_num_tasks_);
  // Load tasks' runtime.
  trace_loader.LoadTasksRunningTime(MaxEventIdToRetain(), task_runtime_);
  // Populate the knowledge base.
  trace_loader.LoadTaskUtilizationStats(&trace_task_id_to_stats_);
}

JobDescriptor* GoogleTraceBridge::PopulateJob(uint64_t job_id) {
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

void GoogleTraceBridge::RemoveMachine(uint64_t machine_id) {
  ResourceTopologyNodeDescriptor* rtnd_ptr =
    FindPtrOrNull(trace_machine_id_to_rtnd_, machine_id);
  CHECK_NOTNULL(rtnd_ptr);
  // Traverse the resource topology tree in order to evict tasks and
  // remove resources from resource_map.
  DFSTraverseResourceProtobufTreeReturnRTND(
      rtnd_ptr, boost::bind(&GoogleTraceBridge::RemoveResource, this, _1));
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
  trace_machine_id_to_rtnd_.erase(machine_id);
  // We can't delete the node because we haven't removed it from it's parent
  // children list.
  // delete rtnd_ptr;
}

void GoogleTraceBridge::RemoveResource(
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

void GoogleTraceBridge::RemoveResourceNodeFromParentChildrenList(
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

void GoogleTraceBridge::RemoveTaskStats(TaskID_t task_id) {
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

void GoogleTraceBridge::ResetUuidAndAddResource(
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

multimap<uint64_t, uint64_t>* GoogleTraceBridge::RunSolver(
    double *algorithm_time,
    double *flow_solver_time,
    FILE *graph_output) {
  return solver_dispatcher_->Run(algorithm_time, flow_solver_time,
                                 graph_output);
}

void GoogleTraceBridge::TaskCompleted(
    const TraceTaskIdentifier& task_identifier) {
  TaskDescriptor** td_ptr = FindOrNull(trace_task_id_to_td_, task_identifier);
  CHECK_NOTNULL(td_ptr);
  TaskID_t task_id = (*td_ptr)->uid();
  JobID_t job_id = JobIDFromString((*td_ptr)->job_id());
  // Remove the task node from the flow graph.
  flow_graph_->TaskCompleted(task_id);
  // Erase from local state: trace_task_id_to_td_, task_id_to_identifier_,
  // task_map_ and task_bindings_, res_id_to_task_id_.
  trace_task_id_to_td_.erase(task_identifier);
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
  uint64_t* num_tasks = FindOrNull(job_num_tasks_, task_identifier.job_id);
  CHECK_NOTNULL(num_tasks);
  (*num_tasks)--;
  if (*num_tasks == 0) {
    JobCompleted(task_identifier.job_id, job_id);
  }
}

void GoogleTraceBridge::TaskEvicted(TaskID_t task_id,
                                    const ResourceID_t& res_id) {
  VLOG(2) << "Evict task " << task_id << " from resource " << res_id;
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // Change the state of the task from running to runnable.
  td_ptr->set_state(TaskDescriptor::RUNNABLE);
  flow_graph_->NodeForTaskID(task_id)->type_ = FlowNodeType::UNSCHEDULED_TASK;
  // Remove the running arc and add back arcs to EC and UNSCHED.
  flow_graph_->TaskEvicted(task_id, res_id);

  // Get the Google trace identifier of the task.
  TraceTaskIdentifier* ti_ptr = FindOrNull(task_id_to_identifier_, task_id);
  CHECK_NOTNULL(ti_ptr);
  uint64_t task_end_time = td_ptr->finish_time();
  // Unset the finish time.
  td_ptr->set_finish_time(0);
  TaskEvictedClearSimulatorState(task_id, task_end_time, res_id, *ti_ptr);
}

void GoogleTraceBridge::TaskEvictedClearSimulatorState(
    TaskID_t task_id,
    uint64_t task_end_time,
    const ResourceID_t& res_id,
    const TraceTaskIdentifier& task_identifier) {
  event_manager_->RemoveTaskEndRuntimeEvent(task_identifier, task_end_time);
  ResourceID_t res_id_tmp = res_id;
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
  rd_ptr->clear_current_running_task();
  task_bindings_.erase(task_id);
  res_id_to_task_id_.erase(res_id_tmp);
}

void GoogleTraceBridge::UpdateFlowGraph(
    uint64_t scheduling_timestamp,
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
      TraceTaskIdentifier* task_identifier =
        FindOrNull(task_id_to_identifier_, task_id);
      CHECK_NOTNULL(task_identifier);
      AddTaskEndEvent(scheduling_timestamp, *task_identifier);
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

void GoogleTraceBridge::UpdateResourceStats() {
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

} // namespace sim
} // namespace firmament
