// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
#include "sim/trace-extract/google_trace_bridge.h"

#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/units.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow/flow_scheduler.h"
#include "sim/trace-extract/google_trace_loader.h"
#include "sim/trace-extract/knowledge_base_simulator.h"
#include "storage/simple_object_store.h"

using boost::lexical_cast;
using boost::hash;

DECLARE_double(events_fraction);

namespace firmament {
namespace sim {

GoogleTraceBridge::GoogleTraceBridge(
    const string& trace_path,
    GoogleTraceEventManager* event_manager) :
    event_manager_(event_manager), job_map_(new JobMap_t),
    knowledge_base_(new KnowledgeBaseSimulator),
    resource_map_(new ResourceMap_t), task_map_(new TaskMap_t),
    trace_path_(trace_path) {
  ResourceID_t root_uuid = GenerateRootResourceID("XXXgoogleXXX");
  ResourceDescriptor* rd_ptr = rtn_root_.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(root_uuid));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
  CHECK(InsertIfNotPresent(resource_map_.get(), root_uuid,
                           new ResourceStatus(rd_ptr, &rtn_root_,
                                              "endpoint_uri",
                                              GetCurrentTimestamp())));
  messaging_adapter_ =
    new platform::sim::SimulatedMessagingAdapter<BaseMessage>();
  SchedulingParameters params;
  scheduler_ = new scheduler::FlowScheduler(
      job_map_, resource_map_, &rtn_root_,
      shared_ptr<store::ObjectStoreInterface>(
          new store::SimpleObjectStore(root_uuid)),
      task_map_, knowledge_base_,
      shared_ptr<machine::topology::TopologyManager>(
          new machine::topology::TopologyManager),
      messaging_adapter_, this, root_uuid, "http://localhost", params);
}

GoogleTraceBridge::~GoogleTraceBridge() {
  while (rtn_root_.children_size() > 0) {
    rtn_root_.mutable_children()->RemoveLast();
  }
  // N.B. We don't have to delete:
  // 1) event_manager_ because it is owned by google_trace_simulator.
  // 2) machine_res_id_pus_ and resource_map because rds are deleted when
  // we remove the machines from rtn_root_.
  // 3) job_map_ and trace_job_id_to_jd_ because the jds are deleted
  // automatically.
  // 4) task_map_ and trace_task_id_to_td_ because the tds are deleted
  // when job_map_ is freed.
  delete scheduler_;
  delete messaging_adapter_;
}

ResourceDescriptor* GoogleTraceBridge::AddMachine(
    const ResourceTopologyNodeDescriptor& machine_tmpl,
    uint64_t machine_id) {
  // Create a new machine topology descriptor.
  ResourceTopologyNodeDescriptor* new_machine = rtn_root_.add_children();
  new_machine->CopyFrom(machine_tmpl);
  const string& root_uuid = rtn_root_.resource_desc().uuid();
  char hn[100];
  snprintf(hn, sizeof(hn), "h%ju", machine_id);
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&GoogleTraceBridge::ResetUuidAndAddResource,
                               this, _1, string(hn), root_uuid));
  ResourceDescriptor* rd_ptr = new_machine->mutable_resource_desc();
  rd_ptr->set_friendly_name(hn);
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  CHECK(InsertIfNotPresent(&trace_machine_id_to_rtnd_, machine_id,
                           new_machine));
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&GoogleTraceBridge::RegisterMachinePUs,
                               this, _1, res_id));
  return rd_ptr;
}

void GoogleTraceBridge::AddMachineSamples(uint64_t current_time) {
  for (auto& machine_id_rtnd : trace_machine_id_to_rtnd_) {
    ResourceDescriptor* machine_rd_ptr =
      machine_id_rtnd.second->mutable_resource_desc();
    unordered_map<TaskID_t, ResourceDescriptor*> running_task_id_to_rd;
    vector<ResourceDescriptor*> pu_rds;
    pair<multimap<ResourceID_t, ResourceDescriptor*>::iterator,
         multimap<ResourceID_t, ResourceDescriptor*>::iterator> range_it =
      machine_res_id_pus_.equal_range(
          ResourceIDFromString(machine_rd_ptr->uuid()));
    for (; range_it.first != range_it.second; range_it.first++) {
      pu_rds.push_back(range_it.first->second);
    }
    for (auto& rd_ptr : pu_rds) {
      vector<TaskID_t> tasks =
        scheduler_->BoundTasksForResource(ResourceIDFromString(rd_ptr->uuid()));
      for (auto& task : tasks) {
        CHECK(InsertIfNotPresent(&running_task_id_to_rd, task, rd_ptr));
      }
    }
    knowledge_base_->AddMachineSample(current_time, machine_rd_ptr,
                                      running_task_id_to_rd);
  }
}

TaskDescriptor* GoogleTraceBridge::AddTask(
    const TraceTaskIdentifier& task_identifier) {
  JobDescriptor* jd_ptr = FindPtrOrNull(trace_job_id_to_jd_,
                                        task_identifier.job_id);
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
    // TODO(ionel): Populate task with the appropriate type.
    td_ptr->set_task_type(TaskDescriptor::DEVIL);
    if (InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr)) {
      CHECK(InsertIfNotPresent(&task_id_to_identifier_,
                               td_ptr->uid(), task_identifier));
      // Add task to the google (job_id, task_index) to TaskDescriptor* map.
      CHECK(InsertIfNotPresent(&trace_task_id_to_td_, task_identifier, td_ptr));
      // Update statistics used by cost models. This must be done prior
      // to adding the job to the scheduler, as costs computed in that step.
      AddTaskStats(task_identifier, td_ptr->uid());
      scheduler_->AddJob(jd_ptr);
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
    const TraceTaskIdentifier& task_identifier,
    TaskDescriptor* td_ptr) {
  uint64_t* runtime_ptr = FindOrNull(task_runtime_, task_identifier);
  EventDescriptor event_desc;
  event_desc.set_job_id(task_identifier.job_id);
  event_desc.set_task_index(task_identifier.task_index);
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  if (runtime_ptr != NULL) {
    // We can approximate the duration of the task.
    event_manager_->AddEvent(event_manager_->current_simulation_time() +
                             *runtime_ptr, event_desc);
    td_ptr->set_finish_time(event_manager_->current_simulation_time() +
                            *runtime_ptr);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // last timestamp of the simulation.
    event_manager_->AddEvent(FLAGS_runtime, event_desc);
    td_ptr->set_finish_time(FLAGS_runtime);
  }
}

void GoogleTraceBridge::AddTaskStats(
    const TraceTaskIdentifier& trace_task_identifier,
    TaskID_t task_id) {
  TraceTaskStats* task_stats =
    FindOrNull(trace_task_id_to_stats_, trace_task_identifier);
  if (task_stats == NULL) {
    // Already added stats to knowledge base.
    LOG(WARNING) << "Already added stats to knowledge base for "
                 << trace_task_identifier.job_id << "/"
                 << trace_task_identifier.task_index;
    return;
  }
  uint64_t* task_runtime_ptr =
    FindOrNull(task_runtime_, trace_task_identifier);
  double runtime = 0.0;
  if (task_runtime_ptr != NULL) {
    // knowledge base has runtime in ms, but we get it in micros
    runtime = *task_runtime_ptr / MILLISECONDS_TO_MICROSECONDS;
  } else {
    // We don't have information about this task's runtime.
    // Set its runtime to max which means it's a service task.
    runtime = numeric_limits<double>::max();
  }
  knowledge_base_->SetTraceTaskStats(task_id, *task_stats);
  trace_task_id_to_stats_.erase(trace_task_identifier);
}

TaskDescriptor* GoogleTraceBridge::AddTaskToJob(JobDescriptor* jd_ptr) {
  CHECK_NOTNULL(jd_ptr);
  TaskDescriptor* root_task = jd_ptr->mutable_root_task();
  TaskDescriptor* new_task = root_task->add_spawned();
  new_task->set_uid(GenerateTaskID(*root_task));
  new_task->set_state(TaskDescriptor::CREATED);
  new_task->set_job_id(jd_ptr->uuid());
  return new_task;
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
  trace_loader.LoadTasksRunningTime(MaxEventIdToRetain(), &task_runtime_);
  // Populate the knowledge base.
  trace_loader.LoadTaskUtilizationStats(&trace_task_id_to_stats_);
}

void GoogleTraceBridge::TaskCompleted(
    const TraceTaskIdentifier& task_identifier) {
  TaskDescriptor* td_ptr = FindPtrOrNull(trace_task_id_to_td_, task_identifier);
  CHECK_NOTNULL(td_ptr);
  TaskFinalReport report;
  scheduler_->HandleTaskCompletion(td_ptr, &report);
  scheduler_->HandleTaskFinalReport(report, td_ptr);
  task_map_->erase(td_ptr->uid());
  knowledge_base_->EraseTraceTaskStats(td_ptr->uid());
}

void GoogleTraceBridge::OnJobCompletion(JobID_t job_id) {
  uint64_t* trace_job_id = FindOrNull(job_id_to_trace_job_id_, job_id);
  CHECK_NOTNULL(trace_job_id);
  uint64_t* num_tasks = FindOrNull(job_num_tasks_, *trace_job_id);
  CHECK_EQ(*num_tasks, 0) << "There are tasks that haven't finished";
  JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd_ptr);
  task_map_->erase(jd_ptr->root_task().uid());
  job_map_->erase(job_id);
  trace_job_id_to_jd_.erase(*trace_job_id);
  job_num_tasks_.erase(*trace_job_id);
  job_id_to_trace_job_id_.erase(job_id);
}

void GoogleTraceBridge::OnTaskCompletion(TaskDescriptor* td_ptr,
                                         ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  TraceTaskIdentifier* ti_ptr = FindOrNull(task_id_to_identifier_, task_id);
  CHECK_NOTNULL(ti_ptr);
  trace_task_id_to_td_.erase(*ti_ptr);
  task_runtime_.erase(*ti_ptr);
  // Decrease the number of tasks left to complete.
  uint64_t* num_tasks = FindOrNull(job_num_tasks_, ti_ptr->job_id);
  CHECK_NOTNULL(num_tasks);
  (*num_tasks)--;
  // We don't erase the task from the task_map_ here because it's still
  // used in the HandleTaskFinalReport method. We erase it right at the end
  // in the TaskCompleted method.
  task_id_to_identifier_.erase(task_id);
  // We don't have to remove the end event from the event_manager
  // because the event is removed by the simulator.
}

void GoogleTraceBridge::OnTaskEviction(TaskDescriptor* td_ptr,
                                       ResourceDescriptor* rd_ptr) {
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  uint64_t task_end_time = td_ptr->finish_time();
  td_ptr->set_start_time(0);
  event_manager_->RemoveTaskEndRuntimeEvent(*ti_ptr, task_end_time);
}

void GoogleTraceBridge::OnTaskFailure(TaskDescriptor* td_ptr,
                                      ResourceDescriptor* rd_ptr) {
  // We don't have to do anything because simulated tasks do not fail.
  LOG(FATAL) << "Task should not fail in the simulator";
}

void GoogleTraceBridge::OnTaskMigration(TaskDescriptor* td_ptr,
                                        ResourceDescriptor* rd_ptr) {
  // XXX(ionel): This assumes that the Migration is done via two calls.
  // First, a call to OnTaskEviction and then OnTaskMigration.
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  uint64_t task_end_time = td_ptr->finish_time();
  td_ptr->set_start_time(event_manager_->current_simulation_time());
  event_manager_->RemoveTaskEndRuntimeEvent(*ti_ptr, task_end_time);
  AddTaskEndEvent(*ti_ptr, td_ptr);
}

void GoogleTraceBridge::OnTaskPlacement(TaskDescriptor* td_ptr,
                                        ResourceDescriptor* rd_ptr) {
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  td_ptr->set_start_time(event_manager_->current_simulation_time());
  AddTaskEndEvent(*ti_ptr, td_ptr);
}

JobDescriptor* GoogleTraceBridge::PopulateJob(uint64_t trace_job_id) {
  JobDescriptor jd;
  // Generate a hash out of the trace job_id.
  JobID_t new_job_id = GenerateJobID(trace_job_id);

  CHECK(InsertIfNotPresent(job_map_.get(), new_job_id, jd));
  // Get the new value of the pointer because jd has been copied.
  JobDescriptor* jd_ptr = FindOrNull(*job_map_, new_job_id);

  // Maintain a mapping between the trace job_id and the generated job_id.
  jd_ptr->set_uuid(to_string(new_job_id));
  InsertOrUpdate(&trace_job_id_to_jd_, trace_job_id, jd_ptr);
  InsertOrUpdate(&job_id_to_trace_job_id_, new_job_id, trace_job_id);
  TaskDescriptor* rt = jd_ptr->mutable_root_task();
  string bin;
  // XXX(malte): hack, should use logical job name
  spf(&bin, "%jd", trace_job_id);
  rt->set_binary(bin);
  rt->set_uid(GenerateRootTaskID(*jd_ptr));
  rt->set_state(TaskDescriptor::CREATED);
  CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
  return jd_ptr;
}

void GoogleTraceBridge::RegisterMachinePUs(
    ResourceTopologyNodeDescriptor* rtnd_ptr,
    ResourceID_t machine_res_id) {
  if (rtnd_ptr->resource_desc().type() == ResourceDescriptor::RESOURCE_PU) {
    scheduler_->RegisterResource(
        ResourceIDFromString(rtnd_ptr->resource_desc().uuid()), false, true);
    machine_res_id_pus_.insert(
        pair<ResourceID_t, ResourceDescriptor*>(
            machine_res_id, rtnd_ptr->mutable_resource_desc()));
  }
}

void GoogleTraceBridge::RemoveMachine(uint64_t machine_id) {
  ResourceTopologyNodeDescriptor* rtnd_ptr =
    FindPtrOrNull(trace_machine_id_to_rtnd_, machine_id);
  CHECK_NOTNULL(rtnd_ptr);
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  machine_res_id_pus_.erase(res_id);
  // Traverse the resource topology tree in order to evict tasks and
  // remove resources from resource_map.
  DFSTraverseResourceProtobufTreeReturnRTND(
      rtnd_ptr, boost::bind(&GoogleTraceBridge::RemoveResource, this, _1));
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
  // We only free the ResourceTopologyNodeDescriptor in the destructor.
}

void GoogleTraceBridge::RemoveResource(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  // TODO(ionel): Move logic to event_driven_scheduler once DeregisterREsource
  // has support for task termination.
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  // First we need to evict the tasks.
  vector<TaskID_t> tasks = scheduler_->BoundTasksForResource(res_id);
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs_ptr);
  ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
  for (auto& task_id : tasks) {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    scheduler_->HandleTaskEviction(td_ptr, rd_ptr);
  }
  scheduler_->DeregisterResource(res_id);
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

void GoogleTraceBridge::RunSolver(SchedulerStats* scheduler_stats) {
  scheduler_->ScheduleAllJobs(scheduler_stats);
}

} // namespace sim
} // namespace firmament
