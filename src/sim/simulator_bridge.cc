// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
#include "sim/simulator_bridge.h"

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
#include "scheduling/simple/simple_scheduler.h"
#include "sim/knowledge_base_simulator.h"
#include "sim/trace_loader.h"
#include "storage/simple_object_store.h"

using boost::lexical_cast;
using boost::hash;

DECLARE_uint64(runtime);
DECLARE_string(scheduler);

namespace firmament {
namespace sim {

SimulatorBridge::SimulatorBridge(EventManager* event_manager,
                                 SimulatedWallTime* simulated_time)
    : event_manager_(event_manager), simulated_time_(simulated_time),
    job_map_(new JobMap_t), knowledge_base_(new KnowledgeBaseSimulator),
    resource_map_(new ResourceMap_t), task_map_(new TaskMap_t),
    num_duplicate_task_ids_(0) {
  ResourceID_t root_uuid = GenerateRootResourceID("XXXsimulatorXXX");
  ResourceDescriptor* rd_ptr = rtn_root_.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(root_uuid));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
  CHECK(InsertIfNotPresent(
      resource_map_.get(), root_uuid,
      new ResourceStatus(rd_ptr, &rtn_root_,
                         "endpoint_uri",
                         simulated_time_->GetCurrentTimestamp())));
  messaging_adapter_ =
    new platform::sim::SimulatedMessagingAdapter<BaseMessage>();
  if (FLAGS_scheduler == "flow") {
    scheduler_ = new scheduler::FlowScheduler(
        job_map_, resource_map_, &rtn_root_,
        shared_ptr<store::ObjectStoreInterface>(
            new store::SimpleObjectStore(root_uuid)),
        task_map_, knowledge_base_,
        shared_ptr<machine::topology::TopologyManager>(
            new machine::topology::TopologyManager),
        messaging_adapter_, this, root_uuid, "http://localhost",
        simulated_time_);
  } else {
    scheduler_ = new scheduler::SimpleScheduler(
        job_map_, resource_map_, &rtn_root_,
        shared_ptr<store::ObjectStoreInterface>(
            new store::SimpleObjectStore(root_uuid)),
        task_map_, knowledge_base_,
        shared_ptr<machine::topology::TopologyManager>(
            new machine::topology::TopologyManager),
        messaging_adapter_, this, root_uuid, "http://localhost",
        simulated_time_);
  }
  // Import a fictional machine resource topology
  LoadMachineTemplate(&machine_tmpl_);
}

SimulatorBridge::~SimulatorBridge() {
  while (rtn_root_.children_size() > 0) {
    rtn_root_.mutable_children()->RemoveLast();
  }
  // N.B. We don't have to delete:
  // 1) event_manager_ and simulated_time_ because they are owned by
  // trace_simulator.
  // 2) machine_res_id_pus_ and resource_map because rds are deleted when
  // we remove the machines from rtn_root_.
  // 3) job_map_ and trace_job_id_to_jd_ because the jds are deleted
  // automatically.
  // 4) task_map_ and trace_task_id_to_td_ because the tds are deleted
  // when job_map_ is freed.
  delete scheduler_;
  delete messaging_adapter_;
}

ResourceDescriptor* SimulatorBridge::AddMachine(
    uint64_t machine_id) {
  // Create a new machine topology descriptor.
  ResourceTopologyNodeDescriptor* new_machine = rtn_root_.add_children();
  new_machine->CopyFrom(machine_tmpl_);
  const string& root_uuid = rtn_root_.resource_desc().uuid();
  // Mark the hostname as a simulation one. This is useful for TraceGenerator
  // which if it detects a simulated host then it outputs the host's trace id
  // rather than Firmament's machine id.
  string hostname = "firmament_simulation_machine_" +
    lexical_cast<string>(machine_id);
  ResourceDescriptor* rd_ptr = new_machine->mutable_resource_desc();
  rd_ptr->set_friendly_name(hostname);
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  ResourceVector* res_cap = rd_ptr->mutable_resource_capacity();
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&SimulatorBridge::SetupMachine,
                               this, _1, res_cap, hostname, root_uuid, res_id));
  CHECK(InsertIfNotPresent(&trace_machine_id_to_rtnd_, machine_id,
                           new_machine));
  scheduler_->RegisterResource(new_machine, false, true);
  return rd_ptr;
}

void SimulatorBridge::AddMachineSamples(uint64_t current_time) {
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

bool SimulatorBridge::AddTask(const TraceTaskIdentifier& task_identifier) {
  if (submitted_tasks_.find(task_identifier) != submitted_tasks_.end()) {
    // In the trace, a task is submitted again after a task FAIL, EVICT, KILL
    // or LOST event. We can't exactly replay these events because they depend
    // on scheduling decisions. Instead, in the simulation the runtime of
    // a task is equal to the sum of runtimes of every attempt to run the task
    // in the trace. With this approach, the task will keep resources busy
    // for the same amount of time.
    // We only care about the first SUBMIT event because that's when we start
    // the task. The subsequent SUBMIT events can be ignored.
    VLOG(1) << "Task already submitted: " << task_identifier.job_id << ","
            << task_identifier.task_index;
    return false;
  }
  // We never remove the task identifiers from the submitted_tasks_ set. We are
  // using the set to handle the case in which a task finishes before one of
  // its following SUBMIT events.
  submitted_tasks_.insert(task_identifier);
  JobDescriptor* jd_ptr = FindPtrOrNull(trace_job_id_to_jd_,
                                        task_identifier.job_id);
  if (!jd_ptr) {
    // Add new job to the graph
    jd_ptr = PopulateJob(task_identifier.job_id);
    CHECK_NOTNULL(jd_ptr);
  }

  TaskDescriptor* td_ptr = AddTaskToJob(jd_ptr, task_identifier.task_index);
  // The task binary is used by GenerateRootTaskID to generate a unique
  // root task identifier. Hence, we set it to the trace job id which
  // is unique.
  td_ptr->set_binary(lexical_cast<string>(task_identifier.job_id));
  // TODO(ionel): Populate task with the appropriate type.
  td_ptr->set_task_type(TaskDescriptor::DEVIL);
  if (InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr)) {
    CHECK(InsertIfNotPresent(&task_id_to_identifier_,
                             td_ptr->uid(), task_identifier));
    // Add task to the simulator (job_id, task_index) to TaskDescriptor* map.
    CHECK(InsertIfNotPresent(&trace_task_id_to_td_, task_identifier, td_ptr));
    // Update statistics used by cost models. This must be done prior
    // to adding the job to the scheduler, as costs computed in that step.
    AddTaskStats(task_identifier, td_ptr->uid());
    scheduler_->AddJob(jd_ptr);
  } else {
    // We can end up with duplicate task ids if the there's a hash collision or
    // if there are two jobs with identical ids.
    // In the trace there is only one job that has the same id after it gets
    // restarted. Hence, we can ignore it as it won't significantly affect
    // the results of the experiments.
    num_duplicate_task_ids_++;
    LOG(ERROR) << "Duplicate task id: " << td_ptr->uid() << " for task "
               << task_identifier.job_id << " "
               << task_identifier.task_index;
    if (jd_ptr->root_task().uid() == td_ptr->uid()) {
      // The task was added as root. We can just delete the entire job
      // to clean up the state.
      JobID_t job_id = JobIDFromString(jd_ptr->uuid());
      job_map_->erase(job_id);
      trace_job_id_to_jd_.erase(task_identifier.job_id);
      job_num_tasks_.erase(task_identifier.job_id);
      job_id_to_trace_job_id_.erase(job_id);
    } else {
      // Remove the task from the job.
      RemoveTaskFromSpawned(jd_ptr, *td_ptr);
    }
    return false;
  }
  return true;
}

void SimulatorBridge::AddTaskEndEvent(
    const TraceTaskIdentifier& task_identifier,
    TaskDescriptor* td_ptr) {
  uint64_t* runtime_ptr = FindOrNull(task_runtime_, task_identifier);
  EventDescriptor event_desc;
  event_desc.set_job_id(task_identifier.job_id);
  event_desc.set_task_index(task_identifier.task_index);
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  if (runtime_ptr != NULL) {
    // We can approximate the duration of the task.
    event_manager_->AddEvent(simulated_time_->GetCurrentTimestamp() +
                             *runtime_ptr, event_desc);
    td_ptr->set_finish_time(simulated_time_->GetCurrentTimestamp() +
                            *runtime_ptr);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // last timestamp of the simulation.
    event_manager_->AddEvent(FLAGS_runtime, event_desc);
    td_ptr->set_finish_time(FLAGS_runtime);
  }
}

void SimulatorBridge::AddTaskStats(
    const TraceTaskIdentifier& trace_task_identifier,
    TaskID_t task_id) {
  TraceTaskStats* task_stats =
    FindOrNull(trace_task_id_to_stats_, trace_task_identifier);
  if (!task_stats) {
    // We have no stats for the task.
    LOG(WARNING) << "No stats for " << trace_task_identifier.job_id << ","
                 << trace_task_identifier.task_index << " exist";
    return;
  }
  // XXX(ionel): We do not add statistics about tasks runtime.
  // Hence, the simulations work without knowing tasks' runtime
  // before their first run. I believe this is the correct way
  // to replay the trace.
  knowledge_base_->SetTraceTaskStats(task_id, *task_stats);
  trace_task_id_to_stats_.erase(trace_task_identifier);
}

TaskDescriptor* SimulatorBridge::AddTaskToJob(JobDescriptor* jd_ptr,
                                              uint64_t trace_task_id) {
  CHECK_NOTNULL(jd_ptr);
  TaskDescriptor* root_task = jd_ptr->mutable_root_task();
  TaskDescriptor* new_task;
  if (root_task->has_uid()) {
    new_task = root_task->add_spawned();
    new_task->set_uid(GenerateTaskID(*root_task));
  } else {
    // This is the first task we add for the job. We add it as the root task.
    new_task = root_task;
    new_task->set_uid(GenerateRootTaskID(*jd_ptr));
  }
  // XXX(ionel): HACK! I set the index to the trace task id which I can
  // then access in TraceGenerator to print it instead of the Firmament
  // task id. I can't set it directly as the uid of the task because
  // trace task id are only uid within a job (i.e. they are indices).
  new_task->set_index(trace_task_id);
  new_task->set_state(TaskDescriptor::CREATED);
  new_task->set_job_id(jd_ptr->uuid());
  // XXX(ionel): HACK! I set the name of the task to the hacked name of the job
  // so that we can reconstruct the job trace id of the task in the
  // TraceGenerator.
  // TODO(ionel): Add an optional trace task and job id fields to TaskDescriptor
  // protobuf.
  new_task->set_name(jd_ptr->name());
  return new_task;
}

void SimulatorBridge::LoadTraceData(TraceLoader* trace_loader) {
  // Load all the machine events.
  multimap<uint64_t, EventDescriptor> machine_events;
  trace_loader->LoadMachineEvents(&machine_events);
  for (auto& machine_event : machine_events) {
    event_manager_->AddEvent(machine_event.first, machine_event.second);
  }
  // Populate the job_id to number of tasks mapping.
  trace_loader->LoadJobsNumTasks(&job_num_tasks_);
  // Load tasks' runtime.
  trace_loader->LoadTasksRunningTime(&task_runtime_);
  // Populate the knowledge base.
  trace_loader->LoadTaskUtilizationStats(&trace_task_id_to_stats_);
}

void SimulatorBridge::ProcessSimulatorEvents(uint64_t events_up_to_time) {
  while (true) {
    if (event_manager_->GetTimeOfNextEvent() > events_up_to_time) {
      // Processed all events <= events_up_to_time.
      break;
    }
    pair<uint64_t, EventDescriptor> event = event_manager_->GetNextEvent();
    if (event.second.type() == EventDescriptor::ADD_MACHINE) {
      AddMachine(event.second.machine_id());
    } else if (event.second.type() == EventDescriptor::REMOVE_MACHINE) {
      RemoveMachine(event.second.machine_id());
    } else if (event.second.type() == EventDescriptor::UPDATE_MACHINE) {
      // TODO(ionel): Handle machine update event.
    } else if (event.second.type() == EventDescriptor::TASK_END_RUNTIME) {
      TraceTaskIdentifier task_identifier;
      task_identifier.task_index = event.second.task_index();
      task_identifier.job_id = event.second.job_id();
      TaskCompleted(task_identifier);
    } else if (event.second.type() == EventDescriptor::MACHINE_HEARTBEAT) {
      AddMachineSamples(event.first);
    } else if (event.second.type() == EventDescriptor::TASK_SUBMIT) {
      TraceTaskIdentifier task_identifier;
      task_identifier.task_index = event.second.task_index();
      task_identifier.job_id = event.second.job_id();
      AddTask(task_identifier);
    } else {
      LOG(FATAL) << "Unexpected event type " << event.second.type() << " @ "
                 << event.first;
    }
  }
}

void SimulatorBridge::TaskCompleted(
    const TraceTaskIdentifier& task_identifier) {
  TaskDescriptor* td_ptr = FindPtrOrNull(trace_task_id_to_td_, task_identifier);
  CHECK_NOTNULL(td_ptr);
  TaskFinalReport report;
  scheduler_->HandleTaskCompletion(td_ptr, &report);
  scheduler_->HandleTaskFinalReport(report, td_ptr);
  JobDescriptor* jd_ptr =
    FindOrNull(*job_map_, JobIDFromString(td_ptr->job_id()));
  // Don't delete the root task so that tasks can still be appended to the job.
  // The root task is only deleted when the job completes.
  // XXX(ionel): Do not erase the tasks here if we want to run simulations that
  // allow tasks to spawn other tasks.
  if (td_ptr != jd_ptr->mutable_root_task()) {
    task_map_->erase(td_ptr->uid());
  }
  knowledge_base_->EraseTraceTaskStats(td_ptr->uid());
  // Check if it was the last task of the job.
  uint64_t* num_tasks = FindOrNull(job_num_tasks_, task_identifier.job_id);
  CHECK_NOTNULL(num_tasks);
  if (*num_tasks == 0) {
    scheduler_->HandleJobCompletion(JobIDFromString(td_ptr->job_id()));
  }
}

void SimulatorBridge::OnJobCompletion(JobID_t job_id) {
  uint64_t* trace_job_id = FindOrNull(job_id_to_trace_job_id_, job_id);
  CHECK_NOTNULL(trace_job_id);
  uint64_t* num_tasks = FindOrNull(job_num_tasks_, *trace_job_id);
  CHECK_EQ(*num_tasks, 0) << "There are tasks that haven't finished";
  JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd_ptr);
  // Delete the root task.
  task_map_->erase(jd_ptr->root_task().uid());
  job_map_->erase(job_id);
  trace_job_id_to_jd_.erase(*trace_job_id);
  job_num_tasks_.erase(*trace_job_id);
  job_id_to_trace_job_id_.erase(job_id);
}

void SimulatorBridge::OnSchedulingDecisionsCompletion(uint64_t timestamp) {
  ProcessSimulatorEvents(timestamp);
}

void SimulatorBridge::OnTaskCompletion(TaskDescriptor* td_ptr,
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

void SimulatorBridge::OnTaskEviction(TaskDescriptor* td_ptr,
                                     ResourceDescriptor* rd_ptr) {
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  uint64_t task_end_time = td_ptr->finish_time();
  td_ptr->set_start_time(0);
  event_manager_->RemoveTaskEndRuntimeEvent(*ti_ptr, task_end_time);
}

void SimulatorBridge::OnTaskFailure(TaskDescriptor* td_ptr,
                                    ResourceDescriptor* rd_ptr) {
  // We don't have to do anything because simulated tasks do not fail.
  LOG(FATAL) << "Task should not fail in the simulator";
}

void SimulatorBridge::OnTaskMigration(TaskDescriptor* td_ptr,
                                      ResourceDescriptor* rd_ptr) {
  // XXX(ionel): This assumes that the Migration is done via two calls.
  // First, a call to OnTaskEviction and then OnTaskMigration.
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  uint64_t task_end_time = td_ptr->finish_time();
  td_ptr->set_start_time(simulated_time_->GetCurrentTimestamp());
  event_manager_->RemoveTaskEndRuntimeEvent(*ti_ptr, task_end_time);
  AddTaskEndEvent(*ti_ptr, td_ptr);
}

void SimulatorBridge::OnTaskPlacement(TaskDescriptor* td_ptr,
                                      ResourceDescriptor* rd_ptr) {
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  td_ptr->set_start_time(simulated_time_->GetCurrentTimestamp());
  AddTaskEndEvent(*ti_ptr, td_ptr);
}

JobDescriptor* SimulatorBridge::PopulateJob(uint64_t trace_job_id) {
  JobDescriptor jd;
  // Generate a hash out of the trace job_id.
  JobID_t new_job_id = GenerateJobID(trace_job_id);

  CHECK(InsertIfNotPresent(job_map_.get(), new_job_id, jd));
  // Get the new value of the pointer because jd has been copied.
  JobDescriptor* jd_ptr = FindOrNull(*job_map_, new_job_id);

  // Maintain a mapping between the trace job_id and the generated job_id.
  jd_ptr->set_uuid(to_string(new_job_id));
  // Set the name of the job to a string that denotes we're running a simulation
  // continued by the trace job id. We use the name to generate a simulation
  // trace that uses trace job ids.
  jd_ptr->set_name("firmament_simulation_job_" +
                   lexical_cast<string>(trace_job_id));
  if (!InsertIfNotPresent(&trace_job_id_to_jd_, trace_job_id, jd_ptr)) {
    LOG(ERROR) << "Job " << trace_job_id << " has already been populated";
  } else {
    InsertOrUpdate(&job_id_to_trace_job_id_, new_job_id, trace_job_id);
  }
  return jd_ptr;
}

void SimulatorBridge::RemoveMachine(uint64_t machine_id) {
  ResourceTopologyNodeDescriptor* rtnd_ptr =
    FindPtrOrNull(trace_machine_id_to_rtnd_, machine_id);
  CHECK_NOTNULL(rtnd_ptr);
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  machine_res_id_pus_.erase(res_id);
  // Traverse the resource topology tree in order to evict tasks and
  // remove resources from resource_map.
  DFSTraverseResourceProtobufTreeReturnRTND(
      rtnd_ptr, boost::bind(&SimulatorBridge::RemoveResource, this, _1));
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

void SimulatorBridge::RemoveResource(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  // TODO(ionel): Move logic to event_driven_scheduler once DeregisterResource
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

void SimulatorBridge::RemoveResourceNodeFromParentChildrenList(
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

void SimulatorBridge::RemoveTaskFromSpawned(
    JobDescriptor* jd_ptr,
    const TaskDescriptor& td_to_remove) {
  TaskDescriptor* root_td_ptr = jd_ptr->mutable_root_task();
  RepeatedPtrField<TaskDescriptor>* spawned = root_td_ptr->mutable_spawned();
  int32_t index = 0;
  for (auto& td : *spawned) {
    if (td.uid() == td_to_remove.uid()) {
      break;
    }
    ++index;
  }
  if (index < spawned->size()) {
    if (index < spawned->size() - 1) {
      spawned->SwapElements(index, spawned->size() - 1);
    }
    spawned->RemoveLast();
  } else {
    LOG(FATAL) << "Could not find task among job's tasks";
  }
}

void SimulatorBridge::SetupMachine(
    ResourceTopologyNodeDescriptor* rtnd,
    ResourceVector* machine_res_cap,
    const string& hostname,
    const string& root_uuid,
    ResourceID_t machine_res_id) {
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
  CHECK(InsertIfNotPresent(
      resource_map_.get(),
      ResourceIDFromString(rd->uuid()),
      new ResourceStatus(rd, rtnd, "endpoint_uri",
                         simulated_time_->GetCurrentTimestamp())));
  if (rd->type() == ResourceDescriptor::RESOURCE_PU) {
    uint64_t cpu_cores = 1;
    if (machine_res_cap->has_cpu_cores()) {
      cpu_cores = machine_res_cap->cpu_cores() + 1;
    }
    // NOTE: We set the number of cpu_cores to the number of PUs.
    machine_res_cap->set_cpu_cores(cpu_cores);
    machine_res_id_pus_.insert(
        pair<ResourceID_t, ResourceDescriptor*>(machine_res_id, rd));
  }
}

void SimulatorBridge::ScheduleJobs(SchedulerStats* scheduler_stats) {
  scheduler_->ScheduleAllJobs(scheduler_stats);
}

} // namespace sim
} // namespace firmament
