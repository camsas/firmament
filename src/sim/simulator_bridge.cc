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
#include "sim/dfs/simulated_data_layer_manager.h"
#include "sim/knowledge_base_simulator.h"
#include "sim/trace_loader.h"
#include "storage/simple_object_store.h"

#define TIME_ZERO_TOTAL_UNSCHEDULED_TIME 600000000
using boost::lexical_cast;
using boost::hash;

DECLARE_uint64(runtime);
DECLARE_string(scheduler);
DECLARE_uint64(sim_machine_max_ram);
DECLARE_int32(flow_scheduling_cost_model);

namespace firmament {
namespace sim {

SimulatorBridge::SimulatorBridge(EventManager* event_manager,
                                 SimulatedWallTime* simulated_time)
    : event_manager_(event_manager), simulated_time_(simulated_time),
    job_map_(new JobMap_t),
    resource_map_(new ResourceMap_t), task_map_(new TaskMap_t),
    num_duplicate_task_ids_(0) {
  trace_generator_ = new TraceGenerator(simulated_time_);
  if (FLAGS_flow_scheduling_cost_model == 3) {
    // We're running Quincy => simulate the DFS.
    data_layer_manager_ = new SimulatedDataLayerManager(trace_generator_);
  } else {
    data_layer_manager_ = NULL;
  }
  knowledge_base_ = shared_ptr<KnowledgeBaseSimulator>(
      new KnowledgeBaseSimulator(data_layer_manager_));
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
        simulated_time_, trace_generator_);
  } else {
    scheduler_ = new scheduler::SimpleScheduler(
        job_map_, resource_map_, &rtn_root_,
        shared_ptr<store::ObjectStoreInterface>(
            new store::SimpleObjectStore(root_uuid)),
        task_map_, knowledge_base_,
        shared_ptr<machine::topology::TopologyManager>(
            new machine::topology::TopologyManager),
        messaging_adapter_, this, root_uuid, "http://localhost",
        simulated_time_, trace_generator_);
  }
  // Import a fictional machine resource topology
  LoadMachineTemplate(&machine_tmpl_);
  scheduler_->RegisterResource(&rtn_root_, false, true);
}

SimulatorBridge::~SimulatorBridge() {
  delete trace_generator_;
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
  if (data_layer_manager_) {
    delete data_layer_manager_;
  }
}

ResourceDescriptor* SimulatorBridge::AddMachine(
    uint64_t machine_id) {
  // Create a new machine topology descriptor.
  ResourceTopologyNodeDescriptor* new_machine = rtn_root_.add_children();
  new_machine->CopyFrom(machine_tmpl_);
  const string& root_uuid = rtn_root_.resource_desc().uuid();
  string hostname = "firmament_simulation_machine_" +
    lexical_cast<string>(machine_id);
  ResourceDescriptor* rd_ptr = new_machine->mutable_resource_desc();
  rd_ptr->set_friendly_name(hostname);
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  rd_ptr->set_trace_machine_id(machine_id);
  ResourceVector* res_cap = rd_ptr->mutable_resource_capacity();
  // TODO(ionel): Do not manually set ram_cap! Update the machine protobuf
  // to include resource capacity values.
  res_cap->set_ram_cap(FLAGS_sim_machine_max_ram);
  DFSTraverseResourceProtobufTreeReturnRTND(
      new_machine, boost::bind(&SimulatorBridge::SetupMachine,
                               this, _1, res_cap, hostname, machine_id,
                               root_uuid, rd_ptr->uuid()));
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

bool SimulatorBridge::AddTask(const TraceTaskIdentifier& task_identifier,
                              const EventDescriptor& event_desc) {
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

  TaskDescriptor* td_ptr = AddTaskToJob(jd_ptr, task_identifier);
  td_ptr->mutable_resource_request()->set_cpu_cores(
      event_desc.requested_cpu_cores());
  td_ptr->mutable_resource_request()->set_ram_cap(event_desc.requested_ram());
  if (event_desc.has_priority()) {
    td_ptr->set_priority(event_desc.priority());
  }
  if (InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr)) {
    CHECK(InsertIfNotPresent(&task_id_to_identifier_,
                             td_ptr->uid(), task_identifier));
    // Add task to the simulator (job_id, task_index) to TaskDescriptor* map.
    CHECK(InsertIfNotPresent(&trace_task_id_to_td_, task_identifier, td_ptr));
    // Update statistics used by cost models. This must be done prior
    // to adding the job to the scheduler, as costs computed in that step.
    AddTaskStats(task_identifier, td_ptr->uid());
    // We can only set the type of the task after we've added the stats.
    knowledge_base_->SetTaskType(td_ptr);
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
      immutable_job_num_tasks_.erase(task_identifier.job_id);
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
  uint64_t* runtime_ptr = FindOrNull(task_runtime_, td_ptr->uid());
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
  TraceTaskStats* task_stats = FindOrNull(task_id_to_stats_, task_id);
  if (!task_stats) {
    // We have no stats for the task.
    LOG(WARNING) << "No stats for " << trace_task_identifier.job_id << ","
                 << trace_task_identifier.task_index << " exist";
    return;
  }
  // NOTE: We do not add runtime statistics./ Hence, the simulations
  // work without knowing tasks' runtime before their first run. I believe this
  // is the correct way to replay the trace.
  knowledge_base_->SetTraceTaskStats(task_id, *task_stats);
  task_id_to_stats_.erase(task_id);
}

TaskDescriptor* SimulatorBridge::AddTaskToJob(
    JobDescriptor* jd_ptr,
    const TraceTaskIdentifier& task_identifier) {
  CHECK_NOTNULL(jd_ptr);
  TaskDescriptor* root_task = jd_ptr->mutable_root_task();
  TaskDescriptor* new_task;
  if (root_task->has_uid()) {
    new_task = root_task->add_spawned();
    new_task->set_uid(GenerateTaskIDFromTraceIdentifier(task_identifier));
  } else {
    // This is the first task we add for the job. We add it as the root task.
    new_task = root_task;
    new_task->set_uid(GenerateTaskIDFromTraceIdentifier(task_identifier));
  }
  new_task->set_state(TaskDescriptor::CREATED);
  new_task->set_job_id(jd_ptr->uuid());
  // Set the submission timestamp for the task.
  new_task->set_submit_time(simulated_time_->GetCurrentTimestamp());
  new_task->set_trace_job_id(task_identifier.job_id);
  new_task->set_trace_task_id(task_identifier.task_index);
  if (simulated_time_->GetCurrentTimestamp() == 0) {
    // XXX(ionel): HACK! We set the total_unscheduled_time for tasks
    // created at time zero. By setting it to a high value we make sure that
    // all the tasks get scheduled in the first solver run when using
    // time-dependent cost models.
    new_task->set_total_unscheduled_time(TIME_ZERO_TOTAL_UNSCHEDULED_TIME);
  }
  TaskID_t task_id = new_task->uid();
  // The task binary is used by GenerateRootTaskID to generate a unique
  // root task identifier. Hence, we set it to the trace job id which
  // is unique.
  new_task->set_binary(lexical_cast<string>(task_identifier.job_id));
  // Add a dependency for the task.
  if (data_layer_manager_) {
    ReferenceDescriptor* dependency =  new_task->add_dependencies();
    uint64_t* runtime_ptr = FindOrNull(task_runtime_, task_id);
    uint64_t input_size = 0;
    if (runtime_ptr) {
      uint64_t* num_tasks =
        FindOrNull(immutable_job_num_tasks_, task_identifier.job_id);
      CHECK_NOTNULL(num_tasks);
      input_size =
        data_layer_manager_->AddFilesForTask(*new_task, *runtime_ptr, false,
                                             *num_tasks);
    } else {
      // The task didn't finish in the trace => it is a long running
      // service job. Inform the DFS that the task should not have
      // any input data.
      input_size = data_layer_manager_->AddFilesForTask(*new_task, 0, true, 0);
    }
    // XXX(ionel): Remove the set_id hack once we get rid of DataObjects.
    char buffer[DIOS_NAME_BYTES] = {0};
    memcpy(&buffer, &task_id, sizeof(task_id));
    dependency->set_id(buffer, DIOS_NAME_BYTES);
    dependency->set_type(ReferenceDescriptor::CONCRETE);
    dependency->set_size(input_size);
    dependency->set_location(to_string(task_id));
  }
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
  trace_loader->LoadJobsNumTasks(&immutable_job_num_tasks_);
  // Load tasks' runtime.
  trace_loader->LoadTasksRunningTime(&task_runtime_);
  // Populate the knowledge base.
  trace_loader->LoadTaskUtilizationStats(&task_id_to_stats_);
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
      AddTask(task_identifier, event.second);
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
  knowledge_base_->PopulateTaskFinalReport(td_ptr, &report);
  scheduler_->HandleTaskFinalReport(report, td_ptr);
  if (data_layer_manager_) {
    data_layer_manager_->RemoveFilesForTask(*td_ptr);
  }
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
  immutable_job_num_tasks_.erase(*trace_job_id);
  job_id_to_trace_job_id_.erase(job_id);
}

void SimulatorBridge::OnSchedulingDecisionsCompletion(
    uint64_t scheduler_start_time, uint64_t scheduler_runtime) {
  // We only need to process the simulator events that happened while the
  // scheduler was running when we're in online mode. In batch mode, we assume
  // that the scheduler runs in "no time" in order to avoid it overtaking the
  // batch steps.
  if (FLAGS_batch_step == 0)
    ProcessSimulatorEvents(scheduler_start_time + scheduler_runtime);
}

void SimulatorBridge::OnTaskCompletion(TaskDescriptor* td_ptr,
                                       ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  td_ptr->set_total_run_time(UpdateTaskTotalRunTime(*td_ptr));
  TraceTaskIdentifier* ti_ptr = FindOrNull(task_id_to_identifier_, task_id);
  CHECK_NOTNULL(ti_ptr);
  trace_task_id_to_td_.erase(*ti_ptr);
  task_runtime_.erase(task_id);
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
  TaskID_t task_id = td_ptr->uid();
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, task_id);
  CHECK_NOTNULL(ti_ptr);
  uint64_t task_end_time = td_ptr->finish_time();
  uint64_t task_executed_for =
    simulated_time_->GetCurrentTimestamp() - td_ptr->start_time();
  td_ptr->set_total_run_time(UpdateTaskTotalRunTime(*td_ptr));
  uint64_t* runtime_ptr = FindOrNull(task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // NOTE: We assume that the work conducted by a task until eviction is
    // saved. Hence, we update the time the task has left to run.
    InsertOrUpdate(&task_runtime_, task_id, *runtime_ptr - task_executed_for);
  } else {
    // The task didn't finish in the trace.
  }
  td_ptr->clear_start_time();
  td_ptr->set_submit_time(simulated_time_->GetCurrentTimestamp());
  event_manager_->RemoveTaskEndRuntimeEvent(*ti_ptr, task_end_time);
}

void SimulatorBridge::OnTaskFailure(TaskDescriptor* td_ptr,
                                    ResourceDescriptor* rd_ptr) {
  // We don't have to do anything because simulated tasks do not fail.
  LOG(FATAL) << "Task should not fail in the simulator";
}

void SimulatorBridge::OnTaskMigration(TaskDescriptor* td_ptr,
                                      ResourceDescriptor* rd_ptr) {
  td_ptr->set_submit_time(simulated_time_->GetCurrentTimestamp());
  td_ptr->set_start_time(simulated_time_->GetCurrentTimestamp());
  // We don't have to update the end event because we assume that it
  // takes no time to migrate a task.
}

void SimulatorBridge::OnTaskPlacement(TaskDescriptor* td_ptr,
                                      ResourceDescriptor* rd_ptr) {
  TraceTaskIdentifier* ti_ptr =
    FindOrNull(task_id_to_identifier_, td_ptr->uid());
  CHECK_NOTNULL(ti_ptr);
  td_ptr->set_start_time(simulated_time_->GetCurrentTimestamp());
  td_ptr->set_total_unscheduled_time(UpdateTaskTotalUnscheduledTime(*td_ptr));
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
  scheduler_->DeregisterResource(rtnd_ptr);
  trace_machine_id_to_rtnd_.erase(machine_id);
  // We only free the ResourceTopologyNodeDescriptor in the destructor.
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
    uint64_t trace_machine_id,
    const string& root_uuid,
    const string& old_machine_res_id) {
  string new_uuid;
  if (rtnd->has_parent_id()) {
    // This is an intermediate node, so translate the parent UUID via the
    // lookup table
    const string& old_parent_id = rtnd->parent_id();
    string* new_parent_id = FindOrNull(uuid_conversion_map_, rtnd->parent_id());
    CHECK_NOTNULL(new_parent_id);
    VLOG(3) << "Resetting parent UUID for " << rtnd->resource_desc().uuid()
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
  VLOG(3) << "Resetting UUID for " << rtnd->resource_desc().uuid() << " to "
          << new_uuid;
  InsertOrUpdate(&uuid_conversion_map_, rtnd->resource_desc().uuid(), new_uuid);
  ResourceDescriptor* rd = rtnd->mutable_resource_desc();
  rd->set_uuid(new_uuid);
  rd->set_trace_machine_id(trace_machine_id);
  // Add the resource node to the map.
  CHECK(InsertIfNotPresent(
      resource_map_.get(),
      ResourceIDFromString(rd->uuid()),
      new ResourceStatus(rd, rtnd, "endpoint_uri",
                         simulated_time_->GetCurrentTimestamp())));
  if (rd->type() == ResourceDescriptor::RESOURCE_PU) {
    string* new_machine_res_id = FindOrNull(uuid_conversion_map_,
                                            old_machine_res_id);
    CHECK_NOTNULL(new_machine_res_id);
    ResourceID_t machine_res_id = ResourceIDFromString(*new_machine_res_id);
    float cpu_cores = 1;
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

uint64_t SimulatorBridge::UpdateTaskTotalRunTime(const TaskDescriptor& td) {
  uint64_t task_executed_for =
    simulated_time_->GetCurrentTimestamp() - td.start_time();
  if (td.has_total_run_time()) {
    return td.total_run_time() + task_executed_for;
  } else {
    return task_executed_for;
  }
}

} // namespace sim
} // namespace firmament
