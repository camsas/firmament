/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

// Bridge between the simulator and Firmament.

#ifndef FIRMAMENT_SIM_SIMULATOR_BRIDGE_H
#define FIRMAMENT_SIM_SIMULATOR_BRIDGE_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "misc/trace_generator.h"
#include "platforms/sim/simulated_messaging_adapter.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/scheduling_event_notifier_interface.h"
#include "sim/dfs/simulated_data_layer_manager.h"
#include "sim/event_manager.h"
#include "sim/interference/task_interference_interface.h"
#include "sim/knowledge_base_simulator.h"
#include "sim/simulated_wall_time.h"
#include "sim/trace_loader.h"
#include "sim/trace_utils.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace sim {

using scheduler::SchedulerStats;

class SimulatorBridge : public scheduler::SchedulingEventNotifierInterface {
 public:
  SimulatorBridge(EventManager* event_manager,
                  SimulatedWallTime* simulated_time);
  virtual ~SimulatorBridge();

  /**
   * Add new machine to the topology. The method updates simulator's mapping
   * state.
   * @param machine_id the simulator machine id
   * @return a pointer to the resource descriptor of the new machine
   */
  ResourceDescriptor* AddMachine(uint64_t machine_id);

  /**
   * Adds machine perf statistics to the knowledge base for every machine in the
   * trace.
   * @param current_time current simulation time
   */
  void AddMachineSamples(uint64_t current_time);

  /**
   * Adds a new task to the flow graph. Updates the internal mappings as well.
   * @param task_identifier the simulator task identifier
   * @param event_desc a descriptor of the event
   * @return true if the task has been added.
   */
  bool AddTask(const TraceTaskIdentifier& task_identifier,
               const EventDescriptor& event_desc);

  void LoadTraceData(TraceLoader* trace_loader);

  /**
   * Event called by the event driven scheduler upon job completion.
   * @param job_id the id of the completed job
   */
  void OnJobCompletion(JobID_t job_id);
  void OnJobRemoval(JobID_t job_id);

  /**
   * Event called by the event driven scheduler when the scheduler finishes
   * making decisions where to place tasks (i.e. when the solver finishes for
   * the flow_scheduler).
   * This method is to be used to apply the simulation changes that happen
   * while the scheduler is deciding where to place tasks.
   * @param scheduler_start_time the timestamp when the scheduler started
   * @param scheduler_runtime the time it took for the scheduler to complete
   */
  void OnSchedulingDecisionsCompletion(uint64_t scheduler_start_time,
                                       uint64_t scheduler_runtime);

  /**
   * Event called by the event driven scheduler upon task completion.
   * @param td_ptr the descriptor of the completed task
   * @param rd_ptr the descriptor of the resource on which the task was running
   */
  void OnTaskCompletion(TaskDescriptor* td_ptr,
                        ResourceDescriptor* rd_ptr);

  /**
   * Event called by the event driven scheduler upon task eviction.
   * @param td_ptr the descriptor of the evicted task
   * @param rd_ptr the descriptor of the resource on which the task was running
   */
  void OnTaskEviction(TaskDescriptor* td_ptr,
                      ResourceDescriptor* rd_ptr);

  /**
   * Event called by the event driven scheduler upon task failure.
   * @param td_ptr the descriptor of the failed task
   * @param rd_ptr the descriptor of the resource on which the task was running
   */
  void OnTaskFailure(TaskDescriptor* td_ptr,
                     ResourceDescriptor* rd_ptr);

  /**
   * Event called by the event driven scheduler upon task migration.
   * @param td_ptr the descriptor of the migrated task
   * @param old_rd_ptr descriptor of the resource from which task is migrated
   * @param rd_ptr the descriptor of the resource to which the task is migrated
   */
  void OnTaskMigration(TaskDescriptor* td_ptr,
                       ResourceDescriptor* old_rd_ptr,
                       ResourceDescriptor* rd_ptr);

  /**
   * Event called by the event driven scheduler upon task placement.
   * @param td_ptr the descriptor of the placed task
   * @param rd_ptr the descriptor of the resource on which the task was placed
   */
  void OnTaskPlacement(TaskDescriptor* td_ptr,
                       ResourceDescriptor* rd_ptr);

  /**
   * Processes all the simulator events that happen at a given time.
   * @param cur_time the timestamp for which to process the simulator events
   */
  void ProcessSimulatorEvents(uint64_t events_up_to_time);

  /**
   * Removes a machine from the topology and all its associated state.
   * NOTE: The method currently assumes that the machine is directly
   * connected to the topology root.
   * @param machine_id the simulator id of the machine to be removed
   */
  void RemoveMachine(uint64_t machine_id);

  void ScheduleJobs(SchedulerStats* scheduler_stats);

  /**
   * Notifies the flow_graph of a task completion and updates the simulator's
   * state.
   * @param task_identifier the simulator identifier of the completed task
   */
  void TaskCompleted(const TraceTaskIdentifier& task_identifier);

  uint64_t get_num_duplicate_task_ids() {
    return num_duplicate_task_ids_;
  }

  unordered_map<uint64_t, uint64_t>* job_num_tasks() {
    return &job_num_tasks_;
  }

 private:
  FRIEND_TEST(SimulatorBridgeTest, AddMachine);
  FRIEND_TEST(SimulatorBridgeTest, AddTask);
  FRIEND_TEST(SimulatorBridgeTest, OnJobCompletion);
  FRIEND_TEST(SimulatorBridgeTest, OnTaskCompletion);
  FRIEND_TEST(SimulatorBridgeTest, OnTaskEviction);
  FRIEND_TEST(SimulatorBridgeTest, OnTaskPlacement);
  FRIEND_TEST(SimulatorBridgeTest, RemoveMachine);

  /**
   * Add task statistics to the knowledge base.
   * @param trace_task_identifier the trace identifier of the task
   * @param task_id the Firmament task id
   */
  void AddTaskStats(const TraceTaskIdentifier& trace_task_identifier,
                    TaskID_t task_id);

  /**
   * Creates a new task for a job.
   * @param jd_ptr the job descriptor of the job for which to create a new task
   * @param task_identifier the trace identifier of the task
   * @return a pointer to the task descriptor of the new task
   */
  TaskDescriptor* AddTaskToJob(JobDescriptor* jd_ptr,
                               const TraceTaskIdentifier& task_identifier);

  /**
   * Create and populate a new job.
   * @param job_id the simulator job id
   * @return a pointer to the job descriptor
   */
  JobDescriptor* PopulateJob(uint64_t job_id);

  /**
   * Removes a spawned task from the job's spanwed list.
   * @param jd_ptr the descriptor of the job
   * @param td_to_remove the descriptor of the task to be removed
   */
  void RemoveTaskFromSpawned(JobDescriptor* jd_ptr,
                             const TaskDescriptor& td_to_remove);

  /**
   * The resource topology is built from the same protobuf file. The function
   * changes the uuids to make sure that there's no two identical uuids.
   */
  void SetupMachine(ResourceTopologyNodeDescriptor* rtnd,
                    ResourceVector* machine_res_cap,
                    const string& hostname,
                    uint64_t trace_machine_id,
                    const string& root_uuid,
                    const string& old_machine_res_id);

  /**
   * Helper method that updates TASK_END_RUNTIME events for tasks whose end
   * time has updated, removes TASK_END_RUNTIME events for preempted tasks, and
   * adds TASK_END_RUNTIME events for placed tasks.
   */
  void UpdateTaskEndEvents(const vector<TaskEndRuntimes>& tasks_end_time);

  EventManager* event_manager_;
  SimulatedWallTime* simulated_time_;

  // Map used to convert between the simulator job_ids and the Firmament
  // job descriptors.
  unordered_map<uint64_t, JobDescriptor*> trace_job_id_to_jd_;
  // Map used to convert between the Firmament job id and the trace job id.
  unordered_map<JobID_t, uint64_t, boost::hash<boost::uuids::uuid> >
    job_id_to_trace_job_id_;

  // Map from JobID_t to JobDescriptor
  shared_ptr<JobMap_t> job_map_;

  // Map holding the number of tasks for each job.
  unordered_map<uint64_t, uint64_t> job_num_tasks_;
  unordered_map<uint64_t, uint64_t> immutable_job_num_tasks_;

  SimulatedDataLayerManager* data_layer_manager_;

  shared_ptr<KnowledgeBaseSimulator> knowledge_base_;

  // Multimap storing the mapping between machine resource ids and their PU
  // resource descriptors.
  multimap<ResourceID_t, ResourceDescriptor*> machine_res_id_pus_;

  platform::sim::SimulatedMessagingAdapter<BaseMessage>* messaging_adapter_;

  // Map from ResourceID_t to ResourceStatus*
  shared_ptr<ResourceMap_t> resource_map_;

  // The root node of the machine topology.
  ResourceTopologyNodeDescriptor rtn_root_;

  scheduler::SchedulerInterface* scheduler_;

  // Map from Firmament TaskID_t to simulator trace task identifier.
  unordered_map<TaskID_t, TraceTaskIdentifier> task_id_to_identifier_;

  // Map from TaskID_t to TaskDescriptor*
  shared_ptr<TaskMap_t> task_map_;

  // Map holding the per-task runtime information
  unordered_map<TaskID_t, uint64_t> task_runtime_;

  // Map from the simulator machine id to the Firmament rtnd.
  unordered_map<uint64_t,
    ResourceTopologyNodeDescriptor*> trace_machine_id_to_rtnd_;

  unordered_map<TaskID_t, TraceTaskStats> task_id_to_stats_;

  // Map used to convert between the simulator task_ids and the Firmament
  // task descriptors.
  unordered_map<TraceTaskIdentifier, TaskDescriptor*, TraceTaskIdentifierHasher>
      trace_task_id_to_td_;
  // Set storing all the submitted tasks.
  unordered_set<TraceTaskIdentifier,
    TraceTaskIdentifierHasher> submitted_tasks_;

  // Map used to convert between the new uuids assigned to the machine nodes and
  // the old uuids read from the machine topology file.
  unordered_map<string, string> uuid_conversion_map_;
  // The template topology descriptor of the new machine.
  ResourceTopologyNodeDescriptor machine_tmpl_;
  // Counter used to store the number of duplicate task ids seed in the trace.
  uint64_t num_duplicate_task_ids_;
  // Object used to get task interference information.
  TaskInterferenceInterface* task_interference_model_;
  TraceGenerator* trace_generator_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMULATOR_BRIDGE_H
