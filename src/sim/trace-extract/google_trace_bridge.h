// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google trace simulator bridge between the simulator and Firmament.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_BRIDGE_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_BRIDGE_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "sim/knowledge_base_simulator.h"
#include "sim/trace-extract/google_trace_event_manager.h"
#include "sim/trace-extract/google_trace_utils.h"

namespace firmament {
namespace sim {

class GoogleTraceBridge {
 public:
  GoogleTraceBridge(const string& trace_path,
                    GoogleTraceEventManager* event_manager);
  virtual ~GoogleTraceBridge();

  /**
   * Add new machine to the topology. The method updates simulator's mapping
   * state.
   * @param machine_tmpl the template topology descriptor of the new machine
   * @param machine_id the google trace machine id
   * @return a pointer to the resource descriptor of the new machine
   */
  ResourceDescriptor* AddMachine(
      const ResourceTopologyNodeDescriptor& machine_tmpl, uint64_t machine_id);

  /**
   * Adds a new task to the flow graph. Updates the internal mappings as well.
   * @param task_identifier the google trace identifier of the task
   * @return a pointer to the descriptor of the task
   */
  TaskDescriptor* AddTask(const TaskIdentifier& task_identifier);

  /**
   * Populate and add the root node of the topology.
   */
  void CreateRootResource();

  shared_ptr<FlowGraph> flow_graph() {
    return flow_graph_;
  }

  void LoadTraceData(ResourceTopologyNodeDescriptor* machine_tmpl);

  /**
   * Removes a machine from the topology and all its associated state.
   * NOTE: The method currently assumes that the machine is directly
   * connected to the topology root.
   * @param machine_id the Google id of the machine to be removed
   */
  void RemoveMachine(uint64_t machine_id);

  multimap<uint64_t, uint64_t>* RunSolver(double *algorithm_time,
                                          double *flow_solver_time,
                                          FILE *graph_output);

  /**
   * Notifies the flow_graph of a task completion and updates the simulator's
   * state.
   * @param task_identifier the simulator identifier of the completed task
   */
  void TaskCompleted(const TaskIdentifier& task_identifier);

  void UpdateFlowGraph(
    uint64_t scheduling_timestamp,
    multimap<uint64_t, uint64_t>* task_mappings);
  void UpdateResourceStats();

 private:
  /**
   * Compute and add task completion event to the simulator's events.
   * @param cur_timestamp the timestap when the task starts running
   * @param task_id the Firmament task id
   * @param task_identifier the simulator task identifier
   * @param task_runtime the mapping between tasks and runtimes
   */
  void AddTaskEndEvent(
      uint64_t cur_timestamp, const TaskID_t& task_id,
      const TaskIdentifier& task_identifier);

  void AddTaskStats(const TaskIdentifier& task_identifier);

  /**
   * Populate the knowledge base for each equivalence class with statistics.
   * @param task_identifier the task for which we populate the knowledge base
   * @param runtime the runtime of the task
   * @param task_stats struct containg various task statistics (e.g. men_usage)
   * @param task_equiv_classes vector of equivalence classes for which to
   * populate the knowledge base
   */
  void AddTaskStatsToKnowledgeBase(
      const TaskIdentifier& task_identifier, double runtime,
      const TaskStats& task_stats,
      const vector<EquivClass_t>& task_equiv_classes);

  /**
   * Creates a new task for a job.
   * @param jd_ptr the job descriptor of the job for which to create a new task
   * @return a pointer to the task descriptor of the new task
   */
  TaskDescriptor* AddTaskToJob(JobDescriptor* jd_ptr);

  void InitializeCostModel();

  /**
   * Notifies the flow graph of job completion and updates the simulator's
   * state.
   * @param simulator_job_id the simulator's job id
   * @param job_id flow graph's job id
   */
  void JobCompleted(uint64_t simulator_job_id, JobID_t job_id);

  /**
   * Create and populate a new job.
   * @param job_id the Google trace job id
   * @return a pointer to the job descriptor
   */
  JobDescriptor* PopulateJob(uint64_t job_id);

  /**
   * Removes the simulator state for a resource and evicts all the tasks
   * running on it or any of its children nodes.
   * @param rtnd the resource to be removed
   */
  void RemoveResource(ResourceTopologyNodeDescriptor* rtnd);

  /**
   * Removes a resource from its parent's children list.
   * @param rtnd the resource node to remove
   */
  void RemoveResourceNodeFromParentChildrenList(
      const ResourceTopologyNodeDescriptor& rtnd);

  void RemoveTaskStats(TaskID_t task_id);

  /**
   * The resource topology is built from the same protobuf file. The function
   * changes the uuids to make sure that there's no two identical uuids.
   */
  void ResetUuidAndAddResource(ResourceTopologyNodeDescriptor* rtnd,
                               const string& hostname, const string& root_uuid);

  /**
   * Evicts a task from a resource and updates the state of the simulator
   * and of the flow graph.
   * @param task_id the id of the evited task
   * @param res_id the id of the resource from which to evict the task
   */
  void TaskEvicted(TaskID_t task_id, const ResourceID_t& res_id);

  /**
   * Updates simulator's state upon the eviction of a task.
   **/
  void TaskEvictedClearSimulatorState(TaskID_t task_id,
                                      uint64_t task_end_time,
                                      const ResourceID_t& res_id,
                                      const TaskIdentifier& task_identifier);

  CostModelInterface* cost_model_;

  GoogleTraceEventManager* event_manager_;

  shared_ptr<FlowGraph> flow_graph_;

  // Map used to convert between the google trace job_ids and the Firmament
  // job descriptors.
  unordered_map<uint64_t, JobDescriptor*> job_id_to_jd_;

  // Map from JobID_t to JobDescriptor
  shared_ptr<JobMap_t> job_map_;

  // Map holding the number of tasks for each job.
  unordered_map<uint64_t, uint64_t> job_num_tasks_;

  KnowledgeBaseSimulator* knowledge_base_;

  // Map used to convert between the google trace machine_ids and the
  // Firmament resource descriptors.
  unordered_map<uint64_t, ResourceDescriptor*> trace_machine_id_to_td_;

  // Map from the Google machine id to the Firmament rtnd.
  unordered_map<uint64_t,
    ResourceTopologyNodeDescriptor*> trace_machine_id_to_rtnd_;

  // Map holding the task_id of the task running on the resource with res_id.
  unordered_map<ResourceID_t, TaskID_t,
      boost::hash<boost::uuids::uuid> > res_id_to_task_id_;

  // Map from ResourceID_t to ResourceStatus*
  shared_ptr<ResourceMap_t> resource_map_;

  // The root node of the machine topology.
  ResourceTopologyNodeDescriptor rtn_root_;

  scheduler::SolverDispatcher* solver_dispatcher_;

  // Map holding the ResourceID_t of every scheduled task.
  unordered_map<TaskID_t, ResourceID_t> task_bindings_;

  // Map holding the end runtime for every running task.
  unordered_map<TaskID_t, uint64_t> task_id_to_end_time_;

  // Map from Firmament TaskID_t to Google trace task identifier.
  unordered_map<TaskID_t, TaskIdentifier> task_id_to_identifier_;

  // Map used to convert between the google trace task_ids and the Firmament
  // task descriptors.
  unordered_map<TaskIdentifier, TaskDescriptor*, TaskIdentifierHasher>
      trace_task_id_to_td_;

  unordered_map<TaskIdentifier, TaskStats, TaskIdentifierHasher>
      trace_task_id_to_stats_;

  // Map from TaskID_t to TaskDescriptor*
  shared_ptr<TaskMap_t> task_map_;

  // Map holding the per-task runtime information
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* task_runtime_;

  string trace_path_;

  // Map used to convert between the new uuids assigned to the machine nodes and
  // the old uuids read from the machine topology file.
  unordered_map<string, string> uuid_conversion_map_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_BRIDGE_H
