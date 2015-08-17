// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/utils.h"
#include "scheduling/flow/cost_models.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "sim/knowledge_base_simulator.h"
#include "sim/trace-extract/event_desc.pb.h"
#include "sim/trace-extract/google_trace_utils.h"

DECLARE_string(flow_scheduling_solver);
DECLARE_string(flow_scheduling_binary);
DECLARE_bool(incremental_flow);
DECLARE_bool(only_read_assignment_changes);
DECLARE_bool(debug_flow_graph);
DECLARE_bool(add_root_task_to_graph);
DECLARE_bool(flow_scheduling_strict);
DECLARE_bool(flow_scheduling_time_reported);
DECLARE_bool(graph_output_events);

namespace firmament {
namespace sim {

class GoogleTraceSimulator {
 public:
  explicit GoogleTraceSimulator(const string& trace_path);
  virtual ~GoogleTraceSimulator();
  void Run();
  static void SolverTimeoutHandler(int sig);

 private:
  /**
   * Add new machine to the topology. The method updates simulator's mapping
   * state.
   * @param machine_tmp the topology descriptor of the new machine
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
  TaskDescriptor* AddNewTask(const TaskIdentifier& task_identifier,
       unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* runtimes);

  /**
   * Compute and add task completion event to the simulator's events.
   * @param cur_timestamp the timestap when the task starts running
   * @param task_id the Firmament task id
   * @param task_identifier the simulator task identifier
   * @param task_runtime the mapping between tasks and runtimes
   */
  void AddTaskEndEvent(
      uint64_t cur_timestamp, const TaskID_t& task_id,
      TaskIdentifier task_identifier,
      unordered_map<TaskIdentifier, uint64_t,
                    TaskIdentifierHasher>* task_runtime);

  void AddTaskStats(
      TaskIdentifier task_identifier,
      unordered_map<TaskIdentifier, uint64_t,
        TaskIdentifierHasher>* task_runtime);

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

  /**
   * Populate and add the root node of the topology.
   */
  void CreateRootResource();

  bool HasSimulationCompleted(uint64_t task_time, uint64_t num_events,
                              uint64_t num_scheduling_rounds);

  void InitializeCostModel();

  /**
   * Notifies the flow graph of job completion and updates the simulator's
   * state.
   * @param simulator_job_id the simulator's job id
   * @param job_id flow graph's job id
   */
  void JobCompleted(uint64_t simulator_job_id, JobID_t job_id);

  void LoadTraceData(ResourceTopologyNodeDescriptor* machine_tmpl);

  uint64_t NextRunSolverAt(uint64_t cur_run_solver_at, double algorithm_time);

  /**
   * Time of the next simulator event. UINT64_MAX if no more simulator events.
   */
  uint64_t NextSimulatorEvent();

  /**
   * Create and populate a new job.
   * @param job_id the Google trace job id
   * @return a pointer to the job descriptor
   */
  JobDescriptor* PopulateJob(uint64_t job_id);

  /**
   * Processes all the simulator events that happen at a given time.
   * @param cur_time the timestamp for which to process the simulator events
   * @param machine_tmpl topology to use in case new machines are added
   */
  void ProcessSimulatorEvents(
      uint64_t cur_time, const ResourceTopologyNodeDescriptor& machine_tmpl);

  /**
   * Process the given task event.
   * @param cut_time the timestamp of the event
   * @param task_identifier the Google trace identifier of the task
   * @param event_type the type of the event
   */
  void ProcessTaskEvent(
      uint64_t cur_time,
      const TaskIdentifier& task_identifier, uint64_t event_type,
      unordered_map<TaskIdentifier, uint64_t,
        TaskIdentifierHasher>* task_runtime);

  /**
   * Removes a machine from the topology and all its associated state.
   * NOTE: The method currently assumes that the machine is directly
   * connected to the topology root.
   * @param machine_id the Google id of the machine to be removed
   */
  void RemoveMachine(uint64_t machine_id);

  /**
   * Removes the simulator state for a resource and evicts all the tasks
   * running on it or any of its children nodes.
   * @param rtnd the resource to be removed
   */
  void RemoveResource(ResourceTopologyNodeDescriptor* rtnd);

  /**
   * Removes a resource from its parents children list.
   * @param rtnd the resource node to remove
   */
  void RemoveResourceNodeFromParentChildrenList(
      const ResourceTopologyNodeDescriptor& rtnd);

  void RemoveTaskStats(TaskID_t task_id);

  void ReplayTrace();

  /**
   * The resource topology is build from the same protobuf file. The function
   * changes the uuids to make sure that there's no two identical uuids.
   */
  void ResetUuidAndAddResource(ResourceTopologyNodeDescriptor* rtnd,
                               const string& hostname, const string& root_uuid);

  /**
   * Reset the fields used to maintain statistics about the current scheduling
   * latency. This method should be called after every run of the scheduler.
   */
  void ResetSchedulingLatencyStats();

  /**
   * Runs the solver.
   * @param the time when the solver should run
   * @return the time when the solver should run after the run at run_solver_at
   */
  uint64_t RunSolver(uint64_t run_solver_at);

  /**
   * Notifies the flow_graph of a task completion and updates the simulator's
   * state.
   * @param task_identifier the simulator identifier of the completed task
   */
  void TaskCompleted(const TaskIdentifier& task_identifier);

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

  void UpdateFlowGraph(
    uint64_t scheduling_timestamp,
    unordered_map<TaskIdentifier, uint64_t,
      TaskIdentifierHasher>* task_runtime,
    multimap<uint64_t, uint64_t>* task_mappings);
  void UpdateResourceStats();

  /**
   * Update the fields used to maintain statistics about the current scheduling
   * latency. This method should be called whenever a trace event happens.
   */
  void UpdateSchedulingLatencyStats(uint64_t time);

  // Map used to convert between the new uuids assigned to the machine nodes and
  // the old uuids read from the machine topology file.
  unordered_map<string, string> uuid_conversion_map_;

  // Map used to convert between the google trace job_ids and the Firmament
  // job descriptors.
  unordered_map<uint64_t, JobDescriptor*> job_id_to_jd_;
  // Map used to convert between the google trace task_ids and the Firmament
  // task descriptors.
  unordered_map<TaskIdentifier, TaskDescriptor*, TaskIdentifierHasher>
      task_id_to_td_;
  // Map used to convert between the google trace machine_ids and the
  // Firmament resource descriptors.
  unordered_map<uint64_t, ResourceDescriptor*> machine_id_to_rd_;

  // Map from Firmament TaskID_t to Google trace task identifier.
  unordered_map<TaskID_t, TaskIdentifier> task_id_to_identifier_;

  // Map holding the number of tasks for each job.
  unordered_map<uint64_t, uint64_t> job_num_tasks_;

  // Map from the Google machine id to the Firmament rtnd.
  unordered_map<uint64_t, ResourceTopologyNodeDescriptor*> machine_id_to_rtnd_;

  // Map from JobID_t to JobDescriptor
  shared_ptr<JobMap_t> job_map_;
  // Map from TaskID_t to TaskDescriptor*
  shared_ptr<TaskMap_t> task_map_;
  // Map from ResourceID_t to ResourceStatus*
  shared_ptr<ResourceMap_t> resource_map_;

  // Map holding the per-task runtime information
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>* task_runtime_;

  // Map holding the ResourceID_t of every scheduled task.
  unordered_map<TaskID_t, ResourceID_t> task_bindings_;

  // Map holding the end runtime for every running task.
  unordered_map<TaskID_t, uint64_t> task_id_to_end_time_;

  // Map holding the task_id of the task running on the resource with res_id.
  unordered_map<ResourceID_t, TaskID_t,
      boost::hash<boost::uuids::uuid> > res_id_to_task_id_;

  unordered_map<TaskIdentifier, TaskStats, TaskIdentifierHasher>
      task_id_to_stats_;

  // The map storing the simulator events. Maps from timestamp to simulator
  // event.
  multimap<uint64_t, EventDescriptor> events_;

  // Timestamp of the first event seen in the current scheduling round. Any
  // event present in the original trace is record, as are those which we have
  // created to replace events in the trace, e.g. when rerunning task runtime.
  // If no event has been seen then the value of the variable is UINT64_MAX.
  uint64_t first_event_in_scheduling_round_;
  uint64_t last_event_in_scheduling_round_;
  uint64_t num_events_in_scheduling_round_;
  uint64_t sum_timestamps_in_scheduling_round_;

  string trace_path_;

  KnowledgeBaseSimulator* knowledge_base_;

  // The root node of the machine topology.
  ResourceTopologyNodeDescriptor rtn_root_;

  shared_ptr<FlowGraph> flow_graph_;

  CostModelInterface* cost_model_;

  scheduler::SolverDispatcher* solver_dispatcher_;

  // Ignore all the events that have an id greater than max_event_id_to_retain_.
  uint64_t max_event_id_to_retain_;

  // File to output graph to. (Optional; NULL if unspecified.)
  FILE *graph_output_;
  // File to output stats to. (Optional; NULL if unspecified.)
  FILE *stats_file_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H
