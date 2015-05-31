// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H

#include <fstream>
#include <map>
#include <string>
#include <boost/timer/timer.hpp>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/dimacs_change_stats.h"
#include "scheduling/flow_graph.h"
#include "scheduling/knowledge_base_simulator.h"
#include "scheduling/quincy_dispatcher.h"
#include "scheduling/cost_models/cost_models.h"
#include "sim/trace-extract/event_desc.pb.h"

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

struct TaskIdentifier {
  uint64_t job_id;
  uint64_t task_index;

  bool operator==(const TaskIdentifier& other) const {
    return job_id == other.job_id && task_index == other.task_index;
  }
};

struct TaskIdentifierHasher {
  size_t operator()(const TaskIdentifier& key) const {
    return hash<uint64_t>()(key.job_id) * 17 + hash<uint64_t>()(key.task_index);
  }
};

struct TaskStats {
  double avg_mean_cpu_usage;
  double avg_canonical_mem_usage;
  double avg_assigned_mem_usage;
  double avg_unmapped_page_cache;
  double avg_total_page_cache;
  double avg_mean_disk_io_time;
  double avg_mean_local_disk_used;
  double avg_cpi;
  double avg_mai;
};

class GoogleTraceSimulator {
 public:
  explicit GoogleTraceSimulator(const string& trace_path);
  virtual ~GoogleTraceSimulator();
  void Run();

 private:
  /**
   * Add new machine to the topology. The method updates simulator's mapping state.
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

  void AddTaskEndEvent(
      uint64_t cur_timestamp, const TaskID_t& task_id,
      TaskIdentifier task_identifier,
      unordered_map<TaskIdentifier, uint64_t,
                    TaskIdentifierHasher>* task_runtime);

  void AddTaskStats(
      TaskIdentifier task_identifier,
      unordered_map<TaskIdentifier, uint64_t,
        TaskIdentifierHasher>* task_runtime);

  void RemoveTaskStats(TaskID_t task_id);

  /**
   * Creates a new task for a job.
   * @param jd_ptr the job descriptor of the job for which to create a new task
   * @return a pointer to the task descriptor of the new task
   */
  TaskDescriptor* AddTaskToJob(JobDescriptor* jd_ptr);

  /**
   * Compute the number of events of a particular type withing each time interval.
   */
  void BinTasksByEventType(uint64_t event_type, ofstream& out_file); // NOLINT

  /**
   * Populate and add the root node of the topology.
   */
  void CreateRootResource();

  void JobCompleted(uint64_t simulator_job_id, JobID_t job_id);

  /**
   * Loads all the machine events and returns a multimap timestamp -> event.
   */
  void LoadMachineEvents();

  void LoadMachineTemplate(ResourceTopologyNodeDescriptor* machine_tmpl);

  void LoadJobsNumTasks();

  void LoadTaskRuntimeStats();

  /**
   * Loads all the task runtimes and returns map task_identifier -> runtime.
   */
  void LoadTasksRunningTime();

  void LoadTraceData();

  inline void LogEvent(const string& msg) {
    VLOG(1) << msg;
    if (graph_output_ && FLAGS_graph_output_events) {
      fprintf(graph_output_, "c %s\n", msg.c_str());
    }
  }

  void LogStartOfSolverRun(uint64_t time_interval_bound);
  void LogSolverRunStats(const boost::timer::cpu_timer timer,
                         uint64_t time_interval_bound,
                         double algorithm_time,
                         double flowsolver_time,
                         const DIMACSChangeStats& change_stats,
                         ofstream* stats_file);

  uint64_t NextTimeIntervalBound(uint64_t cur_time_interval_bound,
                                 double algorithm_time);

  /**
   * Create and populate a new job.
   * @param job_id the Google trace job id
   * @return a pointer to the job descriptor
   */
  JobDescriptor* PopulateJob(uint64_t job_id);

  /**
   * Must be called every time an exogenous event is processed.
   * @param time the time of the exogenous event
   */
  void SeenExogenous(uint64_t time);
  /**
   * Time of the next simulator event. UINT64_MAX if no more simulator events.
   */
  uint64_t NextSimulatorEvent();

  void OutputStatsHeader(ofstream* stats_file);
  void OutputChangeStats(const DIMACSChangeStats& stats,
                         ofstream* stats_file);

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

  void RemoveMachine(uint64_t machine_id);

  void RemoveResource(ResourceTopologyNodeDescriptor* rtnd);

  /**
   * The resource topology is build from the same protobuf file. The function
   * changes the uuids to make sure that there's no two identical uuids.
   */
  void ResetUuidAndAddResource(ResourceTopologyNodeDescriptor* rtnd,
                               const string& hostname, const string& root_uuid);

  void ReplayTrace(ofstream *stats_file);

  void TaskCompleted(const TaskIdentifier& task_identifier);
  void TaskEvicted(TaskID_t task_id, const ResourceID_t& res_id);

  EventDescriptor_EventType TranslateMachineEvent(int32_t machine_event);

  void UpdateFlowGraph(
    uint64_t scheduling_timestamp,
    unordered_map<TaskIdentifier, uint64_t,
      TaskIdentifierHasher>* task_runtime,
    multimap<uint64_t, uint64_t>* task_mappings);
  void UpdateResourceStats();

  // XXX(ionel): These methods are copied from quincy_scheduler. We copy them
  // because we don't have access to the scheduler in the simulator.
  FlowGraphNode* GatherWhareMCStats(FlowGraphNode* accumulator,
                                    FlowGraphNode* other);
  void AccumulateWhareMapStats(WhareMapStats* accumulator,
                               WhareMapStats* other);
  void PrintResourceStats(uint64_t id, WhareMapStats* wms) {
    LOG(INFO) << "Node: " << id << " " << wms->num_devils() << " "
              << wms->num_rabbits() << " " << wms->num_sheep() << " "
              << wms->num_turtles();
  }

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

  // Timestamp of the first exogenous event seen this iteration. Any event
  // present in the original trace is exogeneous, as are those which we have
  // created to replace events in the trace, e.g. when rerunning task runtime.
  // Currently, the only endogeneous changes are due to graph updates when a
  // task is scheduled.
  // If no exogenous event has been, it is UINT64_MAX.
  uint64_t first_exogenous_event_seen_;

  string trace_path_;

  KnowledgeBaseSimulator* knowledge_base_;

  // The root node of the machine topology.
  ResourceTopologyNodeDescriptor rtn_root_;

  shared_ptr<FlowGraph> flow_graph_;

  FlowSchedulingCostModelInterface* cost_model_;

  scheduler::QuincyDispatcher* quincy_dispatcher_;

  // Proportion of events to retain, as a ratio out of UINT64_MAX
  uint64_t proportion_to_retain_;

  // File to output graph to. (Optional; NULL if unspecified.)
  FILE *graph_output_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H
