// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/solver_dispatcher.h"

#include <sys/stat.h>
#include <pthread.h>
#include <utility>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>

#include "base/common.h"
#include "base/units.h"
#include "misc/string_utils.h"
#include "misc/utils.h"

DEFINE_bool(debug_flow_graph, false, "Write out a debug copy of the scheduling"
            " flow graph to the debug directory.");
DEFINE_string(flow_scheduling_solver, "cs2",
              "Solver to use for flow network optimization. Possible values:"
              "\"cs2\": Goldberg solver, \"flowlessly\": local Flowlessly "
              "solver reimplementation; \"custom\": specify custom solver. "
              "with -flow_scheduling_binary and -flow_scheduling_args.");
DEFINE_string(flow_scheduling_binary, "", "Path to flow solving executable. "
              "If specified, overrides default path. "
              "Must be specified when using custom solver.");
DEFINE_string(custom_flow_scheduling_args, "", "Arguments for custom solver. "
              "Defaults to no arguments.");
DEFINE_bool(incremental_flow, false, "Generate incremental graph changes.");
DEFINE_bool(only_read_assignment_changes, false, "Read only changes in task"
            " assignments.");
DEFINE_string(flowlessly_binary, "ext/flowlessly-git/build/flow_scheduler",
              "Path to the flowlessly binary.");
DEFINE_string(flowlessly_algorithm, "fast_cost_scaling",
              "Algorithm to be used by flowlessly. Options: cycle_cancelling |"
              "cost_scaling | fast_cost_scaling | relax");
DEFINE_string(cs2_binary, "ext/cs2-4.6/cs2.exe", "Path to the cs2 binary.");
DEFINE_bool(log_solver_stderr, false, "Set to true to log solver's stderr.");

namespace firmament {
namespace scheduler {

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_on;

SolverDispatcher::SolverDispatcher(
    shared_ptr<FlowGraphManager> flow_graph_manager,
    bool solver_ran_once)
  : flow_graph_manager_(flow_graph_manager),
    solver_ran_once_(solver_ran_once),
    debug_seq_num_(0), to_solver_(NULL), from_solver_(NULL),
    from_solver_stderr_(NULL) {
  // Set up debug directory if it doesn't exist
  struct stat st;
  if (!FLAGS_debug_output_dir.empty() &&
      stat(FLAGS_debug_output_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_debug_output_dir.c_str(), 0700);
  } else if (!FLAGS_debug_output_dir.empty()) {
    // Delete any existing debug output in the directory
    string cmd;
    spf(&cmd, "rm %s/*", FLAGS_debug_output_dir.c_str());
    system(cmd.c_str());
  }
}

SolverDispatcher::~SolverDispatcher() {
  if (to_solver_ != NULL) {
    // Print EOS to Make sure the solver closes gracefully when running
    // in daemon mode.
    fprintf(to_solver_, "c EOS\n");
    fflush(to_solver_);
    CHECK_EQ(fclose(to_solver_), 0);
  }
  if (from_solver_ != NULL) {
    CHECK_EQ(fclose(from_solver_), 0);
  }
  if (from_solver_stderr_ != NULL) {
    CHECK_EQ(fclose(from_solver_stderr_), 0);
  }
}

void SolverDispatcher::ExportJSON(string* output) const {
  return json_exporter_.Export(*flow_graph_manager_->flow_graph(), output);
}

void *ExportToSolver(void *x) {
  SolverDispatcher *solver_dispatcher = reinterpret_cast<SolverDispatcher*>(x);
  // Write to pipe to solver
  solver_dispatcher->dimacs_exporter_.Flush(solver_dispatcher->to_solver_);
  if (!FLAGS_incremental_flow) {
    // We need to close the stream because that's what cs expects.
    CHECK_EQ(fclose(solver_dispatcher->to_solver_), 0);
    solver_dispatcher->to_solver_ = NULL;
  }
  return NULL;
}

void SolverDispatcher::NodeBindingToSchedulingDelta(
    const TaskDescriptor& task, const ResourceDescriptor& res,
    unordered_map<TaskID_t, ResourceID_t>* task_bindings,
    vector<SchedulingDelta*>* deltas) {
  // Is the source (task) already placed elsewhere?
  ResourceID_t* bound_res = FindOrNull(*task_bindings, task.uid());
  // Does the destination (resource) already have a task bound?
  TaskID_t current_bound_task_id = res.current_running_task();
  VLOG(2) << "Task ID: " << task.uid() << ", bound_res: " << bound_res
          << ", current bound task ID: " << current_bound_task_id;
  if (bound_res && (*bound_res != ResourceIDFromString(res.uuid()))) {
    // If so, we have a migration
    VLOG(1) << "MIGRATION: take " << task.uid() << " off "
            << *bound_res << " and move it to "
            << res.uuid();
    SchedulingDelta* delta = new SchedulingDelta;
    delta->set_type(SchedulingDelta::MIGRATE);
    delta->set_task_id(task.uid());
    delta->set_resource_id(res.uuid());
    deltas->push_back(delta);
  } else if (bound_res && (*bound_res == ResourceIDFromString(res.uuid()))) {
    // We were already scheduled here. No-op, so insert no delta.
  } else if (!bound_res
             && current_bound_task_id // resource is not idle
             && current_bound_task_id != task.uid()) {
    // XXX: wrong, need to check if unscheduled?
    // Is something else bound to the same resource?
    // If so, we need to kick it off (a preemption)
    VLOG(1) << "PREEMPTION: take " << current_bound_task_id << " off "
            << res.uuid() << " and replace it with " << task.uid();
    SchedulingDelta* preempt_delta = new SchedulingDelta;
    preempt_delta->set_type(SchedulingDelta::PREEMPT);
    preempt_delta->set_task_id(current_bound_task_id);
    preempt_delta->set_resource_id(res.uuid());
    deltas->push_back(preempt_delta);
    SchedulingDelta* place_delta = new SchedulingDelta;
    place_delta->set_type(SchedulingDelta::PLACE);
    place_delta->set_task_id(task.uid());
    place_delta->set_resource_id(res.uuid());
    deltas->push_back(place_delta);
  } else {
    // If neither, we have a scheduling event
    VLOG(1) << "SCHEDULING: place " << task.uid() << " on "
            << res.uuid() << ", which was idle.";
    SchedulingDelta* delta = new SchedulingDelta;
    delta->set_type(SchedulingDelta::PLACE);
    delta->set_task_id(task.uid());
    delta->set_resource_id(res.uuid());
    deltas->push_back(delta);
  }
}

void *ProcessStderrJustlog(void *x) {
  char line[1024];
  FILE *stderr = reinterpret_cast<FILE*>(x);
  while (fgets(line, sizeof(line), stderr) != NULL) {
    LOG(WARNING) << "STDERR from solver: " << line;
  }
  return NULL;
}

multimap<uint64_t, uint64_t>* SolverDispatcher::Run(
    SchedulerStats* scheduler_stats) {
  // Adjusts the costs on the arcs from tasks to unsched aggs.
  if (solver_ran_once_) {
    flow_graph_manager_->UpdateUnscheduledAggArcCosts();
  }

  if (solver_ran_once_ && FLAGS_incremental_flow) {
    dimacs_exporter_.Reset();
    dimacs_exporter_.ExportIncremental(flow_graph_manager_->graph_changes());
    flow_graph_manager_->ResetChanges();
  }

  if (!solver_ran_once_ || !FLAGS_incremental_flow) {
    // Always export full flow graph when running first time. If algorithm
    // is non-incremental, must do it for subsequent iterations too.
    dimacs_exporter_.Reset();
    dimacs_exporter_.Export(*flow_graph_manager_->flow_graph());
    flow_graph_manager_->ResetChanges();
  }

  // Note dimacs_exporter_ is the full graph iff solver is running for the first
  // time, or is non-incremental. Otherwise, dimacs_exporter_ is the incremental
  // delta.

  // Write debugging copy, of whatever we send to flow solver
  if (FLAGS_debug_flow_graph) {
    // TODO(malte): somewhat ugly hack to compose a unique file name for each
    // scheduler iteration
    string out_file_name;
    spf(&out_file_name, "%s/debug_%ju.dm", FLAGS_debug_output_dir.c_str(),
        debug_seq_num_);
    LOG(INFO) << "Writing flow graph debug info into " << out_file_name;
    dimacs_exporter_.FlushAndClose(out_file_name);
  }

  // Now run the solver
  vector<string> args;
  pid_t solver_pid;
  pthread_t logger_thread = -1;
  // If the solver hasn't executed or if we're not running in incremental mode.
  if (!solver_ran_once_ || !FLAGS_incremental_flow) {
    // Pipe setup
    // errfd[0] == PARENT_READ
    // errfd[1] == CHILD_WRITE
    // outfd[0] == PARENT_READ
    // outfd[1] == CHILD_WRITE
    // infd[0] == CHILD_READ
    // infd[1] == PARENT_WRITE
    string binary;
    SolverConfiguration(FLAGS_flow_scheduling_solver, &binary, &args);
    solver_pid = ExecCommandSync(binary, args, infd_, outfd_, errfd_);
    VLOG(2) << "Solver running " << "(PID: " << solver_pid << ")"
            << ", CHILD_READ: " << infd_[0]
            << ", CHILD_WRITE_STD: " << outfd_[1]
            << ", CHILD_WRITE_ERR: " << errfd_[1]
            << ", PARENT_WRITE: " << infd_[1]
            << ", PARENT_READ_STD: " << outfd_[0]
            << ", PARENT_READ_ERR: " << errfd_[0];

    solver_ran_once_ = true;
    if ((from_solver_stderr_ = fdopen(errfd_[0], "r")) == NULL) {
      LOG(ERROR) << "Failed to open FD for reading solver's output. FD "
                 << errfd_[0];
    }
    if ((from_solver_ = fdopen(outfd_[0], "r")) == NULL) {
      LOG(ERROR) << "Failed to open FD for reading solver's output. FD "
                 << outfd_[0];
    }
    if ((to_solver_ = fdopen(infd_[1], "w")) == NULL) {
      LOG(ERROR) << "Failed to open FD to solver for writing. FD: "
                 << infd_[1];
    }

    if (!FLAGS_log_solver_stderr) {
      if (pthread_create(&logger_thread, NULL,
                         ProcessStderrJustlog, from_solver_stderr_)) {
        PLOG(FATAL) << "Error creating thread";
      }
    }
  }

  boost::timer::cpu_timer flowsolver_timer;

  // We must export graph and read from STDOUT/STDERR in parallel
  // Otherwise, the solver might block if STDOUT/STDERR buffer gets full.
  // (For example, if it outputs lots of warnings on STDERR.)

  // Create thread to write the DIMACS
  pthread_t exporter_thread;
  if (pthread_create(&exporter_thread, NULL, ExportToSolver, this)) {
    PLOG(FATAL) << "Error creating thread";
  }

  uint64_t algorithm_runtime = numeric_limits<uint64_t>::max();
  multimap<uint64_t, uint64_t>* task_mappings =
    ReadOutput(&algorithm_runtime);

  // Wait for exporter to complete. (Should already have happened when we
  // get here, given we've finished reading the output.)
  if (pthread_join(exporter_thread, NULL)) {
    PLOG(FATAL) << "Error joining thread";
  }

  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime = flowsolver_timer.elapsed().wall /
      NANOSECONDS_IN_MICROSECOND;
    scheduler_stats->algorithm_runtime = algorithm_runtime;
  }

  if (!FLAGS_incremental_flow) {
    // We're done with the solver and can let it terminate here.
    int status = WaitForFinish(solver_pid);

    CHECK_EQ(fclose(from_solver_), 0);
    from_solver_ = NULL;
    CHECK_EQ(fclose(from_solver_stderr_), 0);
    from_solver_stderr_ = NULL;
    // N.B.: we DON'T close to_solver_ here, as the export thread already does
    // this (cs2 expects stdin to be closed before it terminates, so we can't do
    // it here)

    if (!FLAGS_log_solver_stderr) {
      // wait for logger thread
      if (pthread_join(logger_thread, NULL)) {
        PLOG(FATAL) << "Error joining thread";
      }
    }

    if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
      LOG(FATAL) << "Solver terminated abnormally";
    }
  }
  debug_seq_num_++;
  return task_mappings;
}

void SolverDispatcher::SolverConfiguration(const string& solver,
                                           string* binary,
                                           vector<string> *args) {
  // New solvers need to have their binary registered here.
  // Paths are relative to the Firmament root directory.
  if (solver == "cs2") {
    *binary = FLAGS_cs2_binary;
  } else if (solver == "flowlessly") {
    *binary = FLAGS_flowlessly_binary;
  } else if (solver == "custom") {
    // no-op; set binary below
  } else {
    LOG(FATAL) << "Non-existed flow network solver specified: " << solver;
  }

  if (!FLAGS_flow_scheduling_binary.empty()) {
    *binary = FLAGS_flow_scheduling_binary;
  } else {
    if (solver == "custom") {
      LOG(FATAL) << "Must specify -flow_scheduling_binary "
                 << "in conjunction with custom solver.";
    }
  }

  if (solver == "custom") {
    boost::split(*args, FLAGS_custom_flow_scheduling_args,
                 boost::is_any_of(" "));
  } else {
    if (!FLAGS_custom_flow_scheduling_args.empty()) {
      LOG(FATAL) << "Error: cannot specify custom arguments with solver "
                 << solver;
    }

    if (solver == "flowlessly") {
      args->push_back("--graph_has_node_types=true");
      args->push_back("--algorithm=" + FLAGS_flowlessly_algorithm);
      if (FLAGS_only_read_assignment_changes) {
        args->push_back("--print_assignments=true");
      } else {
        args->push_back("--print_assignments=false");
      }
      if (!FLAGS_incremental_flow) {
        args->push_back("--daemon=false");
      }
    } else if (solver == "cs2") {
      // Nothing to do
    } else {
      CHECK(false) << "Unknown flow solver chosen!";
    }
  }
}

// Assigns a leaf node to a worker|root task. At each step it checks if there
// is an arc to a worker|root task, if not then it goes one layer up in the
// graph.
// NOTE: The extracted_flow is changed by the method.
uint64_t SolverDispatcher::AssignNode(
    vector< map< uint64_t, uint64_t > >* extracted_flow, uint64_t node) {
  map<uint64_t, uint64_t>::iterator map_it;
  for (map_it = (*extracted_flow)[node].begin();
       map_it != (*extracted_flow)[node].end(); map_it++) {
    VLOG(2) << "Checking direct edge back to " << map_it->first << " (type: "
            << flow_graph_manager_->flow_graph()->Node(map_it->first)->type_
            << ")";
    // Check if node = root or node = task
    if (flow_graph_manager_->CheckNodeType(map_it->first,
                                           FlowNodeType::ROOT_TASK) ||
        flow_graph_manager_->CheckNodeType(map_it->first,
                                           FlowNodeType::UNSCHEDULED_TASK) ||
        flow_graph_manager_->CheckNodeType(map_it->first,
                                           FlowNodeType::SCHEDULED_TASK)) {
      // Shouldn't really modify the collection in the iterator loop.
      // However, we don't use the iterator after modification.
      uint64_t flow = map_it->second;
      uint64_t ret_node = map_it->first;
      if (flow == 1) {
        (*extracted_flow)[node].erase(map_it);
      } else {
        InsertOrUpdate(&((*extracted_flow)[node]), ret_node, flow - 1);
      }
      return ret_node;
    }
  }
  // If here it means we didn't find any arc with flow to worker or root
  for (map_it = (*extracted_flow)[node].begin();
       map_it != (*extracted_flow)[node].end(); map_it++) {
    VLOG(2) << "Checking indirect edge from " << map_it->second << " back to "
            << map_it->first;
    uint64_t flow = map_it->second;
    uint64_t ret_node = map_it->first;
    if (flow == 1) {
      (*extracted_flow)[node].erase(map_it);
    } else {
      InsertOrUpdate(&(*extracted_flow)[node], ret_node, flow - 1);
    }
    return AssignNode(extracted_flow, ret_node);
  }
  // If here it means that the leaf node will not be assigned.
  // Should not happen because it initially had flow.
  VLOG(2) << "Failed to find a task mapping for node " << node
          << ", which has flow!";
  return 0;
}

// Maps worker|root tasks to leaves. It expects a extracted_flow containing
// only the arcs with positive flow (i.e. what ReadFlowGraph returns).
multimap<uint64_t, uint64_t>* SolverDispatcher::GetMappings(
    vector< map< uint64_t, uint64_t > >* extracted_flow,
    unordered_set<uint64_t> leaves, uint64_t sink) {
  multimap<uint64_t, uint64_t>* task_node =
    new multimap<uint64_t, uint64_t>();
  unordered_set<uint64_t>::iterator set_it;
  for (set_it = leaves.begin(); set_it != leaves.end(); set_it++) {
    uint64_t* flow = FindOrNull((*extracted_flow)[sink], *set_it);
    if (flow != NULL) {
      // Exists flow from node to sink.
      // This could technically be optimized and done in assign_node.
      // It's not done like that for now because we're only expecting one task
      // per leaf node.
      VLOG(2) << "Have flow from PU node " << *set_it << " to sink: "
              << *flow;
      CHECK_EQ(*flow, 1);
      for (uint64_t flow_used = 1;  flow_used <= *flow; ++flow_used) {
        uint64_t task = AssignNode(extracted_flow, *set_it);
        // Arc back to a task, so we have a scheduling assignment
        if (task != 0) {
          VLOG(1) << "Assigning task node " << task << " to PU node "
                  << *set_it;
          task_node->insert(pair<uint64_t, uint64_t>(task, *set_it));
        } else {
          // PU left unassigned.
        }
      }
    }
  }
  return task_node;
}

// Returns a vector containing a nodes arcs with flow > 0.
// In the returned graph the arcs are the inverse of the arcs in the file.
// If there is (i,j) with flow 1 then in the graph we will have (j,i).
multimap<uint64_t, uint64_t>* SolverDispatcher::ReadOutput(
    uint64_t* algorithm_runtime) {
  multimap<uint64_t, uint64_t>* task_mappings;

  // If we read from stdout and stderr, then we must process both
  // in parallel. Otherwise, the buffer on one could get full, and the solver
  // would block. This could result in a situation of deadlock.

  // Process stdout in main thread
  if (FLAGS_only_read_assignment_changes) {
    task_mappings = ReadTaskMappingChanges(from_solver_, algorithm_runtime);
  } else {
    // Parse and process the result
    uint64_t num_nodes = flow_graph_manager_->flow_graph()->NumNodes();
    vector<map<uint64_t, uint64_t> >* extracted_flow =
      ReadFlowGraph(from_solver_, algorithm_runtime, num_nodes);
    task_mappings = GetMappings(extracted_flow,
                                flow_graph_manager_->leaf_node_ids(),
                                flow_graph_manager_->sink_node()->id_);
    delete extracted_flow;
  }
  return task_mappings;
}

vector<map< uint64_t, uint64_t> >* SolverDispatcher::ReadFlowGraph(
    FILE* fptr, uint64_t* algorithm_runtime, uint64_t num_vertices) {
  vector<map< uint64_t, uint64_t > >* adj_list =
    new vector<map<uint64_t, uint64_t> >(num_vertices + 1);
  // The cost is not returned.
  uint64_t cost;
  char line[100];
  vector<string> vals;
  FILE* dbg_fptr = NULL;
  if (FLAGS_debug_flow_graph) {
    // Somewhat ugly hack to generate unique output file name.
    string out_file_name;
    spf(&out_file_name, "%s/debug-flow_%ju.dm",
        FLAGS_debug_output_dir.c_str(), debug_seq_num_);
    CHECK((dbg_fptr = fopen(out_file_name.c_str(), "w")) != NULL);
  }
  while (fgets(line, sizeof(line), fptr) != NULL) {
    if (FLAGS_debug_flow_graph) {
      fputs(line, dbg_fptr);
      fputc('\n', dbg_fptr);
    }
    if (line[0] == 'f') {
      uint64_t src;
      uint64_t dst;
      uint64_t flow;
      CHECK_EQ(sscanf(line, "%*c %ju %ju %ju", &src, &dst, &flow), 3);
      // Only add it to the adjacency list if flow > 0
      if (flow > 0) {
        (*adj_list)[dst].insert(make_pair(src, flow));
      }
    } else if (line[0] == 'c') {
      if (!strcmp(line, "c EOI\n")) {
        break;
      } else if (!strncmp(line, "c ALGORITHM TIME", 16)) {
        sscanf(line, "%*c %*s %*s %ju", algorithm_runtime);
      }
    } else if (line[0] == 's') {
      sscanf(line, "%*c %ju", &cost);
    } else {
      LOG(ERROR) << "Unexpected line in flow graph: " << line;
    }
  }
  if (FLAGS_debug_flow_graph)
    CHECK_EQ(fclose(dbg_fptr), 0);
  return adj_list;
}

multimap<uint64_t, uint64_t>* SolverDispatcher::ReadTaskMappingChanges(
    FILE* fptr, uint64_t* algorithm_runtime) {
  multimap<uint64_t, uint64_t>* task_node =
    new multimap<uint64_t, uint64_t>();
  char line[100];
  bool end_of_iteration = false;
  while (!end_of_iteration) {
    if (fgets(line, 100, fptr) != NULL) {
      if (line[0] == 'm') {
        uint64_t task_id;
        uint64_t core_id;
        CHECK_EQ(sscanf(line, "%*c %ju %ju", &task_id, &core_id), 2);
        VLOG(1) << "Assigning task node " << task_id << " to PU node "
                << core_id;
        task_node->insert(pair<uint64_t, uint64_t>(task_id, core_id));
      } else if (line[0] == 'c') {
        if (!strcmp(line, "c EOI\n")) {
          end_of_iteration = true;
        } else if (!strncmp(line, "c ALGORITHM TIME", 16)) {
          sscanf(line, "%*c %*s %*s %ju", algorithm_runtime);
        }
      } else {
        LOG(ERROR) << "Unknown type of row in flow graph.";
      }
    }
  }
  return task_node;
}
} // namespace scheduler
} // namespace firmament
