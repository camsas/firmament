// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/solver_dispatcher.h"

#include <sys/stat.h>
#include <pthread.h>
#include <utility>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>

#include "misc/string_utils.h"
#include "misc/utils.h"

DEFINE_bool(debug_flow_graph, false, "Write out a debug copy of the scheduling"
            " flow graph to /tmp/debug.dm.");
DEFINE_string(debug_output_dir, "/tmp/firmament-debug",
              "The directory to write debug output to.");
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
DEFINE_bool(flow_scheduling_time_reported, false,
            "Does solver report runtime to stderr?");
DEFINE_bool(flow_scheduling_strict, false, "Terminate if flow solving binary"
            " fails.");
DEFINE_bool(incremental_flow, false, "Generate incremental graph changes.");
DEFINE_bool(only_read_assignment_changes, false, "Read only changes in task"
            " assignments.");
DEFINE_string(flowlessly_binary, "ext/flowlessly-git/run_fast_cost_scaling",
              "Path to the flowlessly binary.");
DEFINE_string(cs2_binary, "ext/cs2-4.6/cs2.exe", "Path to the cs2 binary.");

namespace firmament {
namespace scheduler {

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_on;

SolverDispatcher::SolverDispatcher(shared_ptr<FlowGraph> flow_graph,
                                   bool solver_ran_once)
  : flow_graph_(flow_graph),
    solver_ran_once_(solver_ran_once),
    debug_seq_num_(0) {
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

void *ExportToSolver(void *x) {
  SolverDispatcher *qd = reinterpret_cast<SolverDispatcher*>(x);
  // Write to pipe to solver
  qd->exporter_.Flush(qd->to_solver_);
  if (!FLAGS_incremental_flow) {
    // We need to close the stream because that's what cs expects.
    fclose(qd->to_solver_);
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
    double *algorithm_time, double *flowsolver_time, FILE *graph_output) {
  if (algorithm_time && !FLAGS_flow_scheduling_time_reported) {
    LOG(ERROR) << "Error: cannot record algorithm time with solver "
               << "which does not report this time.";
    *algorithm_time = nan("");
  }

  // Adjusts the costs on the arcs from tasks to unsched aggs.
  if (solver_ran_once_) {
    flow_graph_->AdjustUnscheduledAggArcCosts();
  }

  if (solver_ran_once_ &&
      (graph_output != NULL || FLAGS_incremental_flow)) {
    // Only generate incremental delta if not first time running
    // If we're running an incremental algorithm, have to generate deltas.
    // But if we're logging incremental changes, generate deltas even when
    // algorithm is non-incremental.

    exporter_.Reset();
    exporter_.ExportIncremental(flow_graph_->graph_changes());
    flow_graph_->ResetChanges();

    if (graph_output != NULL) {
      exporter_.Flush(graph_output);
    }
  }

  if (!solver_ran_once_ || !FLAGS_incremental_flow) {
    // Always export full flow graph when first time running. If algorithm
    // is non-incremental, must do it for subsequent iterations too.

    exporter_.Reset();
    exporter_.Export(*flow_graph_);
    flow_graph_->ResetChanges();

    if (graph_output != NULL && !solver_ran_once_) {
      // only log the initial graph once, even when running non-incrementally
      exporter_.Flush(graph_output);
    }
  }

  // Note exporter_ is the full graph iff solver is running for the first
  // time, or is non-incremental. Otherwise, exporter_ is the incremental delta.

  // Write debugging copy, of whatever we send to flow solver
  if (FLAGS_debug_flow_graph) {
    // TODO(malte): somewhat ugly hack to compose a unique file name for each
    // scheduler iteration
    string out_file_name;
    spf(&out_file_name, "%s/debug_%ju.dm", FLAGS_debug_output_dir.c_str(),
        debug_seq_num_);
    LOG(INFO) << "Writing flow graph debug info into " << out_file_name;
    exporter_.Flush(out_file_name);
  }

  // Now run the solver
  vector<string> args;
  pid_t solver_pid;
  pthread_t logger_thread = -1;
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

    if (!FLAGS_flow_scheduling_time_reported) {
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

  multimap<uint64_t, uint64_t>* task_mappings;
  task_mappings = ReadOutput(algorithm_time);

  // Wait for exporter to complete. (Should already have happened when we
  // get here, given we've finished reading the output.)
  if (pthread_join(exporter_thread, NULL)) {
    PLOG(FATAL) << "Error joining thread";
  }

  if (flowsolver_time != NULL) {
    boost::timer::nanosecond_type one_second = 1e9;
    *flowsolver_time = flowsolver_timer.elapsed().wall;
    *flowsolver_time /= one_second;
    // restart timer
    flowsolver_timer.stop(); flowsolver_timer.start();
  }

  if (!FLAGS_incremental_flow) {
    // We're done with the solver and can let it terminate here.
    int status = WaitForFinish(solver_pid);

    fclose(from_solver_);
    fclose(from_solver_stderr_);
    // close the pipes
    close(errfd_[0]);
    close(outfd_[0]);
    close(infd_[1]);

    if (!FLAGS_flow_scheduling_time_reported) {
      // wait for logger thread
      if (pthread_join(logger_thread, NULL)) {
        PLOG(FATAL) << "Error joining thread";
      }
    }

    if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
      std::string msg = "Solver terminated abnormally.";
      if (FLAGS_flow_scheduling_strict) {
        LOG(FATAL) << msg;
      } else {
        LOG(ERROR) << msg;
      }
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
      args->push_back("--global_update=false");
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
            << flow_graph_->Node(map_it->first)->type_ << ")";
    // Check if node = root or node = task
    if (flow_graph_->CheckNodeType(map_it->first, FlowNodeType::ROOT_TASK) ||
        flow_graph_->CheckNodeType(map_it->first,
                                   FlowNodeType::UNSCHEDULED_TASK) ||
        flow_graph_->CheckNodeType(map_it->first,
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

struct arguments {
  FILE *fptr;
  double *time;
};

void *ProcessStderrAlgoTime(void *x) {
  struct arguments *args = (struct arguments *)x;
  double *algorithm_time = args->time;
  FILE *stderr = args->fptr;
  char line[1024];

  *algorithm_time = nan("");

  while (fgets(line, sizeof(line), stderr) != NULL) {
    double time;
    int num_matched = sscanf(line, "ALGOTIME: %lf\n", &time);
    if (num_matched == 1) {
      *algorithm_time = time;
      break;
    } else {
      LOG(WARNING) << "STDERR from algorithm: " << line;
    }
  }

  return NULL;
}

// Returns a vector containing a nodes arcs with flow > 0.
// In the returned graph the arcs are the inverse of the arcs in the file.
// If there is (i,j) with flow 1 then in the graph we will have (j,i).
multimap<uint64_t, uint64_t>* SolverDispatcher::ReadOutput(double *time) {
  multimap<uint64_t, uint64_t>* task_mappings;

  // Reading from two streams: stdout and stderr. Must process both
  // in parallel. Otherwise, the buffer on one could get full, and the solver
  // would block. This could result in a situation of deadlock.

  pthread_t stderr_thread = -1;
  if (FLAGS_flow_scheduling_time_reported) {
    // Create thread to process stderr
    struct arguments args = { from_solver_stderr_, time };
    if (pthread_create(&stderr_thread, NULL, ProcessStderrAlgoTime, &args)) {
      PLOG(FATAL) << "Error creating thread";
    }
  }

  // Process stdout in main thread
  if (FLAGS_only_read_assignment_changes) {
    task_mappings = ReadTaskMappingChanges(from_solver_);
  } else {
    // Parse and process the result
    uint64_t num_nodes = flow_graph_->NumNodes();
    vector<map<uint64_t, uint64_t> >* extracted_flow =
      ReadFlowGraph(from_solver_, num_nodes);
    task_mappings = GetMappings(extracted_flow, flow_graph_->leaf_node_ids(),
                                flow_graph_->sink_node()->id_);
    delete extracted_flow;
  }

  if (FLAGS_flow_scheduling_time_reported) {
    // Wait for stderr processing to complete
    if (pthread_join(stderr_thread, NULL)) {
      PLOG(FATAL) << "Error joining thread";
    }
  }

  return task_mappings;
}

vector<map< uint64_t, uint64_t> >* SolverDispatcher::ReadFlowGraph(
     FILE* fptr, uint64_t num_vertices) {
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
  uint64_t l = 0;
  while (fgets(line, sizeof(line), fptr)) {
    size_t len = strlen(line);
    if (len > 0 && line[len-1] == '\n') {
      line[--len] = '\0';
    }
    if (len == 0) {
      // empty line; skip
      continue;
    }

    VLOG(3) << "Processing line " << l << ": " << line;
    if (FLAGS_debug_flow_graph) {
      fputs(line, dbg_fptr);
      fputc('\n', dbg_fptr);
    }
    l++;
    boost::split(vals, line, is_any_of(" "), token_compress_on);
    if (vals[0].compare("f") == 0) {
      if (vals.size() != 4) {
        LOG(ERROR) << "Unexpected structure of flow row";
      } else {
        uint64_t src = lexical_cast<uint64_t>(vals[1]);
        uint64_t dest = lexical_cast<uint64_t>(vals[2]);
        uint64_t flow = lexical_cast<uint64_t>(vals[3]);
        // Only add it to the adjacency list if flow > 0
        if (flow > 0) {
          (*adj_list)[dest].insert(make_pair(src, flow));
        }
      }
    } else {
      if (vals[0].compare("c") == 0) {
        if (vals.size() == 2 && vals[1].compare("EOI") == 0) {
          // end of iteration
          break;
        } else {
          // Comment line. Ignore.
        }
      } else {
        if (vals[0].compare("s") == 0) {
          cost = lexical_cast<uint64_t>(vals[1]);
        } else {
          if (vals[0].compare("") == 0) {
            LOG(INFO) << "Empty row in flow graph.";
          } else {
            LOG(ERROR) << "Unknown type of row in flow graph: " << line;
          }
        }
      }
    }
  }
  if (FLAGS_debug_flow_graph)
    fclose(dbg_fptr);
  return adj_list;
}

multimap<uint64_t, uint64_t>* SolverDispatcher::ReadTaskMappingChanges(
    FILE* fptr) {
  multimap<uint64_t, uint64_t>* task_node =
    new multimap<uint64_t, uint64_t>();
  char line[100];
  vector<string> vals;
  bool end_of_iteration = false;
  while (!end_of_iteration) {
    if (fgets(line, 100, fptr) != NULL) {
      if (line[0] == 'm') {
        uint64_t task_id;
        uint64_t core_id;
        sscanf(line, "%*c %jd %jd", &task_id, &core_id);
        VLOG(1) << "Assigning task node " << task_id << " to PU node "
                << core_id;
        task_node->insert(pair<uint64_t, uint64_t>(task_id, core_id));
      } else if (line[0] == 'c') {
        if (line[2] == 'E' && line[3] == 'O' && line[4] == 'I' &&
            line[5] == '\n') {
          end_of_iteration = true;
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
