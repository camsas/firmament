// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/quincy_dispatcher.h"

#include <utility>

#include <sys/stat.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "misc/string_utils.h"
#include "misc/utils.h"

DEFINE_bool(debug_flow_graph, false, "Write out a debug copy of the scheduling"
            " flow graph to /tmp/debug.dm.");
DEFINE_string(debug_output_dir, "/tmp/firmament-debug",
              "The directory to write debug output to.");
DEFINE_string(flow_scheduling_solver, "cs2",
              "Solver to use for flow network optimization. Possible values:"
              "\"cs2\": Goldberg solver, \"flowlessly\": local Flowlessly"
              "solver reimplementation.");
DEFINE_bool(incremental_flow, false, "Generate incremental graph changes.");
DEFINE_bool(only_read_assignment_changes, false, "Read only changes in task"
            " assignments.");
DEFINE_string(flowlessly_binary, "ext/flowlessly-git/run_fast_cost_scaling",
              "Path to the flowlessly binary.");
DEFINE_string(cs2_binary, "ext/cs2-git/cs2.exe", "Path to the cs2 binary.");

namespace firmament {
namespace scheduler {

  using boost::lexical_cast;
  using boost::algorithm::is_any_of;
  using boost::token_compress_on;

  multimap<uint64_t, uint64_t>* QuincyDispatcher::Run() {
    // Set up debug directory if it doesn't exist
    struct stat st;
    if (!FLAGS_debug_output_dir.empty() &&
        stat(FLAGS_debug_output_dir.c_str(), &st) == -1) {
      mkdir(FLAGS_debug_output_dir.c_str(), 0700);
    }
    // Adjusts the costs on the arcs from tasks to unsched aggs.
    if (solver_ran_once_) {
      flow_graph_->AdjustUnscheduledAggArcCosts();
    }
    // Blow away any old exporter state
    exporter_.Reset();
    if (FLAGS_incremental_flow && solver_ran_once_) {
      exporter_.ExportIncremental(flow_graph_->graph_changes());
      flow_graph_->ResetChanges();
    } else {
      // Export the current flow graph to DIMACS format
      exporter_.Export(*flow_graph_);
      flow_graph_->ResetChanges();
    }
    // Write debugging copy
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
    string solver_binary;
    // Get the binary name of the solver
    SolverBinaryName(FLAGS_flow_scheduling_solver, &solver_binary);
    pid_t solver_pid;

    if (!solver_ran_once_ || !FLAGS_incremental_flow) {
      // Pipe setup
      // infd_[0] == PARENT_READ
      // infd_[1] == CHILD_WRITE
      // outfd_[0] == CHILD_READ
      // outfd_[1] == PARENT_WRITE
      if (FLAGS_flow_scheduling_solver.compare("flowlessly") == 0) {
        args.push_back("--graph_has_node_types=true");
        args.push_back("--global_update=false");
        if (!FLAGS_incremental_flow) {
          args.push_back("--daemon=false");
        }
      }
      solver_pid = ExecCommandSync(solver_binary, args, outfd_, infd_);
      VLOG(2) << "Solver (" << FLAGS_flow_scheduling_solver << "running "
              << "(PID: " << solver_pid << "), CHILD_READ: "
              << outfd_[0] << ", PARENT_WRITE: " << outfd_[1]
              << ", PARENT_READ: " << infd_[0] << ", CHILD_WRITE: " << infd_[1];
      solver_ran_once_ = true;
      if ((from_solver_ = fdopen(infd_[0], "r")) == NULL) {
        LOG(ERROR) << "Failed to open FD for reading solver's output. FD "
                   << infd_[0];
      }
      if ((to_solver_ = fdopen(outfd_[1], "w")) == NULL) {
        LOG(ERROR) << "Failed to open FD to solver for writing. FD: "
                   << outfd_[1];
      }
    }
    // Write to pipe to solver
    exporter_.Flush(to_solver_);
    if (!FLAGS_incremental_flow) {
      // We need to close the stream because that's what cs expects.
      fclose(to_solver_);
    }
    multimap<uint64_t, uint64_t>* task_mappings;
    if (FLAGS_only_read_assignment_changes) {
      task_mappings = ReadTaskMappingChanges(from_solver_);
    } else {
      // Parse and process the result
      uint64_t num_nodes = flow_graph_->NumNodes();
      vector<map<uint64_t, uint64_t> >* extracted_flow =
        ReadFlowGraph(from_solver_, num_nodes);
      task_mappings = GetMappings(extracted_flow, flow_graph_->leaf_node_ids(),
                                  flow_graph_->sink_node().id_);
      delete extracted_flow;
    }
    if (!FLAGS_incremental_flow) {
      // We're done with the solver and can let it terminate here.
      WaitForFinish(solver_pid);
      fclose(from_solver_);
      // close the pipes
      close(outfd_[1]);
      close(infd_[0]);
    }
    debug_seq_num_++;
    return task_mappings;
  }

  void QuincyDispatcher::NodeBindingToSchedulingDelta(
      const FlowGraphNode& src, const FlowGraphNode& dst,
      unordered_map<TaskID_t, ResourceID_t>* task_bindings,
      SchedulingDelta* delta) {
    // Figure out what type of scheduling change this is
    // Source must be a task node as this point
    CHECK(src.type_.type() == FlowNodeType::SCHEDULED_TASK ||
          src.type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
          src.type_.type() == FlowNodeType::ROOT_TASK);
    // Destination must be a PU node
    CHECK(dst.type_.type() == FlowNodeType::PU);
    // Is the source (task) already placed elsewhere?
    ResourceID_t* bound_res = FindOrNull(*task_bindings, src.task_id_);
    VLOG(2) << "task ID: " << src.task_id_ << ", bound_res: " << bound_res;
    if (bound_res && (*bound_res != dst.resource_id_)) {
      // If so, we have a migration
      VLOG(1) << "MIGRATION: take " << src.task_id_ << " off "
              << *bound_res << " and move it to "
              << dst.resource_id_;
      delta->set_type(SchedulingDelta::MIGRATE);
      delta->set_task_id(src.task_id_);
      delta->set_resource_id(to_string(dst.resource_id_));
    } else if (bound_res && (*bound_res == dst.resource_id_)) {
      // We were already scheduled here. No-op.
      delta->set_type(SchedulingDelta::NOOP);
    } else if (!bound_res && false) {  // Is something else bound to the same
      // resource?
      // If so, we have a preemption
      // XXX(malte): This code is NOT WORKING!
      VLOG(1) << "PREEMPTION: take " << src.task_id_ << " off "
              << *bound_res << " and replace it with "
              << src.task_id_;
      delta->set_type(SchedulingDelta::PREEMPT);
    } else {
      // If neither, we have a scheduling event
      VLOG(1) << "SCHEDULING: place " << src.task_id_ << " on "
              << dst.resource_id_ << ", which was idle.";
      delta->set_type(SchedulingDelta::PLACE);
      delta->set_task_id(src.task_id_);
      delta->set_resource_id(to_string(dst.resource_id_));
    }
  }

  // Assigns a leaf node to a worker|root task. At each step it checks if there
  // is an arc to a worker|root task, if not then it goes one layer up in the
  // graph.
  // NOTE: The extracted_flow is changed by the method.
  uint64_t QuincyDispatcher::AssignNode(
      vector< map< uint64_t, uint64_t > >* extracted_flow, uint64_t node) {
    map<uint64_t, uint64_t>::iterator map_it;
    for (map_it = (*extracted_flow)[node].begin();
         map_it != (*extracted_flow)[node].end(); map_it++) {
      VLOG(2) << "Checking direct edge back to " << map_it->first << " (type: "
              << flow_graph_->Node(map_it->first)->type_.type() << ")";
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
  multimap<uint64_t, uint64_t>* QuincyDispatcher::GetMappings(
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
  vector<map< uint64_t, uint64_t> >* QuincyDispatcher::ReadFlowGraph(
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
    while (!feof(fptr)) {
      if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
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
            // Comment line. Ignore.
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
    }
    if (FLAGS_debug_flow_graph)
      fclose(dbg_fptr);
    return adj_list;
  }

  multimap<uint64_t, uint64_t>* QuincyDispatcher::ReadTaskMappingChanges(
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
          sscanf(line, "%*c %" SCNd64 " %" SCNd64, &task_id, &core_id);
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

  void QuincyDispatcher::SolverBinaryName(const string& solver,
                                          string* binary) {
    // New solvers need to have their binary registered here.
    // Paths are relative to the Firmament root directory.
    if (solver == "cs2") {
      *binary = FLAGS_cs2_binary;
    } else if (solver == "flowlessly") {
      *binary = FLAGS_flowlessly_binary;
    } else {
      LOG(FATAL) << "Non-existed flow network solver specified: " << solver;
    }
  }

} // namespace scheduler
} // namespace firmament
