// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/quincy_dispatcher.h"

#include <utility>

#include <sys/stat.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>

#include "misc/string_utils.h"
#include "misc/utils.h"

DEFINE_bool(debug_flow_graph, false, "Write out a debug copy of the scheduling"
            " flow graph to /tmp/debug.dm.");
DEFINE_string(debug_output_dir, "/tmp/firmament-debug",
              "The directory to write debug output to.");
DEFINE_string(flow_scheduling_binary, "", "Path to flow solving executable.");
DEFINE_string(flow_scheduling_args, "", "Optional: arguments for executable.");
DEFINE_bool(flow_scheduling_strict, false, "Terminate if flow solving binary fails.");
DEFINE_bool(incremental_flow, false, "Generate incremental graph changes.");
DEFINE_bool(only_read_assignment_changes, false, "Read only changes in task"
            " assignments.");

namespace firmament {
namespace scheduler {

  using boost::lexical_cast;
  using boost::algorithm::is_any_of;
  using boost::token_compress_on;

  multimap<uint64_t, uint64_t>* QuincyDispatcher::Run(
  		                        double *algorithm_time, double *flowsolver_time,
					                    FILE *graph_output) {
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

    // Note exporter_ is the full graph iff solver is running for the first time,
    // or is non-incremental. Otherwise, exporter_ is the incremental delta.

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
    if (!solver_ran_once_ || !FLAGS_incremental_flow) {
      // Pipe setup
    	// errfd[0] == PARENT_READ
    	// errfd[1] == CHILD_WRITE
    	// outfd[0] == PARENT_READ
    	// outfd[1] == CHILD_WRITE
    	// infd[0] == CHILD_READ
    	// infd[1] == PARENT_WRITE
     	boost::split(args, FLAGS_flow_scheduling_args, boost::is_any_of(" "));

      solver_pid = ExecCommandSync(FLAGS_flow_scheduling_binary, args, infd_, outfd_, errfd_);
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

      setlinebuf(from_solver_);
      setlinebuf(from_solver_stderr_);
    }

    boost::timer::cpu_timer flowsolver_timer;

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

    ProcessStderr(from_solver_stderr_, algorithm_time);

    if (flowsolver_time != NULL) {
    	boost::timer::nanosecond_type one_second = 1e9;
    	*flowsolver_time = flowsolver_timer.elapsed().wall;
    	*flowsolver_time /= one_second;
    	// restart timer
    	flowsolver_timer.stop(); flowsolver_timer.resume();
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

  void QuincyDispatcher::NodeBindingToSchedulingDelta(
      const FlowGraphNode& src, const FlowGraphNode& dst,
      map<TaskID_t, ResourceID_t>* task_bindings, SchedulingDelta* delta) {
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

  void QuincyDispatcher::ProcessStderr(FILE *stats, double *algorithm_time) {
  	char line[100];

  	if (algorithm_time) {
  		*algorithm_time = nan("");
  	}

  	while (fgets(line, sizeof(line), stats) != NULL) {
			double time;
			int num_matched = sscanf(line, "ALGOTIME: %lf\n", &time);
			if (num_matched == 1) {
				if (algorithm_time) {
					*algorithm_time = time;
				}
				return;
			} else {
				LOG(WARNING) << "STDERR from algorithm: " << line;
			}
  	}
  }

} // namespace scheduler
} // namespace firmament
