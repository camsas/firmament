// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of a Quincy-style min-cost flow scheduler.

#include "scheduling/quincy_scheduler.h"

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "base/common.h"
#include "base/types.h"
#include "storage/reference_types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "misc/string_utils.h"
#include "engine/local_executor.h"
#include "engine/remote_executor.h"
#include "storage/object_store_interface.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

DEFINE_bool(debug_flow_graph, true, "Write out a debug copy of the scheduling"
            " flow graph to /tmp/debug.dm.");
DEFINE_int32(flow_scheduling_cost_model, 0,
             "Flow scheduler cost model to use. "
             "Values: 0 = TRIVIAL, 1 = QUINCY");

namespace firmament {
namespace scheduler {

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_on;
using executor::LocalExecutor;
using executor::RemoteExecutor;
using common::pb_to_set;
using store::ObjectStoreInterface;

QuincyScheduler::QuincyScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri,
    const SchedulingParameters& params)
    : EventDrivenScheduler(job_map, resource_map, object_store, task_map,
                           topo_mgr, m_adapter, coordinator_res_id,
                           coordinator_uri),
      flow_graph_(FlowSchedulingCostModelType(FLAGS_flow_scheduling_cost_model)),
      parameters_(params),
      debug_seq_num_(0) {
  LOG(INFO) << "QuincyScheduler initiated; parameters: "
            << parameters_.ShortDebugString();
  // Generate the initial flow graph
  ResourceTopologyNodeDescriptor* root = new ResourceTopologyNodeDescriptor;
  topo_mgr->AsProtobuf(root);
  flow_graph_.AddResourceTopology(root, topo_mgr->NumProcessingUnits());
}

QuincyScheduler::~QuincyScheduler() {
  // XXX(ionel): stub
}

const ResourceID_t* QuincyScheduler::FindResourceForTask(
    TaskDescriptor*) {
  // XXX(ionel): stub
  return NULL;
}

uint64_t QuincyScheduler::ApplySchedulingDeltas(
    const vector<SchedulingDelta*>& deltas) {
  // Perform the necessary actions to apply the scheduling changes passed to the
  // method
  VLOG(1) << "Applying " << deltas.size() << " scheduling deltas...";
  for (vector<SchedulingDelta*>::const_iterator it = deltas.begin();
       it != deltas.end();
       ++it) {
    VLOG(1) << "Processing delta of type " << (*it)->type();
    TaskID_t task_id = (*it)->task_id();
    ResourceID_t res_id = ResourceIDFromString((*it)->resource_id());
    if ((*it)->type() == SchedulingDelta::PLACE) {
      VLOG(1) << "Trying to place task " << task_id
              << " on resource " << (*it)->resource_id();
      TaskDescriptor** td = FindOrNull(*task_map_, task_id);
      ResourceStatus** rs = FindOrNull(*resource_map_, res_id);
      CHECK_NOTNULL(td);
      CHECK_NOTNULL(rs);
      VLOG(1) << "About to bind task " << (*td)->uid() << " to resource "
              << (*rs)->mutable_descriptor()->uuid();
      BindTaskToResource(*td, (*rs)->mutable_descriptor());
      // After the task is bound, we now remove all of its edges into the flow
      // graph apart from the bound resource.
      // N.B.: This disables preemption and migration!
      flow_graph_.UpdateArcsForBoundTask(task_id, res_id);
    }
    delete *it;  // Remove the scheduling delta -- N.B. modifies collection
                 // within loop!
  }
  return deltas.size();
}

void QuincyScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                           TaskFinalReport* report) {
  {
    boost::lock_guard<boost::mutex> lock(scheduling_lock_);
    // Find the task's node
    FlowGraphNode* task_node = flow_graph_.NodeForTaskID(td_ptr->uid());
    CHECK_NOTNULL(task_node);
    // Remove the task's node from the flow graph
    flow_graph_.DeleteTaskNode(task_node);
  }
  // Call into superclass handler
  EventDrivenScheduler::HandleTaskCompletion(td_ptr, report);
}

void QuincyScheduler::NodeBindingToSchedulingDelta(
    const FlowGraphNode& src, const FlowGraphNode& dst,
    SchedulingDelta* delta) {
  // Figure out what type of scheduling change this is
  // Source must be a task node as this point
  CHECK(src.type_.type() == FlowNodeType::SCHEDULED_TASK ||
        src.type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
        src.type_.type() == FlowNodeType::ROOT_TASK);
  // Destination must be a PU node
  CHECK(dst.type_.type() == FlowNodeType::PU);
  // Is the source (task) already placed elsewhere?
  ResourceID_t* bound_res = FindOrNull(task_bindings_, src.task_id_);
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

uint64_t QuincyScheduler::ScheduleJob(JobDescriptor* job_desc) {
  boost::lock_guard<boost::mutex> lock(scheduling_lock_);
  LOG(INFO) << "START SCHEDULING " << job_desc->uuid();
  // Check if we have any runnable tasks in this job
  const set<TaskID_t> runnable_tasks = RunnableTasksForJob(job_desc);
  if (runnable_tasks.size() > 0) {
    // Check if the job is already in the flow graph
    // If not, simply add the whole job
    flow_graph_.AddJobNodes(job_desc);
    // If it is, only add the new bits
    // Run a scheduler iteration
    uint64_t newly_scheduled = RunSchedulingIteration();
    LOG(INFO) << "STOP SCHEDULING " << job_desc->uuid();
    return newly_scheduled;
  } else {
    LOG(INFO) << "STOP SCHEDULING " << job_desc->uuid();
    return 0;
  }
}

// Returns a vector containing a nodes arcs with flow > 0.
// In the returned graph the arcs are the inverse of the arcs in the file.
// If there is (i,j) with flow 1 then in the graph we will have (j,i).
vector<map< uint64_t, uint64_t> >* QuincyScheduler::ReadFlowGraph(
    int fd, uint64_t num_vertices) {
  vector<map< uint64_t, uint64_t > >* adj_list =
      new vector<map<uint64_t, uint64_t> >(num_vertices + 1);
  // The cost is not returned.
  uint64_t cost;
  char line[100];
  vector<string> vals;

  FILE* fptr = NULL;
  FILE* dbg_fptr = NULL;
  if ((fptr = fdopen(fd, "r")) == NULL) {
    LOG(ERROR) << "Failed to open FD for reading of flow graph. FD " << fd;
  }
  if (FLAGS_debug_flow_graph) {
    // Somewhat ugly hack to generate unique output file name.
    string out_file_name;
    spf(&out_file_name, "/tmp/firmament-debug/debug-flow_%ju.dm",
        debug_seq_num_);
    CHECK((dbg_fptr = fopen(out_file_name.c_str(), "w")) != NULL);
    debug_seq_num_++;
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
          LOG(ERROR) << "Unexpected structured of flow row";
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
              LOG(ERROR) << "Unknown type of row in flow graph.";
            }
          }
        }
      }
    }
  }
  fclose(fptr);
  if (FLAGS_debug_flow_graph)
    fclose(dbg_fptr);
  return adj_list;
}

uint64_t QuincyScheduler::RunSchedulingIteration() {
  // Blow away any old exporter state
  exporter_.Reset();
  // Export the current flow graph to DIMACS format
  exporter_.Export(flow_graph_);
  uint64_t num_nodes = flow_graph_.NumNodes();
  // Pipe setup
  // infd[0] == PARENT_READ
  // infd[1] == CHILD_WRITE
  // outfd[0] == CHILD_READ
  // outfd[1] == PARENT_WRITE
  int outfd[2];
  int infd[2];
  // Write debugging copy
  if (FLAGS_debug_flow_graph) {
    // TODO(malte): somewhat ugly hack to compose a unique file name for each
    // scheduler iteration
    string out_file_name;
    spf(&out_file_name, "/tmp/firmament-debug/debug_%ju.dm", debug_seq_num_);
    exporter_.Flush(out_file_name);
  }
  // Now run the solver
  vector<string> args;
  pid_t solver_pid = ExecCommandSync("ext/cs2-4.6/cs2.exe", args, outfd, infd);
  VLOG(2) << "Solver running (PID: " << solver_pid << "), CHILD_READ: "
          << outfd[0] << ", PARENT_WRITE: " << outfd[1] << ", PARENT_READ: "
          << infd[0] << ", CHILD_WRITE: " << infd[1];
  // Write to pipe to solver
  exporter_.Flush(outfd[1]);
  // Parse and process the result
  vector<map<uint64_t, uint64_t> >* extracted_flow =
      ReadFlowGraph(infd[0], num_nodes);
  // We're done with the solver and can let it terminate here.
  WaitForFinish(solver_pid);
  // close the pipes
  close(outfd[1]);
  close(infd[0]);
  // Solver's dead, let's post-process the results.
  map<uint64_t, uint64_t>* task_mappings =
      GetMappings(extracted_flow, flow_graph_.leaf_node_ids(),
                  flow_graph_.sink_node().id_);
  map<uint64_t, uint64_t>::iterator it;
  vector<SchedulingDelta*> deltas;
  for (it = task_mappings->begin();
       it != task_mappings->end(); it++) {
    VLOG(1) << "Bind " << it->first << " to " << it->second << endl;
    SchedulingDelta* delta = new SchedulingDelta;
    NodeBindingToSchedulingDelta(*flow_graph_.Node(it->first),
                                 *flow_graph_.Node(it->second), delta);
    if (delta->type() == SchedulingDelta::NOOP)
      continue;
    // Mark the task as scheduled
    flow_graph_.Node(it->first)->type_.set_type(FlowNodeType::SCHEDULED_TASK);
    // Remember the delta
    deltas.push_back(delta);
  }
  uint64_t num_scheduled = ApplySchedulingDeltas(deltas);
  return num_scheduled;
}

void QuincyScheduler::PrintGraph(vector< map<uint64_t, uint64_t> > adj_map) {
  for (vector< map<uint64_t, uint64_t> >::size_type i = 1;
       i < adj_map.size(); ++i) {
    map<uint64_t, uint64_t>::iterator it;
    for (it = adj_map[i].begin();
         it != adj_map[i].end(); it++) {
      cout << i << " " << it->first << " " << it->second << endl;
    }
  }
}

bool QuincyScheduler::CheckNodeType(uint64_t node, FlowNodeType_NodeType type) {
  FlowNodeType_NodeType node_type = flow_graph_.Node(node)->type_.type();
  return (node_type == type);
}

// Assigns a leaf node to a worker|root task. At each step it checks if there is
// an arc to a worker|root task, if not then it goes one layer up in the graph.
// NOTE: The extracted_flow is changed by the method.
uint64_t QuincyScheduler::AssignNode(
    vector< map< uint64_t, uint64_t > >* extracted_flow,
    uint64_t node) {
  map<uint64_t, uint64_t>::iterator map_it;
  for (map_it = (*extracted_flow)[node].begin();
       map_it != (*extracted_flow)[node].end(); map_it++) {
    VLOG(2) << "Checking direct edge back to " << map_it->first << " (type: "
            << flow_graph_.Node(map_it->first)->type_.type() << ")";
    // Check if node = root or node = task
    if (CheckNodeType(map_it->first, FlowNodeType::ROOT_TASK) ||
        CheckNodeType(map_it->first, FlowNodeType::UNSCHEDULED_TASK)) {
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
  LOG(WARNING) << "Failed to find a task mapping for node " << node
               << ", which has flow!";
  return 0;
}

// Maps worker|root tasks to leaves. It expects a extracted_flow containing only
// the arcs with positive flow (i.e. what ReadFlowGraph returns).
map<uint64_t, uint64_t>* QuincyScheduler::GetMappings(
    vector< map< uint64_t, uint64_t > >* extracted_flow,
    unordered_set<uint64_t> leaves,
    uint64_t sink) {
  map<uint64_t, uint64_t>* task_node = new map<uint64_t, uint64_t>();
  unordered_set<uint64_t>::iterator set_it;
  for (set_it = leaves.begin(); set_it != leaves.end(); set_it++) {
    uint64_t* flow = FindOrNull((*extracted_flow)[sink], *set_it);
    if (flow != NULL) {
      // Exists flow from node to sink.
      // This could technically be optimized and done in assign_node.
      // It's not done like that for now because we're only expecting one task
      // per leaf node.
      VLOG(1) << "Have flow from PU node " << *set_it << " to sink: "
              << *flow;
      CHECK_EQ(*flow, 1);
      for (uint64_t flow_used = 1;  flow_used <= *flow; ++flow_used) {
        uint64_t task = AssignNode(extracted_flow, *set_it);
        // Arc back to a task, so we have a scheduling assignment
        if (task != 0) {
          VLOG(1) << "Assigning task node " << task << " to PU node "
                  << *set_it;
          (*task_node)[task] = *set_it;
        } else {
          // PU left unassigned.
        }
      }
    }
  }
  return task_node;
}

}  // namespace scheduler
}  // namespace firmament
