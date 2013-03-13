// The Firmament project
// Copyright (c) 2012-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of a Quincy scheduler.

#include "engine/quincy_scheduler.h"

#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>


#include "storage/reference_types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "engine/local_executor.h"
#include "engine/remote_executor.h"
#include "storage/object_store_interface.h"

using namespace std;
using namespace boost;

namespace firmament {
namespace scheduler {

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
    const string& coordinator_uri)
    : SchedulerInterface(job_map, resource_map, object_store, task_map),
      coordinator_uri_(coordinator_uri),
      coordinator_res_id_(coordinator_res_id),
      topology_manager_(topo_mgr),
      m_adapter_ptr_(m_adapter),
      scheduling_(false) {
  VLOG(1) << "QuincyScheduler initiated.";
}

QuincyScheduler::~QuincyScheduler() {
  // XXX(ionel): stub
}

void QuincyScheduler::BindTaskToResource(
    TaskDescriptor* task_desc,
    ResourceDescriptor* res_desc) {
  // XXX(ionel): stub
}

const ResourceID_t* QuincyScheduler::FindResourceForTask(
    TaskDescriptor* Task_Desc) {
  // XXX(ionel): stub
  return NULL;
}

void QuincyScheduler::DeregisterResource(ResourceID_t res_id) {
  // XXX(ionel): stub
}

void QuincyScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr) {
  // XXX(ionel): stub
}

bool QuincyScheduler::PlaceDelegatedTask(TaskDescriptor* td,
                                         ResourceID_t target_resource) {
  // XXX(ionel): stub
  return false;
}

void QuincyScheduler::RegisterResource(ResourceID_t res_id, bool local) {
  // XXX(ionel): stub
}

void QuincyScheduler::RegisterLocalResource(ResourceID_t res_id) {
  // XXX(ionel): stub
}

void QuincyScheduler::RegisterRemoteResource(ResourceID_t res_id) {
  // XXX(ionel): stub
}

const set<TaskID_t>& QuincyScheduler::RunnableTasksForJob(
    JobDescriptor* job_desc) {
  // XXX(ionel): stub
  return runnable_tasks_;
}

uint64_t QuincyScheduler::ScheduleJob(JobDescriptor* job_desc) {
  // XXX(ionel): stub
  LOG(ERROR) << "Quincy scheduler unimplemented!";
  return 0;
}

TaskDescriptor* QuincyScheduler::ProducingTaskForDataObjectID(
    DataObjectID_t id) {
  // XXX(ionel): stub
  return NULL;
}

// Returns a vector containing a nodes arcs with flow > 0.
// In the returned graph the arcs are the inverse of the arcs in the file.
// If there is (i,j) with flow 1 then in the graph we will have (j,i).
vector< map< uint64_t, uint64_t> > QuincyScheduler::ReadFlowGraph(
    char* file_name, uint64_t num_vertices) {
  vector< map< uint64_t, uint64_t > > adj_list(num_vertices + 1);
  // The cost is not returned.
  uint64_t cost;
  string line;
  vector<string> vals;

  ifstream graph (file_name);
  if (graph.is_open()) {
    while (graph.good()) {
      getline(graph, line);
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
            adj_list[dest].insert(make_pair(src, flow));
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
              LOG(ERROR) << "Uknown type of row in flow graph.";
            }
          }
        }
      }
    }
    graph.close();
  } else {
    LOG(ERROR) << "Could not open flow graph file: " << file_name;
  }
  return adj_list;
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

bool QuincyScheduler::CheckNodeType(
    const map<uint64_t, uint64_t>& nodes_type, uint64_t node, uint64_t type) {
  const uint64_t* node_type = FindOrNull(nodes_type, node);
  if (node_type != NULL) {
    return *node_type == type;
  }
  // Should not reach here.
  return false;
}

// Assigns a leaf node to a worker|root task. At each step it checks if there is
// an arc to a worker|root task, if not then it goes one layer up in the graph.
// NOTE: The flow_graph is changed by the method.
uint64_t QuincyScheduler::AssignNode(
    vector< map< uint64_t, uint64_t > > &flow_graph,
    const map<uint64_t, uint64_t>& nodes_type,
    uint64_t node) {
  map<uint64_t, uint64_t>::iterator map_it;
  for (map_it = flow_graph[node].begin();
       map_it != flow_graph[node].end(); map_it++) {
    // Check if node = root or node = worker
    if (CheckNodeType(nodes_type, map_it->first, FlowNodeType::ROOT_TASK) ||
        CheckNodeType(nodes_type, map_it->first, FlowNodeType::WORKER_TASK)) {
      // Shouldn't really modify the collection in the iterator loop.
      // However, we don't use the iterator after modification.
      uint64_t flow = map_it->second;
      uint64_t ret_node = map_it->first;
      if (flow == 1) {
        flow_graph[node].erase(map_it);
      } else {
        InsertOrUpdate(&flow_graph[node], ret_node, flow - 1);
      }
      return ret_node;
    }
  }
  // If here it means we didn't find any arc with flow to worker or root
  for (map_it = flow_graph[node].begin();
       map_it != flow_graph[node].end(); map_it++) {
    uint64_t flow = map_it->second;
    uint64_t ret_node = map_it->first;
    if (flow == 1) {
      flow_graph[node].erase(map_it);
    } else {
      InsertOrUpdate(&flow_graph[node], ret_node, flow - 1);
    }
    return AssignNode(flow_graph, nodes_type, ret_node);
  }
  // If here it means that the leaf node will not be assigned.
  // Should not happen because it initially had flow.
  return 0;
}

// Maps worker|root tasks to leaves. It expects a flow_graph containing only
// the arcs with positive flow (i.e. what ReadFlowGraph returns).
// nodes_type maps nodes to NodeType enum values.
map<uint64_t, uint64_t> QuincyScheduler::GetMappings(
    vector< map< uint64_t, uint64_t > >& flow_graph,
    const map<uint64_t, uint64_t>& nodes_type,
    set<uint64_t> leaves,
    uint64_t sink) {
  map<uint64_t, uint64_t> task_node;
  set<uint64_t>::iterator set_it;
  for (set_it = leaves.begin(); set_it != leaves.end(); set_it++) {
    uint64_t* flow = FindOrNull(flow_graph[sink], *set_it);
    if (flow != NULL) {
      // Exists flow from node to sink.
      // This could technically be optimized and done in assign_node.
      // It's not done like that for now because we're only expecting one task
      // per leaf node.
      for (uint64_t flow_used = 1;  flow_used <= *flow; ++flow_used) {
        uint64_t task = AssignNode(flow_graph, nodes_type, *set_it);
        if (task != 0) {
          task_node[task] = *set_it;
        } else {
          // Computer/PU left unassigned.
        }
      }
    }
  }
  return task_node;
}

}  // namespace scheduler
}  // namespace firmament
