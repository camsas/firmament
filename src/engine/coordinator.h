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

// Platform-independent coordinator class definition. This is subclassed by the
// platform-specific coordinator classes.

#ifndef FIRMAMENT_ENGINE_COORDINATOR_H
#define FIRMAMENT_ENGINE_COORDINATOR_H

#include <string>
#include <map>
#include <utility>
#include <vector>
#include <queue>

// XXX(malte): Think about the Boost dependency!
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/uuid/uuid.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build coordinator!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/reference_desc.pb.h"
#include "base/resource_desc.pb.h"
#include "base/resource_topology_node_desc.pb.h"
#include "engine/health_monitor.h"
#include "engine/node.h"
#include "messages/heartbeat_message.pb.h"
#include "messages/registration_message.pb.h"
#include "messages/task_delegation_message.pb.h"
#include "messages/task_heartbeat_message.pb.h"
#include "messages/task_info_message.pb.h"
#include "messages/task_spawn_message.pb.h"
#include "messages/task_state_message.pb.h"
#include "misc/messaging_interface.h"
#include "misc/trace_generator.h"
#include "misc/utils.h"
#include "misc/wall_time.h"
#include "platforms/common.h"
#include "platforms/unix/signal_handler.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/procfs_machine.h"
#ifdef __HTTP_UI__
#include "engine/coordinator_http_ui.h"
#endif
#include "scheduling/flow/flow_scheduler.h"
#include "scheduling/simple/simple_scheduler.h"
#include "storage/object_store_interface.h"
#include "engine/executors/topology_manager.h"

namespace firmament {

//using __gnu_cxx::hash_map;

using machine::topology::TopologyManager;
using platform_unix::SignalHandler;
using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsAdapter;
using platform_unix::ProcFSMachine;
using scheduler::FlowScheduler;
using scheduler::SchedulerInterface;
using scheduler::SimpleScheduler;
using store::ObjectStoreInterface;

#ifdef __HTTP_UI__
// Forward declaration
namespace webui {
class CoordinatorHTTPUI;
}  // namespace webui
using webui::CoordinatorHTTPUI;
#endif

class Coordinator : public Node,
                    public boost::enable_shared_from_this<Coordinator> {
 public:
  explicit Coordinator();
  virtual ~Coordinator();
  void Run();
  JobDescriptor* DescriptorForJob(const string& job_id);
  void Shutdown(const string& reason);
  const string SubmitJob(const JobDescriptor& job_descriptor);

  // Gets a pointer to the resource descriptor for an associated resource
  // (including the coordinator itself); returns NULL if the resource is not
  // associated or the coordinator itself.
  ResourceDescriptor* GetResource(ResourceID_t res_id) {
    if (res_id == uuid_)
      return &resource_desc_;
    ResourceStatus** res = FindOrNull(*associated_resources_, res_id);
    return (res ? (*res)->mutable_descriptor() : NULL);
  }

  // Hacky DFS to extract the RTND for a specific resource from the tree.
  // We ought to have a better solution for this, however -- either a
  // lookup table of some sort, or something else. The DFS won't scale to
  // large numbers of resources.
  // XXX(malte): fix this hacky implementation
  ResourceTopologyNodeDescriptor* GetResourceTreeNode(ResourceID_t res_id) {
    ResourceTopologyNodeDescriptor* root = local_resource_topology_;
    queue<ResourceTopologyNodeDescriptor*> q;
    q.push(root);
    while (!q.empty()) {
      ResourceTopologyNodeDescriptor* cur = q.front();
      if (ResourceIDFromString(cur->resource_desc().uuid()) == res_id) {
        return cur;
      }
      for (int32_t i = 0; i < cur->children_size(); ++i)
        q.push(cur->mutable_children(i));
      q.pop();
    }
    return NULL;
  }

  // Gets a pointer to the resource status for an associated resource.
  // Returns NULL if not associated.
  ResourceStatus* GetResourceStatus(ResourceID_t res_id) {
    ResourceStatus* res = FindPtrOrNull(*associated_resources_, res_id);
    return res;
  }

  // Gets the current location (endpoint) of an associated resource.
  // Returns NULL if not associated.
  const string GetLocationForResource(ResourceID_t res_id) {
    ResourceStatus* res = FindPtrOrNull(*associated_resources_, res_id);
    return res->location();
  }

  ResourceDescriptor* GetMachineRDForResource(ResourceID_t res_id) {
    ResourceID_t machine_res_id =
      MachineResIDForResource(associated_resources_, res_id);
    return GetResource(machine_res_id);
  }

  // Gets a pointer to the job descriptor for a job known to the coordinator.
  // If the job is not known to the coordinator, we will return NULL.
  JobDescriptor* GetJob(JobID_t job_id) {
    return FindOrNull(*job_table_, job_id);
  }

  // Gets a pointer to the task descriptor for a task known to the coordinator.
  // If the task is not known to the coordinator, we will return NULL.
  TaskDescriptor* GetTask(TaskID_t task_id) {
    TaskDescriptor* result = FindPtrOrNull(*task_table_, task_id);
    return result;
  }
  inline uint64_t NumResources() { return associated_resources_->size(); }
  inline uint64_t NumJobs() { return job_table_->size(); }
  inline uint64_t NumJobsInState(JobDescriptor::JobState state) {
    uint64_t count = 0;
    if (job_table_->empty())
      return 0;
    for (JobMap_t::const_iterator j_iter = job_table_->begin();
         j_iter != job_table_->end();
         ++j_iter)
      if (j_iter->second.state() == state)
        count++;
    return count;
  }
  inline uint64_t NumTasks() { return task_table_->size(); }
  inline uint64_t NumTasksInState(TaskDescriptor::TaskState state) {
    uint64_t count = 0;
    for (TaskMap_t::const_iterator t_iter = task_table_->begin();
         t_iter != task_table_->end();
         ++t_iter) {
      if (t_iter->second->state() == state)
        count++;
    }
    return count;
  }

  vector<ResourceStatus*> associated_resources() {
    vector<ResourceStatus*> res_vec;
    for (ResourceMap_t::const_iterator res_iter =
         associated_resources_->begin();
         res_iter != associated_resources_->end();
         ++res_iter) {
      res_vec.push_back(res_iter->second);
    }
    return res_vec;
  }
  vector<JobDescriptor> active_jobs() {
    vector<JobDescriptor> jd_vec;
    for (JobMap_t::const_iterator job_iter =
         job_table_->begin();
         job_iter != job_table_->end();
         ++job_iter) {
      jd_vec.push_back(job_iter->second);
    }
    return jd_vec;
  }
  vector<TaskDescriptor*> active_tasks() {
    vector<TaskDescriptor*> td_vec;
    for (TaskMap_t::const_iterator task_iter =
         task_table_->begin();
         task_iter != task_table_->end();
         ++task_iter) {
      td_vec.push_back(task_iter->second);
    }
    return td_vec;
  }
  inline const ResourceTopologyNodeDescriptor& local_resource_topology() {
    CHECK_NOTNULL(local_resource_topology_);
    return *local_resource_topology_;
  }
  shared_ptr<ObjectStoreInterface> get_object_store() {
    return object_store_;
  }
  const string& hostname() {
    return hostname_;
  }
  const string& parent_uri() {
    return parent_uri_;
  }
  const SchedulerInterface* scheduler() const {
    return scheduler_;
  }

  bool KillRunningJob(JobID_t job_id);
  bool KillRunningTask(TaskID_t task_id,
                       TaskKillMessage::TaskKillReason reason);

 protected:
  void AddJobsTasksToTables(TaskDescriptor* td, JobID_t job_id);
  void AddResource(ResourceTopologyNodeDescriptor* rtnd,
                   const string& endpoint_uri,
                   bool local);
  bool RegisterWithCoordinator(StreamSocketsChannel<BaseMessage>* chan);
  void DetectLocalResources();
  bool HasJobCompleted(const JobDescriptor& jd);
  void HandleIncomingMessage(BaseMessage *bm, const string& remote_endpoint);
  void HandleIncomingReceiveError(const boost::system::error_code& error,
                                  const string& remote_endpoint);
  void HandleHeartbeat(const HeartbeatMessage& msg);
  void HandleRegistrationRequest(const RegistrationMessage& msg);
  void HandleTaskCompletion(const TaskStateMessage& msg, TaskDescriptor* td);
  void HandleTaskDelegationRequest(const TaskDelegationRequestMessage& msg,
                                   const string& endpoint);
  void HandleTaskDelegationResponse(const TaskDelegationResponseMessage& msg,
                                    const string& endpoint);
  void HandleTaskHeartbeat(const TaskHeartbeatMessage& msg);
  void HandleTaskInfoRequest(const TaskInfoRequestMessage& msg,
                             const string& remote_endpoint);
  void HandleTaskSpawn(const TaskSpawnMessage& msg);
  void HandleTaskStateChange(const TaskStateMessage& msg);

#ifdef __HTTP_UI__
  void InitHTTPUI();
#endif
  void SendHeartbeatToParent(const ResourceStats& stats);

#ifdef __HTTP_UI__
  scoped_ptr<CoordinatorHTTPUI> c_http_ui_;
#endif
  // A map of resources associated with this coordinator.
  // The key is a resource UUID, the value a pair.
  // The first component of the pair is the resource descriptor, the second is
  // the timestamp when the latest heartbeat or message was received from this
  // resource..
  shared_ptr<ResourceMap_t> associated_resources_;
  // TODO(malte): Figure out the right representation here. Currently, we
  // maintain both associated_resources_ and the topology tree; one may suffice?
  ResourceTopologyNodeDescriptor* local_resource_topology_;
  // A map of all jobs known to this coordinator, indexed by their job ID.
  // Key is the job ID, value a ResourceDescriptor.
  // Currently, this table grows ad infinitum.
  shared_ptr<JobMap_t> job_table_;
  // A map of all tasks that the coordinator currently knows about.
  // TODO(malte): Think about GC'ing this.
  shared_ptr<TaskMap_t> task_table_;
  // The health monitor periodically checks on the liveness of subordinate
  // coordinators and running tasks.
  HealthMonitor health_monitor_;
  boost::thread* health_monitor_thread_;
  // The topology manager associated with this coordinator; responsible for the
  // local resources.
  shared_ptr<TopologyManager> topology_manager_;
  // The local object store.
  shared_ptr<ObjectStoreInterface> object_store_;
  // The local scheduler object. A coordinator may not have a scheduler, in
  // which case this will be a stub that defers to another scheduler.
  // TODO(malte): Work out the detailed semantics of this.
  SchedulerInterface* scheduler_;
  // Store URI of parent coordinator (if any)
  string parent_uri_;
  // Pointer to channel to the parent coordinator
  StreamSocketsChannel<BaseMessage>* parent_chan_;
  // Machine statistics monitor
  ProcFSMachine machine_monitor_;
  ResourceID_t machine_uuid_;
  // Local machine's host name
  const string hostname_;
  // Object that must be used to get the current time.
  WallTime* time_manager_;
  TraceGenerator* trace_generator_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
