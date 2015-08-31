// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive implementation of a simple-minded queue-based scheduler.

#include "scheduling/simple/simple_scheduler.h"

#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "misc/map-util.h"
#include "misc/utils.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace scheduler {

using store::ObjectStoreInterface;

SimpleScheduler::SimpleScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    ResourceTopologyNodeDescriptor* resource_topology,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    EventNotifierInterface* event_notifier,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri)
    : EventDrivenScheduler(job_map, resource_map, resource_topology,
                           object_store, task_map, topo_mgr, m_adapter,
                           event_notifier, coordinator_res_id,
                           coordinator_uri) {
  VLOG(1) << "SimpleScheduler initiated.";
}

SimpleScheduler::~SimpleScheduler() {
}

const ResourceID_t* SimpleScheduler::FindResourceForTask(
    TaskDescriptor* task_desc) {
  // TODO(malte): This is an extremely simple-minded approach to resource
  // selection (i.e. the essence of scheduling). We will simply traverse the
  // resource map in some order, and grab the first resource available.
  VLOG(2) << "Trying to place task " << task_desc->uid() << "...";
  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    ResourceID_t* rid = new ResourceID_t(res_iter->first);
    VLOG(3) << "Considering resource " << *rid << ", which is in state "
            << res_iter->second->descriptor().state();
    if (res_iter->second->descriptor().state() ==
        ResourceDescriptor::RESOURCE_IDLE)
      return rid;
  }
  // We have not found any idle resources in our local resource map. At this
  // point, we should start looking beyond the machine boundary and towards
  // remote resources.
  return NULL;
}

uint64_t SimpleScheduler::ScheduleAllJobs() {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  vector<JobDescriptor*> jobs;
  for (auto& job_id_jd : jobs_to_schedule_) {
    jobs.push_back(job_id_jd.second);
  }
  uint64_t num_scheduled_tasks = ScheduleJobs(jobs);
  ClearScheduledJobs();
  return num_scheduled_tasks;
}

uint64_t SimpleScheduler::ScheduleJob(JobDescriptor* jd_ptr) {
  uint64_t num_scheduled_tasks = 0;
  VLOG(2) << "Preparing to schedule job " << jd_ptr->uuid();
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  LOG(INFO) << "START SCHEDULING " << jd_ptr->uuid();
  // Get the set of runnable tasks for this job
  set<TaskID_t> runnable_tasks = RunnableTasksForJob(jd_ptr);
  VLOG(2) << "Scheduling job " << jd_ptr->uuid() << ", which has "
          << runnable_tasks.size() << " runnable tasks.";
  for (set<TaskID_t>::const_iterator task_iter =
       runnable_tasks.begin();
       task_iter != runnable_tasks.end();
       ++task_iter) {
    TaskDescriptor** td = FindOrNull(*task_map_, *task_iter);
    CHECK(td);
    VLOG(2) << "Considering task " << (*td)->uid() << ":\n"
            << (*td)->DebugString();
    // TODO(malte): check passing semantics here.
    const ResourceID_t* best_resource = FindResourceForTask(*td);
    if (!best_resource) {
      VLOG(2) << "No suitable resource found, will need to try again.";
    } else {
      ResourceStatus** rp = FindOrNull(*resource_map_, *best_resource);
      CHECK(rp);
      LOG(INFO) << "Scheduling task " << (*td)->uid() << " on resource "
                << (*rp)->descriptor().uuid() << " [" << *rp << "]";
      // HandleTaskPlacement both binds the task AND removes it from the
      // runnable set.
      HandleTaskPlacement(*td, (*rp)->mutable_descriptor());
      num_scheduled_tasks++;
    }
  }
  if (num_scheduled_tasks > 0)
    jd_ptr->set_state(JobDescriptor::RUNNING);
  LOG(INFO) << "STOP SCHEDULING " << jd_ptr->uuid();
  return num_scheduled_tasks;
}

uint64_t SimpleScheduler::ScheduleJobs(const vector<JobDescriptor*>& jds_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  uint64_t num_scheduled_tasks = 0;
  for (auto& jd_ptr : jds_ptr) {
    num_scheduled_tasks += ScheduleJob(jd_ptr);
  }
  return num_scheduled_tasks;
}

}  // namespace scheduler
}  // namespace firmament
