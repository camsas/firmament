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

// Naive implementation of a simple-minded queue-based scheduler.

#include "scheduling/simple/simple_scheduler.h"

#include <boost/timer/timer.hpp>

#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "storage/object_store_interface.h"

DEFINE_bool(randomly_place_tasks, false, "Place tasks randomly");

namespace firmament {
namespace scheduler {

using store::ObjectStoreInterface;

SimpleScheduler::SimpleScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    ResourceTopologyNodeDescriptor* resource_topology,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    SchedulingEventNotifierInterface* event_notifier,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri,
    TimeInterface* time_manager,
    TraceGenerator* trace_generator)
    : EventDrivenScheduler(job_map, resource_map, resource_topology,
                           object_store, task_map, knowledge_base, topo_mgr,
                           m_adapter, event_notifier, coordinator_res_id,
                           coordinator_uri, time_manager, trace_generator) {
  VLOG(1) << "SimpleScheduler initiated.";
}

SimpleScheduler::~SimpleScheduler() {
}

bool SimpleScheduler::FindResourceForTask(const TaskDescriptor& task_desc,
                                          ResourceID_t* best_resource) {
  // TODO(malte): This is an extremely simple-minded approach to resource
  // selection (i.e. the essence of scheduling). We will simply traverse the
  // resource map in some order, and grab the first resource available.
  VLOG(2) << "Trying to place task " << task_desc.uid() << "...";
  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    VLOG(3) << "Considering resource " << res_iter->first
            << ", which is in state "
            << res_iter->second->descriptor().state();
    if (res_iter->second->descriptor().state() ==
        ResourceDescriptor::RESOURCE_IDLE) {
      *best_resource = res_iter->first;
      return true;
    }
  }
  // We have not found any idle resources in our local resource map. At this
  // point, we should start looking beyond the machine boundary and towards
  // remote resources.
  return false;
}

bool SimpleScheduler::FindRandomResourceForTask(const TaskDescriptor& task_desc,
                                                ResourceID_t* best_resource) {
  // TODO(malte): This is an extremely simple-minded approach to resource
  // selection (i.e. the essence of scheduling). We will simply traverse the
  // resource map in some order, and grab the first resource available.
  VLOG(2) << "Trying to place task " << task_desc.uid() << "...";
  vector<ResourceStatus*> resources;
  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    resources.push_back(res_iter->second);
  }
  for (uint64_t max_attempts = 2000; max_attempts > 0; max_attempts--) {
    uint32_t resource_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % resources.size();
    if (resources[resource_index]->descriptor().state() ==
        ResourceDescriptor::RESOURCE_IDLE) {
      *best_resource = ResourceIDFromString(
          resources[resource_index]->descriptor().uuid());
      return true;
    }
  }
  // We have not found any idle resources in our local resource map. At this
  // point, we should start looking beyond the machine boundary and towards
  // remote resources.
  return false;
}

void SimpleScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                           TaskFinalReport* report) {
  ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceDescriptor* rd_ptr = rs->mutable_descriptor();
  // TODO(ionel): This assumes no PU sharing.
  rd_ptr->clear_current_running_tasks();
  EventDrivenScheduler::HandleTaskCompletion(td_ptr, report);
}

void SimpleScheduler::HandleTaskEviction(TaskDescriptor* td_ptr,
                                         ResourceDescriptor* rd_ptr) {
  // TODO(ionel): This assumes no PU sharing.
  rd_ptr->clear_current_running_tasks();
  EventDrivenScheduler::HandleTaskEviction(td_ptr, rd_ptr);
}

void SimpleScheduler::HandleTaskFailure(TaskDescriptor* td_ptr) {
  ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceDescriptor* rd_ptr = rs->mutable_descriptor();
  // TODO(ionel): This assumes no PU sharing.
  rd_ptr->clear_current_running_tasks();
  EventDrivenScheduler::HandleTaskFailure(td_ptr);
}

void SimpleScheduler::KillRunningTask(
    TaskID_t task_id,
    TaskKillMessage::TaskKillReason reason) {
  // TODO(ionel): Make sure the task is removed from current_running_tasks
  // when it is killed.
}

void SimpleScheduler::HandleTaskFinalReport(const TaskFinalReport& report,
                                            TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  EventDrivenScheduler::HandleTaskFinalReport(report, td_ptr);
  TaskID_t task_id = td_ptr->uid();
  vector<EquivClass_t> equiv_classes;
  // We create two equivalence class IDs:
  // 1) an equivalence class ID per task_id
  // 2) an equivalence class ID per program
  // We create these equivalence class IDs in order to make the EC
  // statistics view on the web UI work.
  EquivClass_t task_agg =
    static_cast<EquivClass_t>(HashCommandLine(*td_ptr));
  equiv_classes.push_back(task_agg);
  equiv_classes.push_back(task_id);
  knowledge_base_->ProcessTaskFinalReport(equiv_classes, report);
}

void SimpleScheduler::PopulateSchedulerResourceUI(
    ResourceID_t res_id,
    TemplateDictionary* dict) const {
  // At the moment to we do not show any resource-specific information
  // for the simple scheduler.
}

void SimpleScheduler::PopulateSchedulerTaskUI(TaskID_t task_id,
                                              TemplateDictionary* dict) const {
  // At the moment to we do not show any task-specific information
  // for the simple scheduler.
}

uint64_t SimpleScheduler::ScheduleAllJobs(SchedulerStats* scheduler_stats) {
  return ScheduleAllJobs(scheduler_stats, NULL);
}

uint64_t SimpleScheduler::ScheduleAllJobs(SchedulerStats* scheduler_stats,
                                          vector<SchedulingDelta>* deltas) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  vector<JobDescriptor*> jobs;
  for (auto& job_id_jd : jobs_to_schedule_) {
    jobs.push_back(job_id_jd.second);
  }
  uint64_t num_scheduled_tasks = ScheduleJobs(jobs, scheduler_stats, deltas);
  return num_scheduled_tasks;
}

uint64_t SimpleScheduler::ScheduleJob(JobDescriptor* jd_ptr,
                                      SchedulerStats* scheduler_stats) {
  uint64_t num_scheduled_tasks = 0;
  VLOG(2) << "Preparing to schedule job " << jd_ptr->uuid();
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  LOG(INFO) << "START SCHEDULING " << jd_ptr->uuid();
  boost::timer::cpu_timer scheduler_timer;
  // Get the set of runnable tasks for this job
  unordered_set<TaskID_t> runnable_tasks = ComputeRunnableTasksForJob(jd_ptr);
  VLOG(2) << "Scheduling job " << jd_ptr->uuid() << ", which has "
          << runnable_tasks.size() << " runnable tasks.";
  JobID_t job_id = JobIDFromString(jd_ptr->uuid());
  for (unordered_set<TaskID_t>::const_iterator task_iter =
       runnable_tasks.begin();
       task_iter != runnable_tasks.end();
       ++task_iter) {
    TaskDescriptor* td = FindPtrOrNull(*task_map_, *task_iter);
    CHECK(td);
    trace_generator_->TaskSubmitted(td);
    VLOG(2) << "Considering task " << td->uid() << ":\n"
            << td->DebugString();

    ResourceID_t best_resource;
    bool success = false;
    if (FLAGS_randomly_place_tasks) {
      success = FindRandomResourceForTask(*td, &best_resource);
    } else {
      success = FindResourceForTask(*td, &best_resource);
    }
    if (!success) {
      VLOG(2) << "No suitable resource found, will need to try again.";
    } else {
      ResourceStatus* rp = FindPtrOrNull(*resource_map_, best_resource);
      CHECK(rp);
      VLOG(1) << "Scheduling task " << td->uid() << " on resource "
              << rp->descriptor().uuid();
      // Remove the task from the runnable set.
      runnable_tasks_[job_id].erase(td->uid());
      HandleTaskPlacement(td, rp->mutable_descriptor());
      num_scheduled_tasks++;
    }
  }
  if (num_scheduled_tasks > 0)
    jd_ptr->set_state(JobDescriptor::RUNNING);
  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime_ = scheduler_timer.elapsed().wall /
      NANOSECONDS_IN_MICROSECOND;
  }
  LOG(INFO) << "STOP SCHEDULING " << jd_ptr->uuid();
  return num_scheduled_tasks;
}

uint64_t SimpleScheduler::ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                       SchedulerStats* scheduler_stats,
                                       vector<SchedulingDelta>* deltas) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  uint64_t num_scheduled_tasks = 0;
  boost::timer::cpu_timer scheduler_timer;
  // TODO(ionel): Populate scheduling deltas!
  for (auto& jd_ptr : jds_ptr) {
    num_scheduled_tasks += ScheduleJob(jd_ptr, scheduler_stats);
  }
  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime_ =
      static_cast<uint64_t>(scheduler_timer.elapsed().wall) /
      NANOSECONDS_IN_MICROSECOND;
  }
  return num_scheduled_tasks;
}

}  // namespace scheduler
}  // namespace firmament
