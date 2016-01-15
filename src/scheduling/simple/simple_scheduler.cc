// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
    TimeInterface* time_manager)
    : EventDrivenScheduler(job_map, resource_map, resource_topology,
                           object_store, task_map, knowledge_base, topo_mgr,
                           m_adapter, event_notifier, coordinator_res_id,
                           coordinator_uri, time_manager) {
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
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  vector<JobDescriptor*> jobs;
  for (auto& job_id_jd : jobs_to_schedule_) {
    jobs.push_back(job_id_jd.second);
  }
  uint64_t num_scheduled_tasks = ScheduleJobs(jobs, scheduler_stats);
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
  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime = scheduler_timer.elapsed().wall
      / NANOSECONDS_IN_MICROSECOND;
  }
  LOG(INFO) << "STOP SCHEDULING " << jd_ptr->uuid();
  return num_scheduled_tasks;
}

uint64_t SimpleScheduler::ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                       SchedulerStats* scheduler_stats) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  uint64_t num_scheduled_tasks = 0;
  boost::timer::cpu_timer scheduler_timer;
  for (auto& jd_ptr : jds_ptr) {
    num_scheduled_tasks += ScheduleJob(jd_ptr, scheduler_stats);
  }
  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime = scheduler_timer.elapsed().wall
      / NANOSECONDS_IN_MICROSECOND;
  }
  return num_scheduled_tasks;
}

}  // namespace scheduler
}  // namespace firmament
