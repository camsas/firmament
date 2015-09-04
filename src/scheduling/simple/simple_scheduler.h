// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive simple-minded queue-based scheduler.

#ifndef FIRMAMENT_SCHEDULING_SIMPLE_SIMPLE_SCHEDULER_H
#define FIRMAMENT_SCHEDULING_SIMPLE_SIMPLE_SCHEDULER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/event_notifier_interface.h"
#include "scheduling/knowledge_base.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace scheduler {

class SimpleScheduler : public EventDrivenScheduler {
 public:
  SimpleScheduler(shared_ptr<JobMap_t> job_map,
                  shared_ptr<ResourceMap_t> resource_map,
                  ResourceTopologyNodeDescriptor* resource_topology,
                  shared_ptr<ObjectStoreInterface> object_store,
                  shared_ptr<TaskMap_t> task_map,
                  shared_ptr<KnowledgeBase> knowledge_base,
                  shared_ptr<TopologyManager> topo_mgr,
                  MessagingAdapterInterface<BaseMessage>* m_adapter,
                  EventNotifierInterface* event_notifier,
                  ResourceID_t coordinator_res_id,
                  const string& coordinator_uri);
  ~SimpleScheduler();
  void HandleTaskFinalReport(const TaskFinalReport& report,
                             TaskDescriptor* td_ptr);
  void PopulateSchedulerResourceUI(ResourceID_t res_id,
                                   TemplateDictionary* dict) const;
  void PopulateSchedulerTaskUI(TaskID_t task_id,
                               TemplateDictionary* dict) const;
  uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats);
  uint64_t ScheduleJob(JobDescriptor* jd_ptr,
                       SchedulerStats* scheduler_stats);
  uint64_t ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                        SchedulerStats* scheduler_stats);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<SimpleScheduler>";
  }

 protected:
  const ResourceID_t* FindResourceForTask(TaskDescriptor* task_desc);

 private:
  // Unit tests
  FRIEND_TEST(SimpleSchedulerTest, LazyGraphReductionTest);
  FRIEND_TEST(SimpleSchedulerTest, ObjectIDToReferenceDescLookup);
  FRIEND_TEST(SimpleSchedulerTest, ProducingTaskLookup);
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SIMPLE_SIMPLE_SCHEDULER_H
