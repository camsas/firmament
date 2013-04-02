// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of the coordinator knowledge base.

#include "engine/knowledge_base.h"

#include <deque>

#include "misc/map-util.h"
#include "misc/utils.h"

namespace firmament {

KnowledgeBase::KnowledgeBase() {
}

void KnowledgeBase::AddMachineSample(
    const MachinePerfStatisticsSample& sample) {
  ResourceID_t rid = ResourceIDFromString(sample.resource_id());
  // Check if we already have a record for this machine
  deque<MachinePerfStatisticsSample>* q =
      FindOrNull(machine_map_, rid);
  if (!q) {
    // Add a blank queue for this machine
    CHECK(InsertOrUpdate(&machine_map_, rid,
                         deque<MachinePerfStatisticsSample>()));
    q = FindOrNull(machine_map_, rid);
    CHECK_NOTNULL(q);
  }
  if (q->size() * sizeof(sample) >= MAX_SAMPLE_QUEUE_CAPACITY)
    q->pop_back();  // drom from the front
  q->push_front(sample);
}

void KnowledgeBase::AddTaskSample(const TaskPerfStatisticsSample& sample) {
  TaskID_t tid = sample.task_id();
  // Check if we already have a record for this task
  deque<TaskPerfStatisticsSample>* q = FindOrNull(task_map_, tid);
  if (!q) {
    // Add a blank queue for this task
    CHECK(InsertOrUpdate(&task_map_, tid,
                         deque<TaskPerfStatisticsSample>()));
    q = FindOrNull(task_map_, tid);
    CHECK_NOTNULL(q);
  }
  if (q->size() * sizeof(sample) >= MAX_SAMPLE_QUEUE_CAPACITY)
    q->pop_back();  // drop from the front
  q->push_front(sample);
}

const deque<MachinePerfStatisticsSample>* KnowledgeBase::GetStatsForMachine(
      ResourceID_t id) const {
  const deque<MachinePerfStatisticsSample>* res = FindOrNull(machine_map_, id);
  return res;
}

const deque<TaskPerfStatisticsSample>* KnowledgeBase::GetStatsForTask(
      TaskID_t id) const {
  const deque<TaskPerfStatisticsSample>* res = FindOrNull(task_map_, id);
  return res;
}

void KnowledgeBase::DumpMachineStats(const ResourceID_t& res_id) const {
  // Sanity checks
  const deque<MachinePerfStatisticsSample>* q = FindOrNull(machine_map_, res_id);
  if (!q)
    return;
  // Dump
  LOG(INFO) << "STATS FOR " << res_id << ": ";
  LOG(INFO) << "Have " << q->size() << " samples.";
  for (deque<MachinePerfStatisticsSample>::const_iterator it = q->begin();
      it != q->end();
      ++it) {
    LOG(INFO) << it->free_ram();
  }
}

}  // namespace firmament
