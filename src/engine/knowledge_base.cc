// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of the coordinator knowledge base.

#include "engine/knowledge_base.h"

#include <queue>

#include "misc/map-util.h"
#include "misc/utils.h"

namespace firmament {

KnowledgeBase::KnowledgeBase() {
}

void KnowledgeBase::AddMachineSample(const MachinePerfStatisticsSample& sample) {
  ResourceID_t rid = ResourceIDFromString(sample.resource_id());
  // Check if we already have a record for this machine
  queue<MachinePerfStatisticsSample>* q =
      FindOrNull(machine_map_, rid);
  if (!q) {
    // Add a blank queue for this machine
    CHECK(InsertOrUpdate(&machine_map_, rid,
                         queue<MachinePerfStatisticsSample>()));
    q = FindOrNull(machine_map_, rid);
    CHECK_NOTNULL(q);
  }
  if (q->size() * sizeof(sample) >= MAX_SAMPLE_QUEUE_CAPACITY)
    q->pop();  // drom from the front
  q->push(sample);
}

void KnowledgeBase::AddTaskSample(const TaskPerfStatisticsSample& sample) {
  TaskID_t tid = sample.task_id();
  // Check if we already have a record for this task
  queue<TaskPerfStatisticsSample>* q = FindOrNull(task_map_, tid);
  if (!q) {
    // Add a blank queue for this task
    CHECK(InsertOrUpdate(&task_map_, tid,
                         queue<TaskPerfStatisticsSample>()));
    q = FindOrNull(task_map_, tid);
    CHECK_NOTNULL(q);
  }
  if (q->size() * sizeof(sample) >= MAX_SAMPLE_QUEUE_CAPACITY)
    q->pop();  // drom from the front
  q->push(sample);
}



}  // namespace firmament
