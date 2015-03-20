// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of the coordinator knowledge base.

#include "scheduling/knowledge_base.h"

#include <deque>
#include <vector>

#include "misc/map-util.h"
#include "misc/utils.h"

namespace firmament {

KnowledgeBase::KnowledgeBase() {
}

void KnowledgeBase::AddMachineSample(
    const MachinePerfStatisticsSample& sample) {
  boost::lock_guard<boost::mutex> lock(kb_lock_);
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
    q->pop_front();  // drom from the front
  q->push_back(sample);
}

void KnowledgeBase::AddTaskSample(const TaskPerfStatisticsSample& sample) {
  TaskID_t tid = sample.task_id();
  boost::lock_guard<boost::mutex> lock(kb_lock_);
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
    q->pop_front();  // drop from the front
  q->push_back(sample);
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

const deque<TaskFinalReport>* KnowledgeBase::GetFinalStatsForTask(
      TaskID_t task_id) const {
  vector<TaskEquivClass_t>* equiv_classes =
    cost_model_->GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // TODO(ionel): This only gets the stats from the first task equiv class.
  const deque<TaskFinalReport>* res =
    FindOrNull(task_exec_reports_, equiv_classes->front());
  return res;
}

vector<TaskEquivClass_t>* KnowledgeBase::GetTaskEquivClasses(
    TaskID_t task_id) const {
  return cost_model_->GetTaskEquivClasses(task_id);
}

double KnowledgeBase::GetAvgCPIForTEC(TaskEquivClass_t id) {
  const deque<TaskFinalReport>* res = FindOrNull(task_exec_reports_, id);
  CHECK_NOTNULL(res);
  if (!res || res->size() == 0)
    return 0;
  double accumulator = 0;
  for (deque<TaskFinalReport>::const_iterator it = res->begin();
       it != res->end();
       ++it) {
    accumulator += static_cast<double>(it->cycles()) /
      static_cast<double>(it->instructions());
  }
  return accumulator / res->size();
}

double KnowledgeBase::GetAvgIPMAForTEC(TaskEquivClass_t id) {
  const deque<TaskFinalReport>* res = FindOrNull(task_exec_reports_, id);
  if (!res || res->size() == 0)
    return 0;
  double accumulator = 0;
  for (deque<TaskFinalReport>::const_iterator it = res->begin();
       it != res->end();
       ++it) {
    accumulator += static_cast<double>(it->instructions()) /
      static_cast<double>(it->llc_refs());
  }
  return accumulator / res->size();
}

double KnowledgeBase::GetAvgRuntimeForTEC(TaskEquivClass_t id) {
  const deque<TaskFinalReport>* res = FindOrNull(task_exec_reports_, id);
  if (!res || res->size() == 0)
    return 0;
  double accumulator = 0;
  for (deque<TaskFinalReport>::const_iterator it = res->begin();
       it != res->end();
       ++it) {
    // Runtime is in seconds, but a double -- so convert into ms here
    accumulator += it->runtime() * 1000.0;
  }
  return accumulator / res->size();
}

void KnowledgeBase::DumpMachineStats(const ResourceID_t& res_id) const {
  // Sanity checks
  const deque<MachinePerfStatisticsSample>* q =
      FindOrNull(machine_map_, res_id);
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
void KnowledgeBase::ProcessTaskFinalReport(const TaskFinalReport& report,
                                           TaskID_t task_id) {
  boost::lock_guard<boost::mutex> lock(kb_lock_);
  vector<TaskEquivClass_t>* equiv_classes =
    cost_model_->GetTaskEquivClasses(task_id);
  for (vector<TaskEquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    TaskEquivClass_t tec = *it;
    // Check if we already have a record for this equiv class
    deque<TaskFinalReport>* q = FindOrNull(task_exec_reports_, tec);
    if (!q) {
      // Add a blank queue for this task
      CHECK(InsertOrUpdate(&task_exec_reports_, tec,
                           deque<TaskFinalReport>()));
      q = FindOrNull(task_exec_reports_, tec);
      CHECK_NOTNULL(q);
    }
    if (q->size() * sizeof(report) >= MAX_SAMPLE_QUEUE_CAPACITY)
      q->pop_front();  // drop from the front
    q->push_back(report);
    VLOG(1) << "Recorded final report for task " << report.task_id();
  }
}

void KnowledgeBase::SetCostModel(FlowSchedulingCostModelInterface* cost_model) {
  cost_model_ = cost_model;
}

}  // namespace firmament
