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

// Implementation of the coordinator knowledge base.

#include "scheduling/knowledge_base.h"

#include <algorithm>
#include <deque>
#include <vector>

#include <sys/fcntl.h>

#include "base/task_desc.pb.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"

DEFINE_bool(serialize_knowledge_base, false,
            "True if we should serialize knowledge base");
DEFINE_string(serial_machine_samples, "serial_machine_samples",
              "Path to the file where the knowledge base will serialize machine"
              " specific information");
DEFINE_string(serial_task_samples, "serial_task_samples",
              "Path to the file where the knowledge base will serialize task"
              " specific information");
DEFINE_uint64(max_sample_queue_size, 100,
              "Maximum size (in KB) of each queue storing historical data");

namespace firmament {

KnowledgeBase::KnowledgeBase()
  : data_layer_manager_(NULL) {
  KnowledgeBase(NULL);
}

KnowledgeBase::KnowledgeBase(DataLayerManagerInterface* data_layer_manager)
  : data_layer_manager_(data_layer_manager) {
  if (FLAGS_serialize_knowledge_base) {
    serial_machine_samples_.open(FLAGS_serial_machine_samples.c_str(),
                                 ios::out | ios::trunc | ios::binary);
    CHECK(serial_machine_samples_.is_open());
    raw_machine_output_ =
      new ::google::protobuf::io::OstreamOutputStream(&serial_machine_samples_);
    coded_machine_output_ =
        new ::google::protobuf::io::CodedOutputStream(raw_machine_output_);

    serial_task_samples_.open(FLAGS_serial_task_samples.c_str(),
                              ios::out | ios::trunc | ios::binary);
    CHECK(serial_task_samples_.is_open());
    raw_task_output_ =
      new ::google::protobuf::io::OstreamOutputStream(&serial_task_samples_);
    coded_task_output_ =
        new ::google::protobuf::io::CodedOutputStream(raw_task_output_);
  }
}

KnowledgeBase::~KnowledgeBase() {
  if (serial_machine_samples_.is_open()) {
    delete coded_machine_output_;
    delete raw_machine_output_;
    serial_machine_samples_.close();
  }
  if (serial_task_samples_.is_open()) {
    delete coded_task_output_;
    delete raw_task_output_;
    serial_task_samples_.close();
  }
  // We don't have to delete data_layer_manager because it's not owned by
  // the KnowledgeBase.
}

void KnowledgeBase::AddMachineSample(const ResourceStats& sample) {
  boost::lock_guard<boost::upgrade_mutex> lock(kb_lock_);
  ResourceID_t rid = ResourceIDFromString(sample.resource_id());
  // Check if we already have a record for this machine
  deque<ResourceStats>* q = FindOrNull(machine_map_, rid);
  if (!q) {
    // Add a blank queue for this machine
    CHECK(InsertOrUpdate(&machine_map_, rid, deque<ResourceStats>()));
    q = FindOrNull(machine_map_, rid);
    CHECK_NOTNULL(q);
  }
  if (q->size() * sizeof(sample) >= FLAGS_max_sample_queue_size * KB_TO_BYTES)
    q->pop_front();  // drop from the front
  q->push_back(sample);
  if (FLAGS_serialize_knowledge_base) {
    string message_string;
    sample.SerializeToString(&message_string);
    coded_machine_output_->WriteVarint32(message_string.size());
    coded_machine_output_->WriteRaw(message_string.data(),
                                    message_string.size());
  }
}

void KnowledgeBase::AddTaskStatsSample(const TaskStats& sample) {
  TaskID_t tid = sample.task_id();
  boost::lock_guard<boost::upgrade_mutex> lock(kb_lock_);
  // Check if we already have a record for this task
  deque<TaskStats>* q = FindOrNull(task_map_, tid);
  if (!q) {
    // Add a blank queue for this task
    CHECK(InsertOrUpdate(&task_map_, tid, deque<TaskStats>()));
    q = FindOrNull(task_map_, tid);
    CHECK_NOTNULL(q);
  }
  if (q->size() * sizeof(sample) >= FLAGS_max_sample_queue_size * KB_TO_BYTES)
    q->pop_front();  // drop from the front
  q->push_back(sample);
  if (FLAGS_serialize_knowledge_base) {
    string message_string;
    sample.SerializeToString(&message_string);
    coded_task_output_->WriteVarint32(message_string.size());
    coded_task_output_->WriteRaw(message_string.data(), message_string.size());
  }
}

void KnowledgeBase::DumpMachineStats(const ResourceID_t& res_id) const {
  // Sanity checks
  const deque<ResourceStats>* q = FindOrNull(machine_map_, res_id);
  if (!q)
    return;
  // Dump
  LOG(INFO) << "STATS FOR " << res_id << ": ";
  LOG(INFO) << "Have " << q->size() << " samples.";
  for (deque<ResourceStats>::const_iterator it = q->begin();
      it != q->end();
      ++it) {
    LOG(INFO) << it->mem_capacity() * (1.0 - it->mem_utilization());
  }
}

bool KnowledgeBase::GetLatestStatsForMachine(
    ResourceID_t id, ResourceStats* sample) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
  const deque<ResourceStats>* res = FindOrNull(machine_map_, id);
  if (!res)
    return false;
  // We make a copy here, as we lose the lock when returning
  sample->CopyFrom(res->back());
  return true;
}

const deque<ResourceStats> KnowledgeBase::GetStatsForMachine(
      ResourceID_t id) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
  const deque<ResourceStats>* res = FindOrNull(machine_map_, id);
  if (!res) {
    const deque<ResourceStats> empty;
    return empty;
  }
  // We make a copy here, as we lose the lock when returning
  const deque<ResourceStats> copy(*res);
  return copy;
}

const deque<TaskStats>* KnowledgeBase::GetStatsForTask(TaskID_t id) const {
  const deque<TaskStats>* res = FindOrNull(task_map_, id);
  return res;
}

const deque<TaskFinalReport>* KnowledgeBase::GetFinalReportForTask(
      TaskID_t task_id) const {
  const deque<TaskFinalReport>* res = FindOrNull(task_exec_reports_, task_id);
  return res;
}

const deque<TaskFinalReport>* KnowledgeBase::GetFinalReportsForTEC(
      EquivClass_t ec_id) const {
  const deque<TaskFinalReport>* res = FindOrNull(task_exec_reports_, ec_id);
  return res;
}

double KnowledgeBase::GetAvgCPIForTEC(EquivClass_t id) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
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

double KnowledgeBase::GetAvgIPMAForTEC(EquivClass_t id) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
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

double KnowledgeBase::GetAvgPsPIForTEC(EquivClass_t id) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
  const deque<TaskFinalReport>* res = FindOrNull(task_exec_reports_, id);
  if (!res || res->size() == 0)
    return 0;
  double accumulator = 0;
  for (deque<TaskFinalReport>::const_iterator it = res->begin();
       it != res->end();
       ++it) {
    accumulator += static_cast<double>(it->runtime() * 10000000000.0) /
      static_cast<double>(it->instructions());
  }
  return accumulator / res->size();
}

double KnowledgeBase::GetAvgRuntimeForTEC(EquivClass_t id) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
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

uint64_t KnowledgeBase::GetRuntimeForTask(TaskID_t task_id) {
  boost::lock_guard<boost::upgrade_mutex> lock_shared(kb_lock_);
  const deque<TaskFinalReport>* rep = GetFinalReportForTask(task_id);
  CHECK_NOTNULL(rep);
  CHECK(rep->size() > 0);
  return rep->front().finish_time() - rep->front().start_time();
}

void KnowledgeBase::LoadKnowledgeBaseFromFile() {
  // Load the machine samples.
  fstream machine_samples(FLAGS_serial_machine_samples.c_str(),
                          ios::in | ios::binary);
  if (!machine_samples) {
    LOG(FATAL) << "Could not open machine samples file";
  }
  ::google::protobuf::io::ZeroCopyInputStream* raw_machine_input =
      new ::google::protobuf::io::IstreamInputStream(&machine_samples);
  ::google::protobuf::io::CodedInputStream* coded_machine_input =
      new ::google::protobuf::io::CodedInputStream(raw_machine_input);
  for (bool read_sample = true; read_sample; ) {
    uint32_t msg_size;
    read_sample = coded_machine_input->ReadVarint32(&msg_size);
    if (!read_sample) {
      break;
    }
    string message;
    read_sample = coded_machine_input->ReadString(&message, msg_size);
    if (!read_sample) {
      LOG(ERROR) << "Unexpected format of the input file";
      break;
    }
    ResourceStats machine_stats;
    machine_stats.ParseFromString(message);
    AddMachineSample(machine_stats);
  }
  delete coded_machine_input;
  delete raw_machine_input;
  machine_samples.close();


  // Load the task samples.
  fstream task_samples(FLAGS_serial_task_samples.c_str(),
                       ios::in | ios::binary);
  if (!task_samples) {
    LOG(FATAL) << "Could not open task samples file";
  }
  ::google::protobuf::io::ZeroCopyInputStream* raw_task_input =
      new ::google::protobuf::io::IstreamInputStream(&task_samples);
  ::google::protobuf::io::CodedInputStream* coded_task_input =
      new ::google::protobuf::io::CodedInputStream(raw_task_input);
  for (bool read_sample = true; read_sample; ) {
    uint32_t msg_size;
    read_sample = coded_task_input->ReadVarint32(&msg_size);
    if (!read_sample) {
      break;
    }
    string message;
    read_sample = coded_task_input->ReadString(&message, msg_size);
    if (!read_sample) {
      LOG(ERROR) << "Unexpected format of the input file";
      break;
    }
    TaskStats task_stats;
    task_stats.ParseFromString(message);
    AddTaskStatsSample(task_stats);
  }
  delete coded_task_input;
  delete raw_task_input;
  task_samples.close();
}

void KnowledgeBase::ProcessTaskFinalReport(
    const vector<EquivClass_t>& equiv_classes,
    const TaskFinalReport& report) {
  boost::lock_guard<boost::upgrade_mutex> lock(kb_lock_);
  for (auto& tec : equiv_classes) {
    // Check if we already have a record for this equiv class
    deque<TaskFinalReport>* reports = FindOrNull(task_exec_reports_, tec);
    if (!reports) {
      // Add a blank queue for this task
      CHECK(InsertOrUpdate(&task_exec_reports_, tec,
                           deque<TaskFinalReport>()));
      reports = FindOrNull(task_exec_reports_, tec);
      CHECK_NOTNULL(reports);
    }
    if (reports->size() * sizeof(report) >=
        FLAGS_max_sample_queue_size * KB_TO_BYTES) {
      reports->pop_front();
    }
    reports->push_back(report);
    VLOG(2) << "Recorded final report for task " << report.task_id();
  }
}

}  // namespace firmament
