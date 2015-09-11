// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The task library class is part of libfirmament, the library that gets linked
// into task binaries. Eventually, we should make this an interface, and support
// platform-specific classes.

#ifndef FIRMAMENT_ENGINE_TASK_LIB_H
#define FIRMAMENT_ENGINE_TASK_LIB_H

#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "base/task_perf_statistics_sample.pb.h"
#include "storage/reference_types.h"
#include "base/task_interface.h"
#include "messages/base_message.pb.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.h"
#include "platforms/unix/procfs_monitor.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_channel.h"
#include "storage/types.h"

namespace firmament {

using platform_unix::ProcFSMonitor;
using platform_unix::streamsockets::StreamSocketsAdapter;
using platform_unix::streamsockets::StreamSocketsChannel;

class TaskLib {
 public:
  TaskLib();
  virtual ~TaskLib();

  void RunMonitor(boost::thread::id main_thread_id);
  void AwaitNextMessage();
  bool ConnectToCoordinator(const string& coordinator_uri);
  // CIEL programming model
  //virtual const string Construct(const DataObject& object);
  void Spawn(const ReferenceInterface& code,
             vector<ReferenceInterface>* dependencies,
             vector<FutureReference>* outputs);
  void Publish(const vector<ConcreteReference>& references);
  //virtual void TailSpawn(const ConcreteReference& code);

  void* GetObjectStart(const DataObjectID_t& id);
  void GetObjectEnd(const DataObjectID_t& id);
  void* PutObjectStart(const DataObjectID_t& id, size_t size);
  void PutObjectEnd(const DataObjectID_t& id, size_t size);
  void* Extend(const DataObjectID_t& id, size_t old_size, size_t new_size);

  void SetCompleted(double completed);

  // Terminate tasklib.
  void Stop(bool success);

 protected:
  StreamSocketsAdapter<BaseMessage>* m_adapter_;
  StreamSocketsChannel<BaseMessage>* chan_;
  bool exit_;
  // TODO(malte): transform this into a better representation
  string coordinator_uri_;
  ResourceID_t resource_id_;
  TaskID_t task_id_;
  TaskDescriptor task_descriptor_;

  void AddTaskStatisticsToHeartbeat(
      const ProcFSMonitor::ProcessStatistics_t& proc_stats,
      TaskPerfStatisticsSample* stats);
  void ConvertTaskArgs(int argc, char *argv[], vector<char*>* arg_vec);
  void HandleIncomingMessage(BaseMessage *bm,
                             const string& remote_endpoint);
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);

  bool PullTaskInformationFromCoordinator(TaskID_t task_id,
                                          TaskDescriptor* desc);
  void SendFinalizeMessage(bool success);
  void SendHeartbeat(const ProcFSMonitor::ProcessStatistics_t& stats);
  bool SendMessageToCoordinator(BaseMessage* msg);

  void setUpStorageEngine();
  void AddCompletionStatistics(TaskPerfStatisticsSample *ts);

 private:
  pid_t pid_;
  volatile bool task_running_;
  uint64_t heartbeat_seq_number_;
  bool use_procfs_;
  string hostname_;
  volatile bool stop_;
  bool internal_completed_;
  // If set, gives the fraction of task completed.
  volatile double completed_;
  ProcFSMonitor task_perf_monitor_;
  unique_ptr<FILE> completion_file_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_TASK_LIB_H
