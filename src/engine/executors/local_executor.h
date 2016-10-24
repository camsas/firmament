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

// Ths implements a simple local executor. It currently simply starts processes,
// sets their CPU affinities and runs them under perf profiling.
//
// This class, however, also forms the endpoint of remote executors: what they
// do, in fact, is to send a message to a remote resource, which will then
// instruct its local executor to actually start a process.

#ifndef FIRMAMENT_ENGINE_EXECUTORS_LOCAL_EXECUTOR_H
#define FIRMAMENT_ENGINE_EXECUTORS_LOCAL_EXECUTOR_H

#include "engine/executors/executor_interface.h"

#include <vector>
#include <string>
#include <utility>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#else
#error Boost not available!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/task_final_report.pb.h"
#include "engine/executors/task_health_checker.h"
#include "engine/executors/topology_manager.h"
#include "misc/time_interface.h"

namespace firmament {
namespace executor {

using machine::topology::TopologyManager;

class LocalExecutor : public ExecutorInterface {
 public:
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri,
                TimeInterface* time_manager);
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri,
                TimeInterface* time_manager,
                shared_ptr<TopologyManager> topology_mgr);
  bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks);
  void HandleTaskCompletion(TaskDescriptor* td,
                            TaskFinalReport* report);
  void HandleTaskEviction(TaskDescriptor* td);
  void HandleTaskFailure(TaskDescriptor* td);
  void RunTask(TaskDescriptor* td,
               bool firmament_binary);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<LocalExecutor at resource "
                   << to_string(local_resource_id_)
                   << ">";
  }

 protected:
  // Unit tests
  FRIEND_TEST(LocalExecutorTest, SimpleSyncProcessExecutionTest);
  FRIEND_TEST(LocalExecutorTest, SyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, AsyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, ExecutionFailureTest);
  FRIEND_TEST(LocalExecutorTest, SimpleTaskExecutionTest);
  FRIEND_TEST(LocalExecutorTest, TaskExecutionWithArgsTest);
  ResourceID_t local_resource_id_;
  char* AddPerfMonitoringToCommandLine(const unordered_map<string, string>&,
                                       vector<char*>* argv);
  char* AddDebuggingToCommandLine(vector<char*>* argv);
  void CleanUpCompletedTask(const TaskDescriptor& td);
  void CreateDirectories();
  void GetPerfDataFromLine(TaskFinalReport* report,
                           const string& line);
  int32_t RunProcessAsync(TaskID_t task_id,
                          const string& cmdline,
                          vector<string> args,
                          unordered_map<string, string> env,
                          bool perf_monitoring,
                          bool debug,
                          bool default_args,
                          const string& tasklog);
  int32_t RunProcessSync(TaskID_t task_id,
                         const string& cmdline,
                         vector<string> args,
                         unordered_map<string, string> env,
                         bool perf_monitoring,
                         bool debug,
                         bool default_args,
                         const string& tasklog);
  bool _RunTask(TaskDescriptor* td,
                bool firmament_binary);
  string PerfDataFileName(const TaskDescriptor& td);
  void ReadFromPipe(int fd);
  void SetUpEnvironmentForTask(const TaskDescriptor& td,
                               unordered_map<string, string>* env);
  char* TokenizeIntoArgv(const string& str, vector<char*>* argv);
  bool WaitForPerfFile(const string& file_name);
  void WriteToPipe(int fd, void* data, size_t len);
  // This holds the currently configured URI of the coordinator for this
  // resource (which must be unique, for now).
  const string coordinator_uri_;
  // The health manager checks on the liveness of locally managed tasks.
  TaskHealthChecker health_checker_;
  TimeInterface* time_manager_;
  // Local pointer to topology manager
  // TODO(malte): Figure out what to do if this local executor is associated
  // with a dumb worker, who does not have topology support!
  shared_ptr<TopologyManager> topology_manager_;
  // Heartbeat interval for tasks running on the associated resource, in
  // nanoseconds.
  uint64_t heartbeat_interval_;
  boost::mutex exec_mutex_;
  boost::shared_mutex handler_map_mutex_;
  boost::shared_mutex pid_map_mutex_;
  boost::condition_variable exec_condvar_;
  // Map to each task's local handler thread
  unordered_map<TaskID_t, boost::thread*> task_handler_threads_;
  unordered_map<TaskID_t, pid_t> task_pids_;
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_LOCAL_EXECUTOR_H
