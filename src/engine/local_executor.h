// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Ths implements a simple local executor. It currently simply starts processes,
// sets their CPU affinities and runs them under perf profiling.
//
// This class, however, also forms the endpoint of remote executors: what they
// do, in fact, is to send a message to a remote resource, which will then
// instruct its local executor to actually start a process.

#ifndef FIRMAMENT_ENGINE_LOCAL_EXECUTOR_H
#define FIRMAMENT_ENGINE_LOCAL_EXECUTOR_H

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
#include "engine/executor_interface.h"
#include "engine/task_health_checker.h"
#include "engine/topology_manager.h"

namespace firmament {
namespace executor {

using machine::topology::TopologyManager;

class LocalExecutor : public ExecutorInterface {
 public:
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri);
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri,
                shared_ptr<TopologyManager> topology_mgr);
  bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks);
  void HandleTaskCompletion(const TaskDescriptor& td,
                            TaskFinalReport* report);
  void HandleTaskFailure(const TaskDescriptor& td);
  void RunTask(TaskDescriptor* td,
               bool firmament_binary);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<LocalExecutor at resource "
                   << to_string(local_resource_id_)
                   << ">";
  }

 protected:
  typedef pair<string, string> EnvPair_t;
  // Unit tests
  FRIEND_TEST(LocalExecutorTest, SimpleSyncProcessExecutionTest);
  FRIEND_TEST(LocalExecutorTest, SyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, AsyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, ExecutionFailureTest);
  FRIEND_TEST(LocalExecutorTest, SimpleTaskExecutionTest);
  FRIEND_TEST(LocalExecutorTest, TaskExecutionWithArgsTest);
  ResourceID_t local_resource_id_;
  char* AddPerfMonitoringToCommandLine(vector<char*>* argv);
  char* AddDebuggingToCommandLine(vector<char*>* argv);
  void CleanUpCompletedTask(const TaskDescriptor& td);
  void CreateDirectories();
  void GetPerfDataFromLine(TaskFinalReport* report,
                           const string& line);
  int32_t RunProcessAsync(const string& cmdline,
                          vector<string> args,
                          bool perf_monitoring,
                          bool debug,
                          bool default_args,
                          bool inject_task_lib,
                          const string& tasklog);
  int32_t RunProcessSync(const string& cmdline,
                         vector<string> args,
                         bool perf_monitoring,
                         bool debug,
                         bool default_args,
                         bool inject_task_lib,
                         const string& tasklog);
  bool _RunTask(TaskDescriptor* td,
                bool firmament_binary);
  string PerfDataFileName(const TaskDescriptor& td);
  void ReadFromPipe(int fd);
  void SetUpEnvironmentForTask(const TaskDescriptor& td);
  char* TokenizeIntoArgv(const string& str, vector<char*>* argv);
  void WriteToPipe(int fd, void* data, size_t len);
  // This holds the currently configured URI of the coordinator for this
  // resource (which must be unique, for now).
  const string coordinator_uri_;
  // The health manager checks on the liveness of locally managed tasks.
  TaskHealthChecker health_checker_;
  // Local pointer to topology manager
  // TODO(malte): Figure out what to do if this local executor is associated
  // with a dumb worker, who does not have topology support!
  shared_ptr<TopologyManager> topology_manager_;
  // Heartbeat interval for tasks running on the associated resource, in
  // nanoseconds.
  uint64_t heartbeat_interval_;
  boost::mutex exec_mutex_;
  boost::shared_mutex handler_map_mutex_;
  boost::condition_variable exec_condvar_;

  unordered_map<TaskID_t, uint64_t> task_start_times_;
  unordered_map<TaskID_t, boost::thread*> task_handler_threads_;
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_LOCAL_EXECUTOR_H
