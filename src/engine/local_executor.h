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
#include "engine/executor_interface.h"

namespace firmament {
namespace executor {

class LocalExecutor : public ExecutorInterface {
 public:
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri);
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
  FRIEND_TEST(LocalExecutorTest, ExecutionFailureTest);
  FRIEND_TEST(LocalExecutorTest, SimpleTaskExecutionTest);
  FRIEND_TEST(LocalExecutorTest, TaskExecutionWithArgsTest);
  ResourceID_t local_resource_id_;
  int32_t RunProcessSync(const string& cmdline,
                         vector<string> args,
                         bool perf_monitoring,
                         bool default_args);
  bool _RunTask(TaskDescriptor* td,
                bool firmament_binary);
  string PerfDataFileName(const TaskDescriptor& td);
  void ReadFromPipe(int fd);
  void SetUpEnvironmentForTask(const TaskDescriptor& td);
  void WriteToPipe(int fd);
  // This holds the currently configured URI of the coordinator for this
  // resource (which must be unique, for now).
  const string coordinator_uri_;
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_LOCAL_EXECUTOR_H
