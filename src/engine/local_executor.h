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

#include "base/common.h"
#include "base/types.h"
#include "engine/executor_interface.h"

namespace firmament {
namespace executor {

class LocalExecutor : public ExecutorInterface {
 public:
  explicit LocalExecutor(ResourceID_t resource_id);
  bool RunTask(shared_ptr<TaskDescriptor> td);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<LocalExecutor at resource "
                   << to_string(local_resource_id_)
                   << ">";
  }
 protected:
  // Unit tests
  FRIEND_TEST(LocalExecutorTest, SimpleSyncProcessExecutionTest);
  FRIEND_TEST(LocalExecutorTest, SyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, ExecutionFailureTest);
  ResourceID_t local_resource_id_;
  int32_t RunProcessSync(const string& cmdline,
                         vector<string> args,
                         bool default_args);
  void ReadFromPipe(int fd);
  void WriteToPipe(int fd);
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_LOCAL_EXECUTOR_H
