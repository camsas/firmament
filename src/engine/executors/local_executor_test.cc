// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// LocalExecutor class unit tests.

#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include "base/common.h"
#include "engine/executors/local_executor.h"

namespace firmament {
namespace executor {

using executor::LocalExecutor;

// The fixture for testing class LocalExecutor.
class LocalExecutorTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  LocalExecutorTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 3;
  }

  virtual ~LocalExecutorTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for
  // LocalExecutor.
};

// Test that we can synchronously execute a binary without arguments.
TEST_F(LocalExecutorTest, SimpleSyncProcessExecutionTest) {
  ResourceID_t rid;
  LocalExecutor le(rid, "");
  vector<string> empty_args;
  unordered_map<string, string> empty_env;
  // We expect to get a return code of 0.
  CHECK_EQ(le.RunProcessSync(1, "/bin/ls", empty_args, empty_env, false, false,
                             false, "/tmp/test"), 0);
}

// Tests that we can synchronously execute a binary with arguments.
TEST_F(LocalExecutorTest, SyncProcessExecutionWithArgsTest) {
  ResourceID_t rid;
  LocalExecutor le(rid, "");
  vector<string> args;
  unordered_map<string, string> env;
  args.push_back("-l");
  // We expect to get a return code of 0.
  CHECK_EQ(le.RunProcessSync(1, "/bin/ls", args, env, false, false, false,
                             "/tmp/test"), 0);
}

// Test that we fail if we try to execute a non-existent binary.
// TODO(malte): commented out as failure reporting does not seem to work. Will
// be redesigned using explicit messaging.
/*TEST_F(LocalExecutorTest, ExecutionFailureTest) {
  ResourceID_t rid;
  LocalExecutor le(rid, "");
  vector<string> empty_args;
  // We expect to fail this time.
  CHECK_NE(le.RunProcessSync("/bin/idonotexist", empty_args, false, false,
                             "/tmp/test"), 0);
}*/

// Tests that we can asynchronously execute a binary with arguments.
// TODO(ionel): The test fails because the thread can still be running. Join
// on the async thread in order to fix the test.
/*TEST_F(LocalExecutorTest, AsyncProcessExecutionWithArgsTest) {
  ResourceID_t rid;
  LocalExecutor le(rid, "");
  vector<string> args;
  unordered_map<string, string> env;
  args.push_back("-l");
  // We expect to get a return code of 0; this is hard-coded and independent of
  // whether the process execution succeeds (the actual execution happens in a
  // newly spawned thread).
  CHECK_EQ(le.RunProcessAsync(1, "/bin/ls", args, env, false, false, false,
                              "/tmp/test"), 0);
}*/

// Tests that we can pass execution information in a task descriptor (just a
// binary name in this case).
TEST_F(LocalExecutorTest, SimpleTaskExecutionTest) {
  ResourceID_t rid;
  LocalExecutor le(rid, "");
  TaskDescriptor* td = new TaskDescriptor;
  td->set_uid(1234ULL);
  td->set_binary("/bin/ls");
  td->set_inject_task_lib(false);
  CHECK(le._RunTask(td, false));
}

// As above, but also passing arguments this time.
TEST_F(LocalExecutorTest, TaskExecutionWithArgsTest) {
  ResourceID_t rid;
  LocalExecutor le(rid, "");
  TaskDescriptor* td = new TaskDescriptor;
  td->set_uid(1234ULL);
  td->set_binary("/bin/ls");
  td->set_inject_task_lib(false);
  td->add_args("-l");
  CHECK(le._RunTask(td, false));
}

}  // namespace executor
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
