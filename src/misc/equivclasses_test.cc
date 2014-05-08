// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Equivalence class utilities tests.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <fcntl.h>

#include "base/common.h"
#include "base/task_desc.pb.h"
#include "misc/equivclasses.h"

namespace firmament {

// The fixture for testing class EquivClasses.
class EquivClassesTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  EquivClassesTest() {
    // You can do set-up work for each test here.
  }

  virtual ~EquivClassesTest() {
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
  // EquivClasses.
};

TEST_F(EquivClassesTest, TaskEquivClassGenerate) {
  TaskDescriptor td;
  td.set_binary("/bin/ls");
  CHECK_EQ(GenerateTaskEquivClass(td), 6807036345993470132ULL);
}

TEST_F(EquivClassesTest, TaskEquivClassGenerateRootTask) {
  JobDescriptor jd;
  TaskDescriptor* rtd = jd.mutable_root_task();
  rtd->set_binary("/bin/ls");
  CHECK_EQ(GenerateTaskEquivClass(*rtd), 6807036345993470132ULL);
}

TEST_F(EquivClassesTest, ResourceEquivClassGenerate) {
  // Test resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open("../tests/testdata/machine_topo.pbin", O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);
  // TODO(malte): update below as we fix the REC generation to no longer use
  // resources' friendly names.
  //CHECK_EQ(GenerateResourceTopologyEquivClass(machine_tmpl),
  //         3470876451359758593ULL);
  // Version without friendly name being included in hash
  CHECK_EQ(GenerateResourceTopologyEquivClass(machine_tmpl),
           11206154957596880893ULL);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
