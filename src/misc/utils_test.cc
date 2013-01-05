// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Utility function unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/task_desc.pb.h"
#include "misc/utils.h"

namespace firmament {

// The fixture for testing class Utils.
class UtilsTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  UtilsTest() {
    // You can do set-up work for each test here.
  }

  virtual ~UtilsTest() {
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
  // Utils.
};

// Tests DO ID generation from producer ID and output ID.
TEST_F(UtilsTest, DataObjectIDGenerateTest) {
  TaskID_t pid = 1234;
  TaskOutputID_t oid = 0;
  CHECK_EQ(GenerateDataObjectID(pid, oid), 175247688336);
}

// Tests DO ID generation from TD.
TEST_F(UtilsTest, DataObjectIDGenerateFromTDTest) {
  TaskDescriptor td;
  td.set_uid(1234);
  CHECK_EQ(GenerateDataObjectID(td), 175247688336);
  CHECK_EQ(GenerateDataObjectID(td.uid(), td.outputs_size()), 175247688336);
}

// Tests task ID parsing from string.
TEST_F(UtilsTest, TaskIDFromString) {
  FLAGS_v = 2;
  // Simple case
  string test1 = "1234567";
  // A value that overflows an int64_t (long), but fits within an uint64_t
  // (unsigned long).
  string test2 = "16733209960240500155";
  // Test both
  CHECK_EQ(TaskIDFromString(test1), 1234567);
  CHECK_EQ(TaskIDFromString(test2), 16733209960240500155U);
}



}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
