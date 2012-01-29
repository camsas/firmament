// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Data object class unit tests.

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/data_object.h"

using namespace firmament;

namespace {

class DataObjectTest : public ::testing::Test {
 protected:
  DataObjectTest() {
    // Set-up work
  }

  virtual ~DataObjectTest() {
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

  // Objects declared here can be used by all tests in the test case for Worker.
};

// Test that verifies all members are initialized correctly when setting up a
// valid data object.
TEST_F(DataObjectTest, CreateDOTest) {
  uint64_t len = 4096;
  void *buf = new char[len];
  DataObject test_do(buf, len);

  // Check all members are set to what we expect.
  EXPECT_EQ(test_do.buffer(), buf);
  EXPECT_EQ(test_do.size(), len);
  EXPECT_TRUE(test_do.valid());

  // Clean up buffer.
  delete static_cast<char*>(buf);
}

// Test that verifies that setting up an invalid data object fails.
TEST_F(DataObjectTest, CreateInvalidDOTest) {
  uint64_t len = 4096;
  void *buf = NULL;
  DataObject invalid_do1(buf, len);
  EXPECT_FALSE(invalid_do1.valid());

  buf = new char[len];
  DataObject invalid_do2(buf, 0);
  EXPECT_FALSE(invalid_do2.valid());

  // Clean up buffer.
  delete static_cast<char*>(buf);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
