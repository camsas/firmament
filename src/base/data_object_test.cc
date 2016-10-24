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

// Data object class unit tests.

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/data_object.h"

namespace firmament {

using firmament::DataObject;

class DataObjectTest : public ::testing::Test {
 protected:
  DataObjectTest() {
    // Allocate a buffer for the test name
    test_name_ = (uint8_t*)malloc(DIOS_NAME_BYTES+1);  // NOLINT
    uint64_t* u64_test_name = (uint64_t*)test_name_;  // NOLINT
    for (uint32_t i = 0; i < DIOS_NAME_QWORDS; ++i)
      (u64_test_name)[i] = 0xFEEDCAFEDEADBEEF;
    // N.B.: This addition of a null char is necessary for the tests taking
    // strings to work.
    ((uint8_t*)test_name_)[DIOS_NAME_BYTES] = '\0';  // NOLINT
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
  const uint8_t* test_name_;
};

// Create a DO from a string name (e.g. pulled from a protobuf bytes field).
TEST_F(DataObjectTest, CreateDOFromString) {
  string name(reinterpret_cast<const char*>(test_name_));
  DataObject test_do(name, false);

  // Check all members are set to what we expect.
  EXPECT_EQ(memcmp(test_do.name_str()->data(),
                   name.data(), DIOS_NAME_BYTES), 0);
}

// Create a DO from a hex-encoded string name.
TEST_F(DataObjectTest, CreateDOFromHexString) {
  string name(
      "feedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeef");
  DataObject test_do(name, true);

  // Check all members are set to what we expect.
  EXPECT_EQ(test_do.name_printable_string(), name);
}


// Create a DO and comare it to itself.
TEST_F(DataObjectTest, SelfSimilarity) {
  string name(reinterpret_cast<const char*>(test_name_));
  DataObject test_do(name, false);

  // Check that equality comparison with itself return true.
  EXPECT_EQ(test_do, test_do);
}

// Create a DO from a dios_name_t.
TEST_F(DataObjectTest, CreateDOFromDIOSName) {
  dios_name_t name;
  for (uint32_t i = 0; i < 4; ++i)
    name.value[i] = 0xFEEDCAFEDEADBEEF;
  DataObject test_do(name);

  // Check all members are set to what we expect.
  EXPECT_EQ(memcmp(test_do.name(), test_name_, DIOS_NAME_BYTES), 0);
}

// Create a DO from an existing DO.
TEST_F(DataObjectTest, CreateDOFromBytes) {
  DataObject test_do(test_name_);

  // Check all members are set to what we expect.
  EXPECT_EQ(memcmp(test_do.name(), test_name_, DIOS_NAME_BYTES), 0);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
