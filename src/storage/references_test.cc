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

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "storage/concrete_reference.h"
#include "storage/future_reference.h"
#include "storage/error_reference.h"
#include "storage/value_reference.h"

namespace firmament {

class ReferencesTest : public ::testing::Test {
 protected:
  ReferencesTest() {
    // Allocate a buffer for the test name
    test_name_ = (uint8_t*)malloc(DIOS_NAME_BYTES+1);  // NOLINT
    uint64_t* u64_test_name = (uint64_t*)test_name_;  // NOLINT
    for (uint32_t i = 0; i < DIOS_NAME_QWORDS; ++i)
      u64_test_name[i] = 0xFEEDCAFEDEADBEEF;
  }

  virtual ~ReferencesTest() {
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

// Test that verifies all members are initialized correctly when setting up a
// valid Future reference.
TEST_F(ReferencesTest, CreateFutureTest) {
  DataObjectID_t ref_id(test_name_);
  FutureReference f(ref_id);

  // Check all members are set to what we expect.
  EXPECT_EQ(f.id(), ref_id);
  EXPECT_FALSE(f.Consumable());
}

// Test that verifies all members are initialized correctly when setting up a
// valid Error reference.
TEST_F(ReferencesTest, CreateErrorTest) {
  DataObjectID_t ref_id(test_name_);
  const string reason = "boom";
  const string details = "kaboom";
  ErrorReference r(ref_id, reason, details);

  // Check all members are set to what we expect.
  EXPECT_EQ(r.id(), ref_id);
  EXPECT_TRUE(r.Consumable());
  EXPECT_EQ(r.reason(), reason);
  EXPECT_EQ(r.details(), details);
}

// Test that verifies all members are initialized correctly when setting up a
// valid Error reference.
TEST_F(ReferencesTest, CreateConcreteTest) {
  FLAGS_v = 2;
  DataObjectID_t ref_id(test_name_);
  string location;
  uint64_t size = 1000;
  location = "here";
  ConcreteReference r(ref_id, size, location);

  // Check all members are set to what we expect.
  EXPECT_EQ(r.id(), ref_id);
  EXPECT_TRUE(r.Consumable());
  EXPECT_EQ(r.size(), size);
  EXPECT_EQ(r.location(), location);
}

// Test that verifies all members are initialized correctly when setting up a
// valid Error reference.
TEST_F(ReferencesTest, CreateValueTest) {
  DataObjectID_t ref_id(test_name_);
  const string value = "test";
  ValueReference r(ref_id, value);

  // Check all members are set to what we expect.
  EXPECT_EQ(r.id(), ref_id);
  EXPECT_TRUE(r.Consumable());
  EXPECT_EQ(r.value(), value);
}

// This tests creates one reference of each type and invokes its
// ValidateInternalDescriptor method, which checks the integrity of the mapping
// between the reference object and its serializable protobuf descriptor.
TEST_F(ReferencesTest, ValidateInternalDescriptors) {
  DataObjectID_t ref_id(test_name_);
  FutureReference f(ref_id);
  ErrorReference e(ref_id, "foo", "bar");
  ConcreteReference c1(ref_id);
  string location;
  location = "whee";
  ConcreteReference c2(ref_id, 100, location);
  ValueReference v(ref_id, "42");

  f.ValidateInternalDescriptor();
  e.ValidateInternalDescriptor();
  c1.ValidateInternalDescriptor();
  c2.ValidateInternalDescriptor();
  v.ValidateInternalDescriptor();
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
