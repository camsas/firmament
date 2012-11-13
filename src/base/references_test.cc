// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Data object class unit tests.

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/concrete_reference.h"
#include "base/future_reference.h"
#include "base/error_reference.h"
#include "base/value_reference.h"

namespace firmament {

class ReferencesTest : public ::testing::Test {
 protected:
  ReferencesTest() {
    // Set-up work
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
};

// Test that verifies all members are initialized correctly when setting up a
// valid Future reference.
TEST_F(ReferencesTest, CreateFutureTest) {
  DataObjectID_t ref_id = 1234;
  FutureReference f(ref_id);

  // Check all members are set to what we expect.
  EXPECT_EQ(f.id(), ref_id);
  EXPECT_FALSE(f.Consumable());
}

// Test that verifies all members are initialized correctly when setting up a
// valid Error reference.
TEST_F(ReferencesTest, CreateErrorTest) {
  DataObjectID_t ref_id = 1234;
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
  DataObjectID_t ref_id = 1234;
  set<string> locations;
  uint64_t size = 1000;
  locations.insert("here");
  locations.insert("there");
  ConcreteReference r(ref_id, size, locations);

  // Check all members are set to what we expect.
  EXPECT_EQ(r.id(), ref_id);
  EXPECT_TRUE(r.Consumable());
  EXPECT_EQ(r.size(), size);
  EXPECT_EQ(r.locations(), locations);
  // Check that adding a location works as expected
  r.AddLocation("elsewhere");
  EXPECT_NE(r.locations(), locations);
}

// Test that verifies all members are initialized correctly when setting up a
// valid Error reference.
TEST_F(ReferencesTest, CreateValueTest) {
  DataObjectID_t ref_id = 1234;
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
  DataObjectID_t ref_id = 1234;
  FutureReference f(ref_id);
  ErrorReference e(ref_id, "foo", "bar");
  ConcreteReference c1(ref_id);
  set<string> locations;
  locations.insert("whee");
  ConcreteReference c2(ref_id, 100, locations);
  ValueReference v(ref_id, "42");

  f.ValidateInternalDescriptor();
  e.ValidateInternalDescriptor();
  c1.ValidateInternalDescriptor();
  c2.ValidateInternalDescriptor();
  v.ValidateInternalDescriptor();
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
