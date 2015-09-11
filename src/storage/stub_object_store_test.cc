// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// In-memory stub object store unit tests.

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "engine/stub_object_store.h"
#include "misc/map-util.h"
#include "misc/utils.h"

namespace firmament {
namespace store {

using store::StubObjectStore;

// The fixture for testing class StubObjectStore.
class StubObjectStoreTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  StubObjectStoreTest() {
    // You can do set-up work for each test here.
  }

  virtual ~StubObjectStoreTest() {
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
    ost_.Flush();
  }

  // Objects declared here can be used by all tests in the test case for
  // StubObjectStore.
  StubObjectStore ost_;
};

// Tests that we can put objects into the store
TEST_F(StubObjectStoreTest, PutObjectTest) {
  FLAGS_v = 2;
  uint64_t o = 42;
  DataObjectID_t id = DataObjectIDFromString("1234");
  ost_.PutObject(id, &o, sizeof(uint64_t));
  CHECK_NOTNULL(FindOrNull(ost_.stub_object_map_, id));
  uint64_t* stored_ptr = reinterpret_cast<uint64_t*>(
      *FindOrNull(ost_.stub_object_map_, id));
  CHECK_NOTNULL(stored_ptr);
  CHECK_EQ(*stored_ptr, o);
}

// Tests that we fail when trying to retrieve an object from that store which
// does not exist.
TEST_F(StubObjectStoreTest, PutGetObjectTest) {
  FLAGS_v = 2;
  uint64_t o = 42;
  DataObjectID_t id = DataObjectIDFromString("1234");
  ost_.PutObject(id, &o, sizeof(uint64_t));
  void* ptr = ost_.GetObject(id);
  CHECK_NOTNULL(ptr);
  CHECK_EQ(*reinterpret_cast<uint64_t*>(ptr), o);
}

// Tests that we fail when trying to retrieve an object from that store which
// does not exist.
TEST_F(StubObjectStoreTest, GetNonExistentObjectTest) {
  DataObjectID_t non_existent_id = DataObjectIDFromString("9999");
  void* ptr = ost_.GetObject(non_existent_id);
  CHECK_EQ(ptr, static_cast<void*>(NULL));
}

}  // namespace store
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
