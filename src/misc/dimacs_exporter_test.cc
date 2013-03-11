// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Tests for DIMACS exporter for CS2 solver.

#include <gtest/gtest.h>

#include <vector>

#include "base/common.h"
#include "misc/dimacs_exporter.h"
#include "misc/utils.h"

namespace firmament {
namespace misc {

// The fixture for testing the DIMACSExporter container class.
class DIMACSExporterTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  DIMACSExporterTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
  }

  virtual ~DIMACSExporterTest() {
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

  // Objects declared here can be used by all tests.
};

// Tests allocation of an empty envelope and puts an integer into it (using
// memcopy internally).
TEST_F(DIMACSExporterTest, SimpleGraphOutput) {
  FlowGraph g();
  g.AddJobNodes();
  exp.Export(g);
  exp.Flush("test.dm");
}

}  // namespace misc
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
