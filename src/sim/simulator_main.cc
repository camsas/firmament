// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "base/common.h"
#include "sim/simulator.h"

using namespace firmament;  // NOLINT

int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);
  //HeapProfilerStart("ts");
  sim::Simulator simulator;
  //HeapProfilerStop();
  simulator.Run();
}
