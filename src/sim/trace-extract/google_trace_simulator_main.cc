// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "base/common.h"
#include "sim/trace-extract/google_trace_simulator.h"

using namespace firmament;  // NOLINT

DEFINE_string(trace_path, "", "Path where the trace files are.");

int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);

  //HeapProfilerStart("ts");
  sim::GoogleTraceSimulator gts(FLAGS_trace_path);
  //HeapProfilerStop();

  gts.Run();
}
