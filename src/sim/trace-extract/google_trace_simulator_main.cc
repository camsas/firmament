// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "base/common.h"
#include "sim/trace-extract/google_trace_extractor.h"

using namespace firmament;  // NOLINT

DEFINE_string(trace_path, "", "Path where the trace files are.");

int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);

  // command line argument sanity checking
  if (FLAGS_trace_path.empty()) {
	LOG(FATAL) << "Please specify a path to the Google trace!";
  }
  sim::GoogleTraceExtractor gte(FLAGS_trace_path);

  gte.Run();
}
