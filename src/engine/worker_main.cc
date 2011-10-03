// TODO: header

#include <stdint.h>
#include <iostream>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "engine/worker.h"

using namespace firmament;

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
 
  LOG(INFO) << "Hello from Firmament worker (LOG)!";
  Worker worker;
  worker.Test();
  LOG(INFO) << "set up worker object";
}
