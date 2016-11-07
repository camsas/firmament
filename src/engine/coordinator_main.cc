/*
 * Firmament
 * Copyright (c) Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
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

// Initialization code for coordinator binary. This delegates to the
// Coordinator class almost immediately after launching.

#include "base/common.h"
#include "engine/coordinator.h"
#include "platforms/common.h"

using namespace firmament;  // NOLINT

// The main method: initializes, parses arguments and sets up a worker for
// the platform we're running on.
int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);

  LOG(INFO) << "Firmament coordinator starting ...";
  boost::shared_ptr<Coordinator> coordinator(new Coordinator());

  coordinator->Run();

  LOG(INFO) << "Coordinator's Run() method returned; terminating...";
}
