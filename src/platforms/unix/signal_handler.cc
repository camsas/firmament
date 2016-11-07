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

// UNIX/POSIX signal handler implementation.

#include "platforms/unix/signal_handler.h"

namespace firmament {
namespace platform_unix {

SignalHandler::SignalHandler() {
  VLOG(1) << "Signal handler set up, ready to add signals.";
}

void SignalHandler::ConfigureSignal(int signum,
                                    void (*legacy_fptr)(int),  // NOLINT
                                    void*) {
  signal(signum, legacy_fptr);
}

}  // namespace platform_unix
}  // namespace firmament
