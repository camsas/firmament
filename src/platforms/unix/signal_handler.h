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

// Signal handling on UNIX/POSIX platforms.
//
#ifndef FIRMAMENT_PLATFORMS_UNIX_SIGNAL_HANDLER_H
#define FIRMAMENT_PLATFORMS_UNIX_SIGNAL_HANDLER_H

#include <csignal>

#include "platforms/unix/common.h"

namespace firmament {
namespace platform_unix {

class SignalHandler {
 public:
  SignalHandler();
  void ConfigureSignal(int signum,
                       void (*legacy_fptr)(int),  // NOLINT
                       void*);
 private:
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_SIGNAL_HANDLER_H
