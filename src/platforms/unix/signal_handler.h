// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
