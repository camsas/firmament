// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
