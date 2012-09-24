// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX/POSIX signal handler implementation.

#include "platforms/unix/signal_handler.h"

namespace firmament {
namespace platform_unix {

SignalHandler::SignalHandler()
#if (BOOST_VERSION >= 104700)
  : signal_set_(signal_set(signal_io_service_)) {
#else
{
#endif
  VLOG(1) << "Signal handler set up, ready to add signals.";
}

#if (BOOST_VERSION >= 104700)
void SignalHandler::ConfigureSignal(int signum, void (*legacy_fptr)(int),
                                    void* object) {
  // Convert function pointer into callback
  // TODO(malte): this is largely untested, and may not even compile. Test
  // with Boost >= 1.48.0.
  signals_.add(signum);
  signals_.async_wait(boost::bind(legacy_fptr, object, signum));
}
#else
void SignalHandler::ConfigureSignal(int signum, void (*legacy_fptr)(int),
                                    void*) {
  signal(signum, legacy_fptr);
}
#endif

}  // namespace platform_unix
}  // namespace firmament
