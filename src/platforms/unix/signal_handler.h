// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Signal handling on UNIX/POSIX platforms.
//
#ifndef FIRMAMENT_PLATFORMS_UNIX_SIGNAL_HANDLER_H
#define FIRMAMENT_PLATFORMS_UNIX_SIGNAL_HANDLER_H

// If available, we use Boost ASIO's signal handling; however, this only became
// available in 1.48.0, so we fall back onto csignal if we're running with an
// older version of Boost.
#if (BOOST_VERSION < 104700)
#include <csignal>
#endif

#include <boost/function.hpp>

#include "platforms/unix/common.h"

namespace firmament {
namespace platform_unix {

class SignalHandler {
 public:
  SignalHandler();
#if (BOOST_VERSION >= 104700)
  void ConfigureSignal(int signum, void (*legacy_fptr)(int), void* object);
#else
  void ConfigureSignal(int signum, void (*legacy_fptr)(int), void*);
#endif
 private:
#if (BOOST_VERSION >= 104700)
  // Signal set from Boost ASIO; used to catch and handle UNIX signals
  boost::asio::signal_set signals_;
  boost::asio::io_service signal_io_service_;
#endif
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_SIGNAL_HANDLER_H
