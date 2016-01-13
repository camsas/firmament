// The Firmament project
// Copyright (c) 2016-2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "misc/real_time.h"

namespace firmament {

RealTime::~RealTime() {
}

uint64_t RealTime::GetCurrentTimestamp() {
  struct timeval ts;
  gettimeofday(&ts, NULL);
  return ts.tv_sec * 1000000 + ts.tv_usec;
}

void RealTime::UpdateCurrentTimestamp(uint64_t timestmap) {
  // NO-OP. We can't change the timestamp returned by a real timer.
}

} // namespace firmament
