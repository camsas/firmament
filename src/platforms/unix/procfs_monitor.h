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

// Simple wrapper to poll information from ProcFS.

#ifndef FIRMAMENT_PLATFORMS_UNIX_PROCFS_MONITOR_H
#define FIRMAMENT_PLATFORMS_UNIX_PROCFS_MONITOR_H

#include "platforms/unix/common.h"

#include <stdio.h>

#include <string>
#include <vector>

#include <boost/thread/condition.hpp>

namespace firmament {
namespace platform_unix {

struct ProcessStatistics {
  uint64_t pid;
  char comm[PATH_MAX];
  char state;

  uint64_t ppid;
  uint64_t pgid;
  uint64_t sid;
  uint64_t tty_nr;
  uint64_t tpgid;

  uint64_t flags;
  uint64_t minflt;
  uint64_t cminflt;
  uint64_t majflt;
  uint64_t cmajflt;
  uint64_t utime;
  uint64_t stime;

  uint64_t cutime;
  uint64_t cstime;
  uint64_t priority;
  uint64_t nice;
  uint64_t num_threads;

  uint64_t starttime;

  uint64_t vsize;
  uint64_t rss;
  uint64_t rsslim;
  uint64_t startcode;
  uint64_t endcode;
  uint64_t startstack;
  uint64_t esp;
  uint64_t eip;

  uint64_t pending;
  uint64_t blocked;
  uint64_t sigign;
  uint64_t sigcatch;
  uint64_t wchan;
  uint64_t zero1;
  uint64_t zero2;
  uint64_t exit_signal;
  uint64_t cpu;
  uint64_t rt_priority;
  uint64_t policy;

  // scheduler statistics
  uint64_t sched_run_ticks;
  uint64_t sched_wait_runnable_ticks;
  uint64_t sched_run_timeslices;
};

struct SystemStatistics {
  uint64_t total_memory_;
  uint64_t free_memory_;
};

class ProcFSMonitor {
 public:
  typedef ProcessStatistics ProcessStatistics_t;
  typedef SystemStatistics SystemStatistics_t;
  explicit ProcFSMonitor(uint64_t polling_frequency);
  const ProcessStatistics_t* ProcessInformation(pid_t pid,
      ProcessStatistics_t* stats);
  void Run();
  void RunForPID(pid_t pid);
  void Stop();
  const SystemStatistics_t SystemInformation();

  inline uint64_t ticks_per_sec() { return ticks_per_sec_; }
  inline uint32_t page_size() { return page_size_; }

 protected:
  boost::condition stop_;
  boost::mutex stop_mut_;

 private:
  // The polling frequency, specified in microseconds
  uint64_t polling_frequency_;
  uint64_t ticks_per_sec_;
  uint32_t page_size_;
  void AddStatsForPID(pid_t pid, ProcessStatistics_t* stats);
  void AddSchedStatsForPID(pid_t pid, ProcessStatistics_t* stats);
  void AggregateStatsForPIDTree(pid_t pid, bool root,
                                ProcessStatistics_t* stats);
  // Find a line matching the regular expression provided
  vector<string>* FindMatchingLine(const string& regexp, const string& data);
  void GetStatsForPID(pid_t pid, ProcessStatistics_t* stats);
  inline void readone(FILE* input, uint64_t *x) {
    CHECK_EQ(fscanf(input, "%ju ", x), 1);
  }
  inline void readunsigned(FILE* input, uint64_t *x) {
    CHECK_EQ(fscanf(input, "%ju ", x), 1);
  }
  inline void readstr(FILE* input, char *x) {
    CHECK_EQ(fscanf(input, "%s ", x), 1);
  }
  inline void readchar(FILE* input, char *x) {
    CHECK_EQ(fscanf(input, "%c ", x), 1);
  }
  inline void skip(FILE* input, int64_t num) {  fseek(input, num, SEEK_CUR); }
};

}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_PROCFS_MONITOR_H
