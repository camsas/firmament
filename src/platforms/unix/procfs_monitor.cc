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

#include "platforms/unix/procfs_monitor.h"

#include <stdio.h>

#include <string>
#include <vector>

#include <boost/regex.hpp>

namespace firmament {
namespace platform_unix {

ProcFSMonitor::ProcFSMonitor(uint64_t polling_frequency)
  : polling_frequency_(polling_frequency) {
  ticks_per_sec_ = sysconf(_SC_CLK_TCK);
  page_size_ = getpagesize();
}

void ProcFSMonitor::AddSchedStatsForPID(pid_t pid, ProcessStatistics_t* stats) {
  // /proc/[pid]/schedstat parsing
  string filename = "/proc/" + to_string(pid) + "/schedstat";
  FILE* input = fopen(filename.c_str(), "r");
  // The procfs file may no longer be there if the process has finished
  if (!input)
    return;
  uint64_t tmp;
  readunsigned(input, &tmp);
  stats->sched_run_ticks += tmp;
  readunsigned(input, &tmp);
  stats->sched_wait_runnable_ticks += tmp;
  readunsigned(input, &tmp);
  stats->sched_run_timeslices += tmp;
  CHECK_EQ(fclose(input), 0);
}

void ProcFSMonitor::AddStatsForPID(pid_t pid, ProcessStatistics_t* stats) {
  // /proc/[pid]/stat parsing
  string filename = "/proc/" + to_string(pid) + "/stat";
  FILE* input = fopen(filename.c_str(), "r");
  // The procfs file may no longer be there if the process has finished
  if (!input)
    return;
  uint64_t tmp;
  char tmp_str[PATH_MAX];
  readone(input, &tmp);  // skip
  readstr(input, tmp_str);  // skip
  readchar(input, &tmp_str[0]);  // skip
  skip(input, sizeof(uint64_t) * 6);
  readone(input, &tmp);
  stats->minflt += tmp;
  readone(input, &tmp);
  stats->cminflt += tmp;
  readone(input, &tmp);
  stats->majflt += tmp;
  readone(input, &tmp);
  stats->cmajflt += tmp;
  readone(input, &tmp);
  stats->utime += tmp;
  readone(input, &tmp);
  stats->stime += tmp;
  readone(input, &tmp);
  stats->cutime += tmp;
  readone(input, &tmp);
  stats->cstime += tmp;
  readone(input, &tmp);
  stats->num_threads += tmp;
  readone(input, &tmp);  // skip
  readunsigned(input, &tmp);  // skip
  readone(input, &tmp);
  stats->vsize += tmp;
  readone(input, &tmp);
  stats->rss += tmp;
  readone(input, &tmp);
  stats->rsslim += tmp;
  skip(input, sizeof(uint64_t) * 16);  // skip remaining 16 fields
  CHECK_EQ(fclose(input), 0);
}

void ProcFSMonitor::AggregateStatsForPIDTree(
    pid_t pid,
    bool root,
    ProcessStatistics_t* stats) {
  VLOG(1) << "Adding stats for PID " << pid;
  // Grab information from /proc/[pid]/stat
  if (root)
    GetStatsForPID(pid, stats);
  else
    AddStatsForPID(pid, stats);
  // Grab information from /proc/[pid]/schedstat
  AddSchedStatsForPID(pid, stats);
  // Now also aggregate from children
  string filename = "/proc/" + to_string(pid) + "/task/" + to_string(pid)
                    + "/children";
  FILE* chld_input = fopen(filename.c_str(), "r");
  if (!chld_input)
    return;
  vector<uint64_t> children;
  uint64_t tmp;
  while (fscanf(chld_input, "%ju ", &tmp) > 0) {
    VLOG(1) << "Found child " << tmp << " for " << pid;
    children.push_back(tmp);
  }
  CHECK_EQ(fclose(chld_input), 0);
  for (uint64_t i = 0; i < children.size(); i++)
    AggregateStatsForPIDTree(children[i], false, stats);
}

void ProcFSMonitor::GetStatsForPID(pid_t pid, ProcessStatistics_t* stats) {
  // /proc/[pid]/stat parsing
  string filename = "/proc/" + to_string(pid) + "/stat";
  FILE* input = fopen(filename.c_str(), "r");
  // The procfs file may no longer be there if the process has finished
  if (!input)
    return;
  readone(input, &stats->pid);
  readstr(input, stats->comm);
  readchar(input, &stats->state);
  readone(input, &stats->ppid);
  readone(input, &stats->pgid);
  readone(input, &stats->sid);
  readone(input, &stats->tty_nr);
  readone(input, &stats->tpgid);
  readone(input, &stats->flags);
  readone(input, &stats->minflt);
  readone(input, &stats->cminflt);
  readone(input, &stats->majflt);
  readone(input, &stats->cmajflt);
  readone(input, &stats->utime);
  readone(input, &stats->stime);
  readone(input, &stats->cutime);
  readone(input, &stats->cstime);
  readone(input, &stats->priority);
  readone(input, &stats->nice);
  readone(input, &stats->num_threads);
  readone(input, &stats->zero1);  // skip unmaintained itrealvalue field
  readunsigned(input, &stats->starttime);
  readone(input, &stats->vsize);
  readone(input, &stats->rss);
  readone(input, &stats->rsslim);
  readone(input, &stats->startcode);
  readone(input, &stats->endcode);
  readone(input, &stats->startstack);
  readone(input, &stats->esp);
  readone(input, &stats->eip);
  readone(input, &stats->pending);
  readone(input, &stats->blocked);
  readone(input, &stats->sigign);
  readone(input, &stats->sigcatch);
  readone(input, &stats->wchan);
  readone(input, &stats->zero1);  // skip unmaintained nswap field
  readone(input, &stats->zero2);  // skip unmaintained cnswap field
  readone(input, &stats->exit_signal);
  readone(input, &stats->cpu);
  readone(input, &stats->rt_priority);
  readone(input, &stats->policy);
  CHECK_EQ(fclose(input), 0);
}

vector<string>* ProcFSMonitor::FindMatchingLine(
    const string& regexp, const string& data) {
  boost::regex e(regexp);
  boost::smatch m;
  // Magic
  if (boost::regex_match(data, m, e, boost::match_extra)) {
    // Found a line
    vector<string>* matches = new vector<string>(m.size());
    for (boost::smatch::const_iterator m_iter = m.begin();
         m_iter != m.end();
           ++m_iter)
        matches->push_back(*m_iter);
    return matches;
  } else {
    // No match
    return NULL;
  }
}

const ProcFSMonitor::ProcessStatistics_t* ProcFSMonitor::ProcessInformation(
    pid_t pid, ProcessStatistics_t* stats) {
  if (stats == NULL) {
    stats = new ProcessStatistics_t;
    bzero(stats, sizeof(ProcessStatistics_t));
  }
  // Grab information recursively for PID and its children
  AggregateStatsForPIDTree(pid, true, stats);
  return stats;
}

void ProcFSMonitor::Run() {
  // Keep going until we're told to stop
  boost::unique_lock<boost::mutex> lock(stop_mut_);
  while (!stop_.timed_wait(
      lock, boost::posix_time::microseconds(polling_frequency_))) {
    VLOG(2) << "ProcFSMonitor polling...";
  }
  // Return -- this typically means the monitoring thread will exit.
}

void ProcFSMonitor::RunForPID(pid_t pid) {
  ProcessStatistics_t stats;
  // Keep going until we're told to stop
  boost::unique_lock<boost::mutex> lock(stop_mut_);
  while (!stop_.timed_wait(
      lock, boost::posix_time::microseconds(polling_frequency_))) {
    VLOG(2) << "ProcFSMonitor polling...";
    ProcessInformation(pid, &stats);
  }
  // Return -- this typically means the monitoring thread will exit.
}


void ProcFSMonitor::Stop() {
  stop_.notify_all();
}

}  // namespace platform_unix
}  // namespace firmament
