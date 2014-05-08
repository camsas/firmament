// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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

void ProcFSMonitor::ProcessPIDSchedStat(pid_t pid, ProcessStatistics_t* stats) {
  // /proc/[pid]/schedstat parsing
  string filename = "/proc/" + to_string(pid) + "/schedstat";
  FILE* input = fopen(filename.c_str(), "r");
  CHECK_NOTNULL(input);
  readunsigned(input, &stats->sched_run_ticks);
  readunsigned(input, &stats->sched_wait_runnable_ticks);
  readunsigned(input, &stats->sched_run_timeslices);
  fclose(input);
}

void ProcFSMonitor::ProcessPIDStat(pid_t pid, ProcessStatistics_t* stats) {
  // /proc/[pid]/stat parsing
  string filename = "/proc/" + to_string(pid) + "/stat";
  FILE* input = fopen(filename.c_str(), "r");
  CHECK_NOTNULL(input);
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
  fclose(input);
}

const ProcFSMonitor::ProcessStatistics_t* ProcFSMonitor::ProcessInformation(
    pid_t pid, ProcessStatistics_t* stats) {
  if (stats == NULL)
    stats = new ProcessStatistics_t;
  // Grab information from /proc/[pid]/stat
  ProcessPIDStat(pid, stats);
  // Grab information from /proc/[pid]/schedstat
  ProcessPIDSchedStat(pid, stats);
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
