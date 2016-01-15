// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#include "sim/google_trace_loader.h"

#include <SpookyV2.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <cstdio>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "misc/string_utils.h"
#include "misc/utils.h"

DEFINE_double(events_fraction, 1.0, "Fraction of events to retain.");
DEFINE_int32(num_files_to_process, 500, "Number of files to process.");
DEFINE_string(trace_path, "", "Path where the trace files are.");

DECLARE_uint64(runtime);

static bool ValidateTracePath(const char* flagname, const string& trace_path) {
  if (trace_path.empty()) {
    LOG(ERROR) << "Please specify a path to the Google trace!";
    return false;
  }
  return true;
}

static const bool trace_path_validator =
  google::RegisterFlagValidator(&FLAGS_trace_path, &ValidateTracePath);

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

namespace firmament {
namespace sim {

GoogleTraceLoader::GoogleTraceLoader(EventManager* event_manager)
  : TraceLoader(event_manager),
    current_task_events_file_id_(0),
    task_events_file_(NULL) {
}

GoogleTraceLoader::~GoogleTraceLoader() {
  if (task_events_file_) {
    fclose(task_events_file_);
  }
}

void GoogleTraceLoader::LoadJobsNumTasks(
    unordered_map<uint64_t, uint64_t>* job_num_tasks) {
  char line[200];
  vector<string> cols;
  FILE* jobs_tasks_file = NULL;
  string jobs_tasks_file_name = FLAGS_trace_path +
    "/jobs_num_tasks/jobs_num_tasks.csv";
  if ((jobs_tasks_file = fopen(jobs_tasks_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open jobs num tasks file.";
  }
  int64_t num_line = 1;
  while (!feof(jobs_tasks_file)) {
    if (fscanf(jobs_tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(","), token_compress_off);
      if (cols.size() != 2) {
        LOG(ERROR) << "Unexpected structure of jobs num tasks row on line: "
                   << num_line;
      } else {
        uint64_t job_id = lexical_cast<uint64_t>(cols[0]);
        uint64_t num_tasks = lexical_cast<uint64_t>(cols[1]);
        CHECK(InsertIfNotPresent(job_num_tasks, job_id, num_tasks));
      }
    }
    num_line++;
  }
  fclose(jobs_tasks_file);
}

void GoogleTraceLoader::LoadMachineEvents(
    multimap<uint64_t, EventDescriptor>* machine_events) {
  char line[200];
  vector<string> cols;
  FILE* machines_file;
  string machines_file_name = FLAGS_trace_path +
    "/machine_events/part-00000-of-00001.csv";
  if ((machines_file = fopen(machines_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace for reading machine events.";
  }

  int64_t num_line = 1;
  while (!feof(machines_file)) {
    if (fscanf(machines_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(","), token_compress_off);
      if (cols.size() != 6) {
        LOG(ERROR) << "Unexpected structure of machine events on line "
                   << num_line << ": found " << cols.size() << " columns.";
      } else {
        uint64_t timestamp = lexical_cast<uint64_t>(cols[0]);
        if (timestamp > FLAGS_runtime) {
          // only load the events that we need
          break;
        }

        // schema: (timestamp, machine_id, event_type, platform, CPUs, Memory)
        uint64_t machine_id = lexical_cast<uint64_t>(cols[1]);
        // Sub-sample the trace if we only retain < 100% of machines.
        if (SpookyHash::Hash64(&machine_id, sizeof(machine_id), kSeed) >
            MaxEventHashToRetain()) {
          // skip event
          continue;
        }

        EventDescriptor event_desc;
        event_desc.set_machine_id(lexical_cast<uint64_t>(cols[1]));
        event_desc.set_type(TranslateMachineEvent(
            lexical_cast<int32_t>(cols[2])));
        if (event_desc.type() == EventDescriptor::REMOVE_MACHINE ||
            event_desc.type() == EventDescriptor::ADD_MACHINE) {
          machine_events->insert(
              pair<uint64_t, EventDescriptor>(timestamp, event_desc));
        } else {
          // TODO(ionel): Handle machine update events.
        }
      }
    }
    num_line++;
  }
  fclose(machines_file);
}

void GoogleTraceLoader::LoadTaskEvents(
    uint64_t events_up_to_time,
    unordered_map<uint64_t, uint64_t>* job_num_tasks) {
  char line[200];
  vector<string> vals;
  while (true) {
    // Check if we're already reading from a file.
    if (!task_events_file_) {
      if (current_task_events_file_id_ < FLAGS_num_files_to_process) {
        // We still have files to open.
        string fname;
        spf(&fname, "%s/task_events/part-%05d-of-00500.csv",
            FLAGS_trace_path.c_str(), current_task_events_file_id_);
        if ((task_events_file_ = fopen(fname.c_str(), "r")) == NULL) {
          LOG(FATAL) << "Failed to open trace for reading of task events.";
        }
      } else {
        // There are no task events left to load.
        return;
      }
    }
    while (!feof(task_events_file_)) {
      if (fscanf(task_events_file_, "%[^\n]%*[\n]", &line[0]) > 0) {
        boost::split(vals, line, is_any_of(","), token_compress_off);
        if (vals.size() != 13) {
          LOG(ERROR) << "Unexpected structure of task event row: found "
                     << vals.size() << " columns.";
        } else {
          TraceTaskIdentifier task_id;
          uint64_t task_event_time = lexical_cast<uint64_t>(vals[0]);
          task_id.job_id = lexical_cast<uint64_t>(vals[2]);
          task_id.task_index = lexical_cast<uint64_t>(vals[3]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);

          // Sub-sample the trace if we only retain < 100% of tasks.
          if (SpookyHash::Hash64(&task_id, sizeof(task_id), kSeed) >
              MaxEventHashToRetain()) {
            if (filtered_tasks_.find(task_id) == filtered_tasks_.end()) {
              // The task has been filtered. Decrease the number of tasks the
              // job has.
              uint64_t* num_tasks = FindOrNull(*job_num_tasks, task_id.job_id);
              CHECK_NOTNULL(num_tasks);
              (*num_tasks)--;
              filtered_tasks_.insert(task_id);
            }
            // skip event
            continue;
          }

          if (event_type == SUBMIT_EVENT) {
            EventDescriptor event_desc;
            event_desc.set_type(EventDescriptor::TASK_SUBMIT);
            event_desc.set_job_id(task_id.job_id);
            event_desc.set_task_index(task_id.task_index);
            event_manager_->AddEvent(task_event_time, event_desc);
          } else {
            // Skip this event and read next event from the trace.
            continue;
          }
          if (task_event_time > events_up_to_time) {
            // We've loaded all the events up to the given time.
            // NOTE: we also loaded the current task event.
            return;
          }
        }
      }
    }
    fclose(task_events_file_);
    current_task_events_file_id_++;
    // We set the file to NULL to indicate that we should open the next file.
    task_events_file_ = NULL;
  }
}

void GoogleTraceLoader::LoadTaskUtilizationStats(
    unordered_map<TraceTaskIdentifier, TraceTaskStats,
      TraceTaskIdentifierHasher>* task_id_to_stats) {
  char line[1000];
  vector<string> cols;
  FILE* usage_file = NULL;
  string usage_file_name = FLAGS_trace_path +
    "/task_usage_stat/task_usage_stat.csv";
  if ((usage_file = fopen(usage_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace task runtime stats file.";
  }
  int64_t num_line = 1;
  while (!feof(usage_file)) {
    if (fscanf(usage_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(","), token_compress_off);
      if (cols.size() != 38) {
        LOG(WARNING) << "Malformed task usage, " << cols.size()
                     << " != 38 columns at line " << num_line;
      } else {
        TraceTaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);
        TraceTaskStats task_stats;
        task_stats.avg_mean_cpu_usage = lexical_cast<double>(cols[4]);
        task_stats.avg_canonical_mem_usage = lexical_cast<double>(cols[8]);
        task_stats.avg_assigned_mem_usage = lexical_cast<double>(cols[12]);
        task_stats.avg_unmapped_page_cache = lexical_cast<double>(cols[16]);
        task_stats.avg_total_page_cache = lexical_cast<double>(cols[20]);
        task_stats.avg_mean_disk_io_time = lexical_cast<double>(cols[24]);
        task_stats.avg_mean_local_disk_used = lexical_cast<double>(cols[28]);
        task_stats.avg_cpi = lexical_cast<double>(cols[32]);
        task_stats.avg_mai = lexical_cast<double>(cols[36]);

        if (!InsertIfNotPresent(task_id_to_stats, task_id, task_stats) &&
            VLOG_IS_ON(1)) {
          LOG(ERROR) << "LoadTaskUtilizationStats: There should not be more "
                     << "than an entry for job " << task_id.job_id
                     << ", task " << task_id.task_index;
        } else {
          VLOG(2) << "Loaded stats for "
                  << task_id.job_id << "/" << task_id.task_index;
        }

        // double min_mean_cpu_usage = lexical_cast<double>(cols[2]);
        // double max_mean_cpu_usage = lexical_cast<double>(cols[3]);
        // double sd_mean_cpu_usage = lexical_cast<double>(cols[5]);
        // double min_canonical_mem_usage = lexical_cast<double>(cols[6]);
        // double max_canonical_mem_usage = lexical_cast<double>(cols[7]);
        // double sd_canonical_mem_usage = lexical_cast<double>(cols[9]);
        // double min_assigned_mem_usage = lexical_cast<double>(cols[10]);
        // double max_assigned_mem_usage = lexical_cast<double>(cols[11]);
        // double sd_assigned_mem_usage = lexical_cast<double>(cols[13]);
        // double min_unmapped_page_cache = lexical_cast<double>(cols[14]);
        // double max_unmapped_page_cache = lexical_cast<double>(cols[15]);
        // double sd_unmapped_page_cache = lexical_cast<double>(cols[17]);
        // double min_total_page_cache = lexical_cast<double>(cols[18]);
        // double max_total_page_cache = lexical_cast<double>(cols[19]);
        // double sd_total_page_cache = lexical_cast<double>(cols[21]);
        // double min_mean_disk_io_time = lexical_cast<double>(cols[22]);
        // double max_mean_disk_io_time = lexical_cast<double>(cols[23]);
        // double sd_mean_disk_io_time = lexical_cast<double>(cols[25]);
        // double min_mean_local_disk_used = lexical_cast<double>(cols[26]);
        // double max_mean_local_disk_used = lexical_cast<double>(cols[27]);
        // double sd_mean_local_disk_used = lexical_cast<double>(cols[29]);
        // double min_cpi = lexical_cast<double>(cols[30]);
        // double max_cpi = lexical_cast<double>(cols[31]);
        // double sd_cpi = lexical_cast<double>(cols[33]);
        // double min_mai = lexical_cast<double>(cols[34]);
        // double max_mai = lexical_cast<double>(cols[35]);
        // double sd_mai = lexical_cast<double>(cols[37]);
      }
    }
    num_line++;
  }
  fclose(usage_file);
}

void GoogleTraceLoader::LoadTasksRunningTime(
    unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
      task_runtime) {
  char line[200];
  vector<string> cols;
  FILE* tasks_file = NULL;
  string tasks_file_name = FLAGS_trace_path +
    "/task_runtime_events/task_runtime_events.csv";
  if ((tasks_file = fopen(tasks_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace runtime events file.";
  }

  int64_t num_line = 1;
  while (!feof(tasks_file)) {
    if (fscanf(tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(","), token_compress_off);
      if (cols.size() != 13) {
        LOG(ERROR) << "Unexpected structure of task runtime row on line: "
                   << num_line;
      } else {
        TraceTaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);

        // Sub-sample the trace if we only retain < 100% of tasks.
        if (SpookyHash::Hash64(&task_id, sizeof(task_id), kSeed) >
            MaxEventHashToRetain()) {
          // skip event
          continue;
        }

        uint64_t runtime = lexical_cast<uint64_t>(cols[4]);
        if (!InsertIfNotPresent(task_runtime, task_id, runtime) &&
            VLOG_IS_ON(1)) {
          LOG(ERROR) << "LoadTasksRunningTime: There should not be more than "
                     << "one entry for job " << task_id.job_id
                     << ", task " << task_id.task_index;
        } else {
          VLOG(2) << "Loaded runtime for "
                  << task_id.job_id << "/" << task_id.task_index;
        }
      }
    }
    num_line++;
  }
  fclose(tasks_file);
}

uint64_t GoogleTraceLoader::MaxEventHashToRetain() {
  // We must check if we're retaining all events. If so, we have to return
  // UINT64_MAX because otherwise we might end up overflowing.
  if (IsEqual(FLAGS_events_fraction, 1.0)) {
    return UINT64_MAX;
  } else {
    return FLAGS_events_fraction * UINT64_MAX;
  }
}

} // namespace sim
} // namespace firmament
