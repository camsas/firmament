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

// Google resource utilization trace processor.

#include "sim/google_trace_task_processor.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <errno.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdio>
#include <limits>
#include <utility>
#include <vector>

#include "misc/map-util.h"
#include "misc/string_utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

#define TASK_SUBMIT 0
#define TASK_SCHEDULE 1
#define TASK_EVICT 2
#define TASK_FAIL 3
#define TASK_FINISH 4
#define TASK_KILL 5
#define TASK_LOST 6
#define TASK_UPDATE_PENDING 7
#define TASK_UPDATE_RUNNING 8

#define EPS 0.00001

DECLARE_bool(aggregate_task_usage);
DECLARE_bool(jobs_runtime);
DECLARE_bool(jobs_num_tasks);
DECLARE_int32(num_files_to_process);

DEFINE_uint64(bin_time_duration, 10, "Bin size in microseconds.");

namespace firmament {
namespace sim {

  void MkdirIfNotPresent(const string &directory) {
    if (mkdir(directory.c_str(), 0777) < 0) {
      // mkdir error
      if (errno != EEXIST) {
        // it's fine if directory already exists, ignore
        PLOG(FATAL) << "Could not make directory " << directory;
      }
    }
  }

  GoogleTraceTaskProcessor::GoogleTraceTaskProcessor(const string& trace_path):
    trace_path_(trace_path) {
  }

  multimap<uint64_t, TaskSchedulingEvent>&
    GoogleTraceTaskProcessor::ReadTaskStateChangingEvents(
      unordered_map<uint64_t, uint64_t>* job_num_tasks) {
    // Store the scheduling events for every timestamp.
    multimap<uint64_t, TaskSchedulingEvent> *scheduling_events =
      new multimap<uint64_t, TaskSchedulingEvent>();
    char line[200];
    vector<string> line_cols;
    FILE* events_file = NULL;
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      LOG(INFO) << "Reading task_events file " << file_num;
      string file_name;
      spf(&file_name, "%s/task_events/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((events_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(FATAL) << "Failed to open trace for reading of task events.";
      }
      int64_t num_line = 1;
      while (!feof(events_file)) {
        if (fscanf(events_file, "%[^\n]%*[\n]", &line[0]) > 0) {
          boost::split(line_cols, line, is_any_of(","), token_compress_off);
          if (line_cols.size() != 13) {
            LOG(ERROR) << "Unexpected structure of task event on line "
                       << num_line << ": found " << line_cols.size()
                       << " columns.";
          } else {
            uint64_t timestamp = lexical_cast<uint64_t>(line_cols[0]);
            uint64_t job_id = lexical_cast<uint64_t>(line_cols[2]);
            uint64_t task_index = lexical_cast<uint64_t>(line_cols[3]);
            int32_t task_event = lexical_cast<int32_t>(line_cols[5]);
            // Only handle the events we're interested in. We do not care about
            // TASK_SUBMIT because that's not the event that starts a task. The
            // events we are interested in are the ones that change the state
            // of a task to/from running.
            if (task_event == TASK_SCHEDULE || task_event == TASK_EVICT ||
                task_event == TASK_FAIL || task_event == TASK_FINISH ||
                task_event == TASK_KILL || task_event == TASK_LOST) {
              TaskSchedulingEvent event;
              event.job_id_ = job_id;
              event.task_index_ = task_index;
              event.event_type_ = task_event;
              scheduling_events->insert(
                  pair<uint64_t, TaskSchedulingEvent>(timestamp, event));
            }
            if (FLAGS_jobs_num_tasks && task_event == TASK_SUBMIT) {
              uint64_t* num_tasks = FindOrNull(*job_num_tasks, job_id);
              if (num_tasks == NULL) {
                CHECK(InsertOrUpdate(job_num_tasks, job_id, 1));
              } else {
                (*num_tasks)++;
              }
            }
          }
        }
        num_line++;
      }
      fclose(events_file);
    }
    return *scheduling_events;
  }

  void GoogleTraceTaskProcessor::BinTasksByEventType(int32_t event,
                                                     FILE* out_file) {
    char line[200];
    vector<string> vals;
    FILE* fptr = NULL;
    uint64_t time_interval_bound = FLAGS_bin_time_duration;
    uint64_t num_tasks = 0;
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      string fname;
      spf(&fname, "%s/task_events/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((fptr = fopen(fname.c_str(), "r")) == NULL) {
        LOG(ERROR) << "Failed to open trace for reading of task events.";
      }
      while (!feof(fptr)) {
        if (fscanf(fptr, "%[^\n]%*[\n]", &line[0]) > 0) {
          boost::split(vals, line, is_any_of(","), token_compress_off);
          if (vals.size() != 13) {
            LOG(ERROR) << "Unexpected structure of task event row: found "
                       << vals.size() << " columns.";
          } else {
            uint64_t task_time = lexical_cast<uint64_t>(vals[0]);
            int32_t event_type = lexical_cast<int32_t>(vals[5]);
            if (event_type == event) {
              if (task_time <= time_interval_bound) {
                num_tasks++;
              } else {
                fprintf(out_file, "(%ju, %ju]: %ju\n",
                        time_interval_bound - FLAGS_bin_time_duration,
                        time_interval_bound, num_tasks);
                time_interval_bound += FLAGS_bin_time_duration;
                while (time_interval_bound < task_time) {
                  fprintf(out_file, "(%ju, %ju]: 0\n",
                          time_interval_bound - FLAGS_bin_time_duration,
                          time_interval_bound);
                  time_interval_bound += FLAGS_bin_time_duration;
                }
                num_tasks = 1;
              }
            }
          }
        }
      }
      fclose(fptr);
    }
    fprintf(out_file, "(%ju, %ju]: %ju\n",
            time_interval_bound - FLAGS_bin_time_duration,
            time_interval_bound, num_tasks);
  }

  TaskResourceUsage GoogleTraceTaskProcessor::BuildTaskResourceUsage(
      vector<string>& line_cols) {
    TaskResourceUsage task_resource_usage;
    // Set resource value to -1 if not present. We can then later not take it
    // into account when we compute the statistics.
    for (uint32_t index = 5; index < 17; index++) {
      if (!line_cols[index].compare("")) {
        line_cols[index] = "-1";
      }
    }
    task_resource_usage.mean_cpu_usage_ = lexical_cast<double>(line_cols[5]);
    task_resource_usage.canonical_mem_usage_ =
      lexical_cast<double>(line_cols[6]);
    task_resource_usage.assigned_mem_usage_ =
      lexical_cast<double>(line_cols[7]);
    task_resource_usage.unmapped_page_cache_ =
      lexical_cast<double>(line_cols[8]);
    task_resource_usage.total_page_cache_ = lexical_cast<double>(line_cols[9]);
    task_resource_usage.max_mem_usage_ = lexical_cast<double>(line_cols[10]);
    task_resource_usage.mean_disk_io_time_ =
      lexical_cast<double>(line_cols[11]);
    task_resource_usage.mean_local_disk_used_ =
      lexical_cast<double>(line_cols[12]);
    task_resource_usage.max_cpu_usage_ = lexical_cast<double>(line_cols[13]);
    task_resource_usage.max_disk_io_time_ = lexical_cast<double>(line_cols[14]);
    task_resource_usage.cpi_ = lexical_cast<double>(line_cols[15]);
    task_resource_usage.mai_ = lexical_cast<double>(line_cols[16]);
    return task_resource_usage;
  }

  void GoogleTraceTaskProcessor::ExpandTaskEvent(
      uint64_t timestamp, const TaskIdentifier& task_id, int32_t event_type,
      unordered_map<TaskIdentifier, TaskRuntime,
                    TaskIdentifierHasher>* tasks_runtime,
      unordered_map<uint64_t, string>* job_id_to_name,
      vector<string>& line_cols) {
    if (event_type == TASK_SCHEDULE) {
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        TaskRuntime task_runtime;
        task_runtime.start_time_ = timestamp;
        task_runtime.last_schedule_time_ = timestamp;
        PopulateTaskRuntime(&task_runtime, line_cols);
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      } else {
        // Update the last scheduling time for the task. Assumes that
        // the previously running instance of the task has finished/failed.
        task_runtime_ptr->last_schedule_time_ = timestamp;
        PopulateTaskRuntime(task_runtime_ptr, line_cols);
      }
    } else if (event_type == TASK_EVICT || event_type == TASK_FAIL ||
               event_type == TASK_KILL || event_type == TASK_LOST) {
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        // First event for this task. This means that the task was running
        // from the beginning of the trace.
        TaskRuntime task_runtime;
        task_runtime.last_schedule_time_ = -1;  // unscheduled
        task_runtime.start_time_ = 0;
        task_runtime.num_runs_ = 1;
        task_runtime.total_runtime_ = timestamp;
        PopulateTaskRuntime(&task_runtime, line_cols);
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      } else {
        // Update the runtime for the task. The failed tasks are included
        // in the runtime.
        task_runtime_ptr->num_runs_++;
        task_runtime_ptr->total_runtime_ +=
          timestamp - task_runtime_ptr->last_schedule_time_;
        PopulateTaskRuntime(task_runtime_ptr, line_cols);
        task_runtime_ptr->last_schedule_time_ = -1;  // unscheduled
      }
    } else if (event_type == TASK_FINISH) {
      string logical_job_name = (*job_id_to_name)[task_id.job_id_];
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        // First event for this task.
        TaskRuntime task_runtime;
        task_runtime.last_schedule_time_ = -1;  // unscheduled
        task_runtime.start_time_ = 0;
        task_runtime.num_runs_ = 1;
        task_runtime.total_runtime_ = timestamp;
        PopulateTaskRuntime(&task_runtime, line_cols);
        task_runtime.runtime_ = timestamp;
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      } else {
        task_runtime_ptr->num_runs_++;
        task_runtime_ptr->total_runtime_ +=
          timestamp - task_runtime_ptr->last_schedule_time_;
        PopulateTaskRuntime(task_runtime_ptr, line_cols);
        CHECK_GE(task_runtime_ptr->last_schedule_time_, 0);
        // NOTE: runtime_ represents the time the task spent running in the run
        // that finished correctly. This value is computed as
        // timestamp of TASK_FINISHED - timestamp of TASK_SCHEDULED. If a task
        // fails then in the trace there's going to be another SCHEDULE event
        // for it before a FINISH event. Moreover, it is guaranteed that there
        // won't be any other task events between the SCHEDULE and the FINISH
        // event. Hence, we just make sure to reset a task's
        // last_schedule_time_ whenever we a TASK_SCHEDULE event.
        // XXX(ionel): As it stands the code assumes that all the work done
        // before an eviction is lost. Otherwise, we would have to account
        // for the time before the eviction.
        task_runtime_ptr->runtime_ =
          timestamp - task_runtime_ptr->last_schedule_time_;
        task_runtime_ptr->last_schedule_time_ = -2;  // unscheduled
      }
    } else if (event_type == TASK_UPDATE_PENDING ||
               event_type == TASK_UPDATE_RUNNING) {
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        // First event for this task. Task has been running from the
        // beginning of the trace.
        TaskRuntime task_runtime;
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      }
    }
  }

  void GoogleTraceTaskProcessor::InitializeResourceUsageStats(
      TaskResourceUsageStats* usage_stats) {
    usage_stats->min_usage_.mean_cpu_usage_ = numeric_limits<double>::max();
    usage_stats->min_usage_.canonical_mem_usage_ =
    numeric_limits<double>::max();
    usage_stats->min_usage_.assigned_mem_usage_ = numeric_limits<double>::max();
    usage_stats->min_usage_.unmapped_page_cache_ =
    numeric_limits<double>::max();
    usage_stats->min_usage_.total_page_cache_ = numeric_limits<double>::max();
    usage_stats->min_usage_.max_mem_usage_ = numeric_limits<double>::max();
    usage_stats->min_usage_.mean_disk_io_time_ = numeric_limits<double>::max();
    usage_stats->min_usage_.mean_local_disk_used_ =
    numeric_limits<double>::max();
    usage_stats->min_usage_.max_cpu_usage_ = numeric_limits<double>::max();
    usage_stats->min_usage_.max_disk_io_time_ = numeric_limits<double>::max();
    usage_stats->min_usage_.cpi_ = numeric_limits<double>::max();
    usage_stats->min_usage_.mai_ = numeric_limits<double>::max();
    usage_stats->max_usage_.mean_cpu_usage_ = -1.0;
    usage_stats->max_usage_.canonical_mem_usage_ = -1.0;
    usage_stats->max_usage_.assigned_mem_usage_ = -1.0;
    usage_stats->max_usage_.unmapped_page_cache_ = -1.0;
    usage_stats->max_usage_.total_page_cache_ = -1.0;
    usage_stats->max_usage_.max_mem_usage_ = -1.0;
    usage_stats->max_usage_.mean_disk_io_time_ = -1.0;
    usage_stats->max_usage_.mean_local_disk_used_ = -1.0;
    usage_stats->max_usage_.max_cpu_usage_ = -1.0;
    usage_stats->max_usage_.max_disk_io_time_ = -1.0;
    usage_stats->max_usage_.cpi_ = -1.0;
    usage_stats->max_usage_.mai_ = -1.0;
    usage_stats->avg_usage_.mean_cpu_usage_ = 0;
    usage_stats->avg_usage_.canonical_mem_usage_ = 0;
    usage_stats->avg_usage_.assigned_mem_usage_ = 0;
    usage_stats->avg_usage_.unmapped_page_cache_ = 0;
    usage_stats->avg_usage_.total_page_cache_ = 0;
    usage_stats->avg_usage_.max_mem_usage_ = 0;
    usage_stats->avg_usage_.mean_disk_io_time_ = 0;
    usage_stats->avg_usage_.mean_local_disk_used_ = 0;
    usage_stats->avg_usage_.max_cpu_usage_ = 0;
    usage_stats->avg_usage_.max_disk_io_time_ = 0;
    usage_stats->avg_usage_.cpi_ = 0;
    usage_stats->avg_usage_.mai_ = 0;
    usage_stats->sample_count_mean_cpu_usage_ = 0;
    usage_stats->sample_count_canonical_mem_usage_ = 0;
    usage_stats->sample_count_assigned_mem_usage_ = 0;
    usage_stats->sample_count_unmapped_page_cache_ = 0;
    usage_stats->sample_count_total_page_cache_ = 0;
    usage_stats->sample_count_max_mem_usage_ = 0;
    usage_stats->sample_count_mean_disk_io_time_ = 0;
    usage_stats->sample_count_mean_local_disk_used_ = 0;
    usage_stats->sample_count_max_cpu_usage_ = 0;
    usage_stats->sample_count_max_disk_io_time_ = 0;
    usage_stats->sample_count_cpi_ = 0;
    usage_stats->sample_count_mai_ = 0;
  }

  void GoogleTraceTaskProcessor::PrintStats(
      FILE* usage_stat_file, const TaskIdentifier& task_id,
      const TaskResourceUsageStats& task_stats) {
    fprintf(usage_stat_file, "%ju,%ju", task_id.job_id_, task_id.task_index_);

    // Set values to -1 if no samples have been recorded.
    if (task_stats.sample_count_mean_cpu_usage_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.mean_cpu_usage_,
              task_stats.max_usage_.mean_cpu_usage_,
              task_stats.avg_usage_.mean_cpu_usage_,
              sqrt(task_stats.variance_usage_.mean_cpu_usage_));
    }
    if (task_stats.sample_count_canonical_mem_usage_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.canonical_mem_usage_,
              task_stats.max_usage_.canonical_mem_usage_,
              task_stats.avg_usage_.canonical_mem_usage_,
              sqrt(task_stats.variance_usage_.canonical_mem_usage_));
    }
    if (task_stats.sample_count_assigned_mem_usage_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.assigned_mem_usage_,
              task_stats.max_usage_.assigned_mem_usage_,
              task_stats.avg_usage_.assigned_mem_usage_,
              sqrt(task_stats.variance_usage_.assigned_mem_usage_));
    }
    if (task_stats.sample_count_unmapped_page_cache_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.unmapped_page_cache_,
              task_stats.max_usage_.unmapped_page_cache_,
              task_stats.avg_usage_.unmapped_page_cache_,
              sqrt(task_stats.variance_usage_.unmapped_page_cache_));
    }
    if (task_stats.sample_count_total_page_cache_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.total_page_cache_,
              task_stats.max_usage_.total_page_cache_,
              task_stats.avg_usage_.total_page_cache_,
              sqrt(task_stats.variance_usage_.total_page_cache_));
    }
    if (task_stats.sample_count_mean_disk_io_time_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.mean_disk_io_time_,
              task_stats.max_usage_.mean_disk_io_time_,
              task_stats.avg_usage_.mean_disk_io_time_,
              sqrt(task_stats.variance_usage_.mean_disk_io_time_));
    }
    if (task_stats.sample_count_mean_local_disk_used_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.mean_local_disk_used_,
              task_stats.max_usage_.mean_local_disk_used_,
              task_stats.avg_usage_.mean_local_disk_used_,
              sqrt(task_stats.variance_usage_.mean_local_disk_used_));
    }

    if (task_stats.sample_count_cpi_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf",
              task_stats.min_usage_.cpi_,
              task_stats.max_usage_.cpi_,
              task_stats.avg_usage_.cpi_,
              sqrt(task_stats.variance_usage_.cpi_));
    }

    if (task_stats.sample_count_mai_ == 0) {
      fprintf(usage_stat_file, ",-1.0,-1.0,-1.0,-1.0\n");
    } else {
      fprintf(usage_stat_file, ",%lf,%lf,%lf,%lf\n",
              task_stats.min_usage_.mai_,
              task_stats.max_usage_.mai_,
              task_stats.avg_usage_.mai_,
              sqrt(task_stats.variance_usage_.mai_));
    }
    // XXX(ionel): We don't print aggerageted statistics of max_cpu_usage,
    // max_mem_usage and max_disk_io_time.
  }

  void GoogleTraceTaskProcessor::AggregateTaskUsage() {
    unordered_map<uint64_t, uint64_t>* job_num_tasks =
      new unordered_map<uint64_t, uint64_t>();
    multimap<uint64_t, TaskSchedulingEvent>& scheduling_events =
      ReadTaskStateChangingEvents(job_num_tasks);
    job_num_tasks->clear();
    delete job_num_tasks;
    // Map job id to map of task id to vector of resource usage.
    unordered_map<TaskIdentifier, TaskResourceUsageStats,
                  TaskIdentifierHasher> task_usage_stats;
    // Set containg the tasks for which we've seen the FINISH event. This set
    // is used to filter task usage events that have been recoreded after the
    // end of the task.
    unordered_set<TaskIdentifier, TaskIdentifierHasher> finished_tasks;
    char line[200];
    vector<string> line_cols;
    FILE* usage_file = NULL;
    FILE* usage_stat_file = NULL;
    string usage_directory;
    spf(&usage_directory, "%s/task_usage_stat", trace_path_.c_str());
    MkdirIfNotPresent(usage_directory);
    string usage_file_name;
    spf(&usage_file_name, "%s/task_usage_stat.csv", usage_directory.c_str());
    if ((usage_stat_file = fopen(usage_file_name.c_str(), "w")) == NULL) {
      LOG(FATAL) << "Failed to open task_usage_stat file for writing";
    }
    uint64_t last_timestamp = 0;
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      LOG(INFO) << "Reading task_usage file " << file_num;
      string file_name;
      spf(&file_name, "%s/task_usage/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((usage_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(FATAL) << "Failed to open trace for reading of task "
                   << "resource usage.";
      }
      int64_t num_line = 1;
      while (!feof(usage_file)) {
        if (fscanf(usage_file, "%[^\n]%*[\n]", &line[0]) > 0) {
          boost::split(line_cols, line, is_any_of(","), token_compress_off);
          if (line_cols.size() != 19 && line_cols.size() != 20) {
            // 19 columns in v2 of trace, 20 columns in v2.1 of trace
            // (we do not use the 20th column, being sampled CPU usage)
            LOG(ERROR) << "Unexpected structure of task usage on line "
                       << num_line << ": found " << line_cols.size()
                       << " columns.";
          } else {
            uint64_t start_timestamp = lexical_cast<uint64_t>(line_cols[0]);
            TaskIdentifier cur_task_id;
            cur_task_id.job_id_ = lexical_cast<uint64_t>(line_cols[2]);
            cur_task_id.task_index_ = lexical_cast<uint64_t>(line_cols[3]);
            if (last_timestamp < start_timestamp) {
              ProcessSchedulingEvents(last_timestamp, &scheduling_events,
                                      &task_usage_stats, &finished_tasks,
                                      usage_stat_file);
            }
            last_timestamp = start_timestamp;
            if (finished_tasks.find(cur_task_id) != finished_tasks.end()) {
              // We've already seen a FINISH event for the task. Ignore task
              // usage statistics after the end of the task.
              num_line++;
              continue;
            }
            TaskResourceUsage task_resource_usage =
              BuildTaskResourceUsage(line_cols);
            TaskResourceUsageStats* usage_stats_ptr =
              FindOrNull(task_usage_stats, cur_task_id);
            if (!usage_stats_ptr) {
              TaskResourceUsageStats new_usage_stats;
              InitializeResourceUsageStats(&new_usage_stats);
              UpdateUsageStats(task_resource_usage, &new_usage_stats);
              InsertOrUpdate(&task_usage_stats, cur_task_id, new_usage_stats);
            } else {
              UpdateUsageStats(task_resource_usage, usage_stats_ptr);
            }
          }
        }
        num_line++;
      }
      fclose(usage_file);
    }
    // Process the scheduling events up to the last timestamp.
    ProcessSchedulingEvents(last_timestamp, &scheduling_events,
                            &task_usage_stats, &finished_tasks,
                            usage_stat_file);
    // Write stats for tasks that are still running.
    for (auto &task_id_to_usage : task_usage_stats) {
      PrintStats(usage_stat_file, task_id_to_usage.first,
                 task_id_to_usage.second);
    }
    task_usage_stats.clear();
    scheduling_events.clear();
    fclose(usage_stat_file);
  }

  // Returns a mapping job id to logical job name.
  unordered_map<uint64_t, string>&
      GoogleTraceTaskProcessor::ReadLogicalJobsName() {
    unordered_map<uint64_t, string> *job_id_to_name =
      new unordered_map<uint64_t, string>();
    char line[200];
    vector<string> line_cols;
    FILE* events_file = NULL;
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      LOG(INFO) << "Reading job_events file " << file_num;
      string file_name;
      spf(&file_name, "%s/job_events/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((events_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(FATAL) << "Failed to open trace for reading of job events.";
      }
      int64_t num_line = 1;
      while (!feof(events_file)) {
        if (fscanf(events_file, "%[^\n]%*[\n]", &line[0]) > 0) {
          boost::split(line_cols, line, is_any_of(","), token_compress_off);
          if (line_cols.size() != 8) {
            LOG(ERROR) << "Unexpected structure of job event on line "
                       << num_line << ": found " << line_cols.size()
                       << " columns.";
          } else {
            uint64_t job_id = lexical_cast<uint64_t>(line_cols[2]);
            InsertOrUpdate(job_id_to_name, job_id, line_cols[7]);
          }
        }
        num_line++;
      }
      fclose(events_file);
    }
    return *job_id_to_name;
  }

  void GoogleTraceTaskProcessor::PopulateTaskRuntime(
      TaskRuntime* task_runtime_ptr, vector<string>& cols) {
    for (uint32_t index = 7; index < 13; ++index) {
      if (!cols[index].compare("")) {
        cols[index] = "-1";
      }
    }
    task_runtime_ptr->scheduling_class_ = lexical_cast<uint64_t>(cols[7]);
    task_runtime_ptr->priority_ = lexical_cast<uint64_t>(cols[8]);
    task_runtime_ptr->cpu_request_ = lexical_cast<double>(cols[9]);
    task_runtime_ptr->ram_request_ = lexical_cast<double>(cols[10]);
    task_runtime_ptr->disk_request_ = lexical_cast<double>(cols[11]);
    task_runtime_ptr->machine_constraint_ = lexical_cast<int32_t>(cols[12]);
  }

  void GoogleTraceTaskProcessor::PrintTaskRuntime(
      FILE* out_events_file, const TaskRuntime& task_runtime,
      const TaskIdentifier& task_id, string logical_job_name) {
    fprintf(out_events_file, "%ju,%ju,%s,%jd,%jd"
            ",%ju,%ju,%jd,%jd,%lf,%lf,%lf,%d\n",
            task_id.job_id_, task_id.task_index_, logical_job_name.c_str(),
            task_runtime.start_time_, task_runtime.total_runtime_,
            task_runtime.runtime_, task_runtime.num_runs_,
            task_runtime.scheduling_class_, task_runtime.priority_,
            task_runtime.cpu_request_, task_runtime.ram_request_,
            task_runtime.disk_request_, task_runtime.machine_constraint_);
  }

  void GoogleTraceTaskProcessor::ProcessSchedulingEvents(
      uint64_t timestamp,
      multimap<uint64_t, TaskSchedulingEvent>* scheduling_events,
      unordered_map<TaskIdentifier, TaskResourceUsageStats,
                    TaskIdentifierHasher>* task_usage_stats,
      unordered_set<TaskIdentifier, TaskIdentifierHasher>* finished_tasks,
      FILE* usage_stat_file) {
    multimap<uint64_t, TaskSchedulingEvent>::iterator it_to =
      scheduling_events->upper_bound(timestamp);
    multimap<uint64_t, TaskSchedulingEvent>::iterator it =
      scheduling_events->begin();
    for (; it != it_to; ++it) {
      TaskSchedulingEvent evt = it->second;
      if (evt.event_type_ == TASK_FINISH) {
        // Print statistics for finished tasks.
        TaskIdentifier task_id;
        task_id.job_id_ = evt.job_id_;
        task_id.task_index_ = evt.task_index_;
        PrintStats(usage_stat_file, task_id, (*task_usage_stats)[task_id]);
        task_usage_stats->erase(task_id);
        finished_tasks->insert(task_id);
      }
    }
    // Free task scheduling events.
    scheduling_events->erase(scheduling_events->begin(), it_to);
  }

  void GoogleTraceTaskProcessor::JobsRuntimeEvents() {
    unordered_map<uint64_t, string>& job_id_to_name = ReadLogicalJobsName();
    unordered_map<TaskIdentifier, TaskRuntime,
                  TaskIdentifierHasher> tasks_runtime;
    uint64_t end_simulation_time = 0;
    char line[200];
    vector<string> line_cols;
    FILE* events_file = NULL;
    string out_events_directory;
    spf(&out_events_directory, "%s/task_runtime_events", trace_path_.c_str());
    MkdirIfNotPresent(out_events_directory);
    FILE* out_events_file;
    string out_file_name;
    spf(&out_file_name, "%s/task_runtime_events.csv",
        out_events_directory.c_str());
    if ((out_events_file = fopen(out_file_name.c_str(), "w")) == NULL) {
      LOG(FATAL) << "Failed to open task_runtime_events file for writing";
    }
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      LOG(INFO) << "Reading task_events file " << file_num;
      string file_name;
      spf(&file_name, "%s/task_events/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((events_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(FATAL) << "Failed to open trace for reading of task events.";
      }
      int64_t num_line = 1;
      while (!feof(events_file)) {
        if (fscanf(events_file, "%[^\n]%*[\n]", &line[0]) > 0) {
          boost::split(line_cols, line, is_any_of(","), token_compress_off);
          if (line_cols.size() != 13) {
            LOG(ERROR) << "Unexpected structure of task event on line "
                       << num_line << ": found " << line_cols.size()
                       << " columns.";
          } else {
            TaskIdentifier task_id;
            uint64_t timestamp = lexical_cast<uint64_t>(line_cols[0]);
            if (timestamp < numeric_limits<int64_t>::max()) {
              end_simulation_time = max(end_simulation_time, timestamp);
            }
            task_id.job_id_ = lexical_cast<uint64_t>(line_cols[2]);
            task_id.task_index_ = lexical_cast<uint64_t>(line_cols[3]);
            int32_t event_type = lexical_cast<int32_t>(line_cols[5]);
            ExpandTaskEvent(timestamp, task_id, event_type, &tasks_runtime,
                            &job_id_to_name, line_cols);
          }
        }
        num_line++;
      }
      fclose(events_file);
    }

    for (auto& task_id_runtime : tasks_runtime) {
      TaskIdentifier task_id = task_id_runtime.first;
      string logical_job_name =  job_id_to_name[task_id.job_id_];
      TaskRuntime task_runtime = task_id_runtime.second;
      if (task_runtime.last_schedule_time_ >= 0) {
        // Task is still running.
        if (task_runtime.num_runs_ == 0) {
          // It's the first time the task is running. We assume it
          // runs until the end of the trace.
          task_runtime.runtime_ =
            end_simulation_time - task_runtime.last_schedule_time_;
          task_runtime.num_runs_++;
          task_runtime.total_runtime_ = task_runtime.runtime_;
        } else {
          if (task_runtime.runtime_ == 0) {
            // The task has never completed successfully.
            // We assume that the task is going to run for the average duration
            // of the previous failed runs.
            task_runtime.runtime_ =
              task_runtime.total_runtime_ / task_runtime.num_runs_;
          }
          // Make sure the time left to run the task doesn't exceed
          // simulation's end time.
          if (task_runtime.runtime_ >
              end_simulation_time - task_runtime.last_schedule_time_) {
            task_runtime.total_runtime_ +=
              end_simulation_time - task_runtime.last_schedule_time_;
          } else {
            task_runtime.total_runtime_ += task_runtime.runtime_;
          }
          task_runtime.num_runs_++;
        }
      } else {
        if (task_runtime.runtime_ == 0) {
          // The task has never completed successfully. Set the runtime
          // to the average of the failed runs.
          task_runtime.runtime_ =
            task_runtime.total_runtime_ / task_runtime.num_runs_;
        }
      }
      PrintTaskRuntime(out_events_file, task_runtime, task_id,
                       logical_job_name);
    }
    job_id_to_name.clear();
    delete &job_id_to_name;
    fclose(out_events_file);
  }

  void GoogleTraceTaskProcessor::JobsNumTasks() {
    unordered_map<uint64_t, uint64_t>* job_num_tasks =
      new unordered_map<uint64_t, uint64_t>();
    multimap<uint64_t, TaskSchedulingEvent>& scheduling_events =
      ReadTaskStateChangingEvents(job_num_tasks);

    string out_directory;
    spf(&out_directory, "%s/jobs_num_tasks/", trace_path_.c_str());
    MkdirIfNotPresent(out_directory);
    FILE* out_file = NULL;
    string out_file_name;
    spf(&out_file_name, "%s/jobs_num_tasks.csv", out_directory.c_str());
    if ((out_file = fopen(out_file_name.c_str(), "w")) == NULL) {
      LOG(FATAL) << "Failed to open jobs_num_tasks file for writing";
    }

    for (unordered_map<uint64_t, uint64_t>::iterator
           it = job_num_tasks->begin();
         it != job_num_tasks->end(); ++it) {
      fprintf(out_file, "%ju,%ju\n", it->first, it->second);
    }
    fclose(out_file);
    scheduling_events.clear();
    job_num_tasks->clear();
    delete job_num_tasks;
  }

  void GoogleTraceTaskProcessor::Run() {
    if (FLAGS_jobs_runtime) {
      JobsRuntimeEvents();
    }
    if (FLAGS_jobs_num_tasks) {
      JobsNumTasks();
    }
    if (FLAGS_aggregate_task_usage) {
      AggregateTaskUsage();
    }
  }

  void GoogleTraceTaskProcessor::UpdateStats(double task_usage,
                                             double* min_usage,
                                             double* max_usage,
                                             double* avg_usage,
                                             double* variance_usage,
                                             uint32_t* num_usage) {
    *min_usage = min(*min_usage, task_usage);
    *max_usage = max(*max_usage, task_usage);
    *num_usage = *num_usage + 1;
    // Incrementally compute variance using the following forumla:
    // var(n) = (n - 2) / (n - 1) * var(n - 1) + 1/n*(x(n) - avg(x1..x(n-1)))^2.
    if (*num_usage == 1) {
      *variance_usage = 0;
    } else {
      *variance_usage = *variance_usage * (*num_usage - 2) / (*num_usage - 1) +
        (task_usage - *avg_usage) * (task_usage - *avg_usage) / (*num_usage);
    }
    *avg_usage = (*avg_usage * (*num_usage - 1) + task_usage) / (*num_usage);
  }

  void GoogleTraceTaskProcessor::UpdateUsageStats(
      const TaskResourceUsage& task_resource_usage,
      TaskResourceUsageStats* usage_stats) {
    if (task_resource_usage.mean_cpu_usage_ >= 0.0) {
      UpdateStats(task_resource_usage.mean_cpu_usage_,
                  &usage_stats->min_usage_.mean_cpu_usage_,
                  &usage_stats->max_usage_.mean_cpu_usage_,
                  &usage_stats->avg_usage_.mean_cpu_usage_,
                  &usage_stats->variance_usage_.mean_cpu_usage_,
                  &usage_stats->sample_count_mean_cpu_usage_);
    }
    if (task_resource_usage.canonical_mem_usage_ >= 0.0) {
      UpdateStats(task_resource_usage.canonical_mem_usage_,
                  &usage_stats->min_usage_.canonical_mem_usage_,
                  &usage_stats->max_usage_.canonical_mem_usage_,
                  &usage_stats->avg_usage_.canonical_mem_usage_,
                  &usage_stats->variance_usage_.canonical_mem_usage_,
                  &usage_stats->sample_count_canonical_mem_usage_);
    }
    if (task_resource_usage.assigned_mem_usage_ >= 0.0) {
      UpdateStats(task_resource_usage.assigned_mem_usage_,
                  &usage_stats->min_usage_.assigned_mem_usage_,
                  &usage_stats->max_usage_.assigned_mem_usage_,
                  &usage_stats->avg_usage_.assigned_mem_usage_,
                  &usage_stats->variance_usage_.assigned_mem_usage_,
                  &usage_stats->sample_count_assigned_mem_usage_);
    }
    if (task_resource_usage.unmapped_page_cache_ >= 0.0) {
      UpdateStats(task_resource_usage.unmapped_page_cache_,
                  &usage_stats->min_usage_.unmapped_page_cache_,
                  &usage_stats->max_usage_.unmapped_page_cache_,
                  &usage_stats->avg_usage_.unmapped_page_cache_,
                  &usage_stats->variance_usage_.unmapped_page_cache_,
                  &usage_stats->sample_count_unmapped_page_cache_);
    }
    if (task_resource_usage.total_page_cache_ >= 0.0) {
      UpdateStats(task_resource_usage.total_page_cache_,
                  &usage_stats->min_usage_.total_page_cache_,
                  &usage_stats->max_usage_.total_page_cache_,
                  &usage_stats->avg_usage_.total_page_cache_,
                  &usage_stats->variance_usage_.total_page_cache_,
                  &usage_stats->sample_count_total_page_cache_);
    }
    if (task_resource_usage.max_mem_usage_ >= 0.0) {
      UpdateStats(task_resource_usage.max_mem_usage_,
                  &usage_stats->min_usage_.max_mem_usage_,
                  &usage_stats->max_usage_.max_mem_usage_,
                  &usage_stats->avg_usage_.max_mem_usage_,
                  &usage_stats->variance_usage_.max_mem_usage_,
                  &usage_stats->sample_count_max_mem_usage_);
    }
    if (task_resource_usage.mean_disk_io_time_ >= 0.0) {
      UpdateStats(task_resource_usage.mean_disk_io_time_,
                  &usage_stats->min_usage_.mean_disk_io_time_,
                  &usage_stats->max_usage_.mean_disk_io_time_,
                  &usage_stats->avg_usage_.mean_disk_io_time_,
                  &usage_stats->variance_usage_.mean_disk_io_time_,
                  &usage_stats->sample_count_mean_disk_io_time_);
    }
    if (task_resource_usage.mean_local_disk_used_ >= 0.0) {
      UpdateStats(task_resource_usage.mean_local_disk_used_,
                  &usage_stats->min_usage_.mean_local_disk_used_,
                  &usage_stats->max_usage_.mean_local_disk_used_,
                  &usage_stats->avg_usage_.mean_local_disk_used_,
                  &usage_stats->variance_usage_.mean_local_disk_used_,
                  &usage_stats->sample_count_mean_local_disk_used_);
    }
    if (task_resource_usage.max_cpu_usage_ >= 0.0) {
      UpdateStats(task_resource_usage.max_cpu_usage_,
                  &usage_stats->min_usage_.max_cpu_usage_,
                  &usage_stats->max_usage_.max_cpu_usage_,
                  &usage_stats->avg_usage_.max_cpu_usage_,
                  &usage_stats->variance_usage_.max_cpu_usage_,
                  &usage_stats->sample_count_max_cpu_usage_);
    }
    if (task_resource_usage.max_disk_io_time_ >= 0.0) {
      UpdateStats(task_resource_usage.max_disk_io_time_,
                  &usage_stats->min_usage_.max_disk_io_time_,
                  &usage_stats->max_usage_.max_disk_io_time_,
                  &usage_stats->avg_usage_.max_disk_io_time_,
                  &usage_stats->variance_usage_.max_disk_io_time_,
                  &usage_stats->sample_count_max_disk_io_time_);
    }
    if (task_resource_usage.cpi_ >= 0.0) {
      UpdateStats(task_resource_usage.cpi_,
                  &usage_stats->min_usage_.cpi_,
                  &usage_stats->max_usage_.cpi_,
                  &usage_stats->avg_usage_.cpi_,
                  &usage_stats->variance_usage_.cpi_,
                  &usage_stats->sample_count_cpi_);
    }
    if (task_resource_usage.mai_ >= 0.0) {
      UpdateStats(task_resource_usage.mai_,
                  &usage_stats->min_usage_.mai_,
                  &usage_stats->max_usage_.mai_,
                  &usage_stats->avg_usage_.mai_,
                  &usage_stats->variance_usage_.mai_,
                  &usage_stats->sample_count_mai_);
    }
  }

} // namespace sim
} // namespace firmament
