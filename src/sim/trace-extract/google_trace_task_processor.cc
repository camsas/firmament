// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
// Google resource utilization trace processor.

#include <algorithm>
#include <cstdio>
#include <limits>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>

#include "misc/map-util.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "sim/trace-extract/google_trace_task_processor.h"

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

namespace firmament {
namespace sim {

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
        LOG(ERROR) << "Failed to open trace for reading of task events.";
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
              event.job_id = job_id;
              event.task_index = task_index;
              event.event_type = task_event;
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

  TaskResourceUsage GoogleTraceTaskProcessor::BuildTaskResourceUsage(
      vector<string>& line_cols) {
    TaskResourceUsage task_resource_usage = {};
    // Set resource value to -1 if not present. We can then later not take it
    // into account when we compute the statistics.
    for (uint32_t index = 5; index < 17; index++) {
      if (!line_cols[index].compare("")) {
        line_cols[index] = "-1";
      }
    }
    task_resource_usage.mean_cpu_usage = lexical_cast<double>(line_cols[5]);
    task_resource_usage.canonical_mem_usage =
      lexical_cast<double>(line_cols[6]);
    task_resource_usage.assigned_mem_usage =
      lexical_cast<double>(line_cols[7]);
    task_resource_usage.unmapped_page_cache =
      lexical_cast<double>(line_cols[8]);
    task_resource_usage.total_page_cache = lexical_cast<double>(line_cols[9]);
    task_resource_usage.max_mem_usage = lexical_cast<double>(line_cols[10]);
    task_resource_usage.mean_disk_io_time =
      lexical_cast<double>(line_cols[11]);
    task_resource_usage.mean_local_disk_used =
      lexical_cast<double>(line_cols[12]);
    task_resource_usage.max_cpu_usage = lexical_cast<double>(line_cols[13]);
    task_resource_usage.max_disk_io_time = lexical_cast<double>(line_cols[14]);
    task_resource_usage.cpi = lexical_cast<double>(line_cols[15]);
    task_resource_usage.mai = lexical_cast<double>(line_cols[16]);
    return task_resource_usage;
  }

  void GoogleTraceTaskProcessor::ExpandTaskEvent(
      uint64_t timestamp, const TaskIdentifier& task_id, int32_t event_type,
      unordered_map<TaskIdentifier, TaskRuntime,
                    TaskIdentifierHasher>* tasks_runtime,
      unordered_map<uint64_t, string>* job_id_to_name,
      FILE* out_events_file, vector<string>& line_cols) {
    if (event_type == TASK_SCHEDULE) {
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        TaskRuntime task_runtime = {};
        task_runtime.start_time = timestamp;
        task_runtime.last_schedule_time = timestamp;
        PopulateTaskRuntime(&task_runtime, line_cols);
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      } else {
        // Update the last scheduling time for the task. Assumes that
        // the previously running instance of the task has finished/failed.
        task_runtime_ptr->last_schedule_time = timestamp;
        PopulateTaskRuntime(task_runtime_ptr, line_cols);
      }
    } else if (event_type == TASK_EVICT || event_type == TASK_FAIL ||
               event_type == TASK_KILL || event_type == TASK_LOST) {
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        // First event for this task. This means that the task was running
        // from the beginning of the trace.
        TaskRuntime task_runtime = {};
        task_runtime.start_time = 0;
        task_runtime.num_runs = 1;
        task_runtime.total_runtime = timestamp;
        PopulateTaskRuntime(&task_runtime, line_cols);
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      } else {
        // Update the runtime for the task. The failed tasks are included
        // in the runtime.
        task_runtime_ptr->num_runs++;
        task_runtime_ptr->total_runtime +=
          timestamp - task_runtime_ptr->last_schedule_time;
        PopulateTaskRuntime(task_runtime_ptr, line_cols);
      }
    } else if (event_type == TASK_FINISH) {
      string logical_job_name = (*job_id_to_name)[task_id.job_id];
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        // First event for this task.
        TaskRuntime task_runtime = {};
        task_runtime.start_time = 0;
        task_runtime.num_runs = 1;
        task_runtime.total_runtime = timestamp;
        PopulateTaskRuntime(&task_runtime, line_cols);
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
        PrintTaskRuntime(out_events_file, task_runtime, task_id,
                         logical_job_name, timestamp);
      } else {
        task_runtime_ptr->num_runs++;
        task_runtime_ptr->total_runtime +=
          timestamp - task_runtime_ptr->last_schedule_time;
        PopulateTaskRuntime(task_runtime_ptr, line_cols);
        PrintTaskRuntime(out_events_file, *task_runtime_ptr, task_id,
                         logical_job_name,
                         timestamp - task_runtime_ptr->last_schedule_time);
      }
    } else if (event_type == TASK_UPDATE_PENDING ||
               event_type == TASK_UPDATE_RUNNING) {
      TaskRuntime* task_runtime_ptr = FindOrNull(*tasks_runtime, task_id);
      if (task_runtime_ptr == NULL) {
        // First event for this task. Task has been running from the
        // beginning of the trace.
        TaskRuntime task_runtime = {};
        InsertIfNotPresent(tasks_runtime, task_id, task_runtime);
      }
    }
  }

  TaskResourceUsage GoogleTraceTaskProcessor::MinTaskUsage(
      const vector<TaskResourceUsage>& resource_usage) {
    TaskResourceUsage task_resource_min = {};
    task_resource_min.mean_cpu_usage = numeric_limits<double>::max();
    task_resource_min.canonical_mem_usage = numeric_limits<double>::max();
    task_resource_min.assigned_mem_usage = numeric_limits<double>::max();
    task_resource_min.unmapped_page_cache = numeric_limits<double>::max();
    task_resource_min.total_page_cache = numeric_limits<double>::max();
    task_resource_min.max_mem_usage = numeric_limits<double>::max();
    task_resource_min.mean_disk_io_time = numeric_limits<double>::max();
    task_resource_min.mean_local_disk_used = numeric_limits<double>::max();
    task_resource_min.max_cpu_usage = numeric_limits<double>::max();
    task_resource_min.max_disk_io_time = numeric_limits<double>::max();
    task_resource_min.cpi = numeric_limits<double>::max();
    task_resource_min.mai = numeric_limits<double>::max();
    for (vector<TaskResourceUsage>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      if (it->mean_cpu_usage >= 0.0) {
        task_resource_min.mean_cpu_usage =
          min(task_resource_min.mean_cpu_usage, it->mean_cpu_usage);
      }
      if (it->canonical_mem_usage >= 0.0) {
        task_resource_min.canonical_mem_usage =
          min(task_resource_min.canonical_mem_usage,
              it->canonical_mem_usage);
      }
      if (it->assigned_mem_usage >= 0.0) {
        task_resource_min.assigned_mem_usage =
          min(task_resource_min.assigned_mem_usage, it->assigned_mem_usage);
      }
      if (it->unmapped_page_cache >= 0.0) {
        task_resource_min.unmapped_page_cache =
          min(task_resource_min.unmapped_page_cache,
              it->unmapped_page_cache);
      }
      if (it->total_page_cache >= 0.0) {
        task_resource_min.total_page_cache =
          min(task_resource_min.total_page_cache, it->total_page_cache);
      }
      if (it->max_mem_usage >= 0.0) {
        task_resource_min.max_mem_usage =
          min(task_resource_min.max_mem_usage, it->max_mem_usage);
      }
      if (it->mean_disk_io_time >= 0.0) {
        task_resource_min.mean_disk_io_time =
          min(task_resource_min.mean_disk_io_time, it->mean_disk_io_time);
      }
      if (it->mean_local_disk_used >= 0.0) {
        task_resource_min.mean_local_disk_used =
          min(task_resource_min.mean_local_disk_used,
              it->mean_local_disk_used);
      }
      if (it->max_cpu_usage >= 0.0) {
        task_resource_min.max_cpu_usage =
          min(task_resource_min.max_cpu_usage, it->max_cpu_usage);
      }
      if (it->max_disk_io_time >= 0.0) {
        task_resource_min.max_disk_io_time =
          min(task_resource_min.max_disk_io_time, it->max_disk_io_time);
      }
      if (it->cpi >= 0.0) {
        task_resource_min.cpi = min(task_resource_min.cpi, it->cpi);
      }
      if (it->mai >= 0.0) {
        task_resource_min.mai = min(task_resource_min.mai, it->mai);
      }
    }
    if (fabs(task_resource_min.mean_cpu_usage -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.mean_cpu_usage = -1.0;
    }
    if (fabs(task_resource_min.canonical_mem_usage -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.canonical_mem_usage = -1.0;
    }
    if (fabs(task_resource_min.assigned_mem_usage -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.assigned_mem_usage = -1.0;
    }
    if (fabs(task_resource_min.unmapped_page_cache -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.unmapped_page_cache = -1.0;
    }
    if (fabs(task_resource_min.total_page_cache -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.total_page_cache = -1.0;
    }
    if (fabs(task_resource_min.max_mem_usage -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.max_mem_usage = -1.0;
    }
    if (fabs(task_resource_min.mean_disk_io_time -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.mean_disk_io_time = -1.0;
    }
    if (fabs(task_resource_min.mean_local_disk_used -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.mean_local_disk_used = -1.0;
    }
    if (fabs(task_resource_min.max_cpu_usage -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.max_cpu_usage = -1.0;
    }
    if (fabs(task_resource_min.max_disk_io_time -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.max_disk_io_time = -1.0;
    }
    if (fabs(task_resource_min.cpi -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.cpi = -1.0;
    }
    if (fabs(task_resource_min.mai -
             numeric_limits<double>::max()) < EPS) {
      task_resource_min.mai = -1.0;
    }
    return task_resource_min;
  }

  TaskResourceUsage GoogleTraceTaskProcessor::MaxTaskUsage(
      const vector<TaskResourceUsage>& resource_usage) {
    TaskResourceUsage task_resource_max = {};
    task_resource_max.mean_cpu_usage = -1.0;
    task_resource_max.canonical_mem_usage = -1.0;
    task_resource_max.assigned_mem_usage = -1.0;
    task_resource_max.unmapped_page_cache = -1.0;
    task_resource_max.total_page_cache = -1.0;
    task_resource_max.max_mem_usage = -1.0;
    task_resource_max.mean_disk_io_time = -1.0;
    task_resource_max.mean_local_disk_used = -1.0;
    task_resource_max.max_cpu_usage = -1.0;
    task_resource_max.max_disk_io_time = -1.0;
    task_resource_max.cpi = -1.0;
    task_resource_max.mai = -1.0;
    for (vector<TaskResourceUsage>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      task_resource_max.mean_cpu_usage =
        max(task_resource_max.mean_cpu_usage, it->mean_cpu_usage);
      task_resource_max.canonical_mem_usage =
        max(task_resource_max.canonical_mem_usage, it->canonical_mem_usage);
      task_resource_max.assigned_mem_usage =
        max(task_resource_max.assigned_mem_usage, it->assigned_mem_usage);
      task_resource_max.unmapped_page_cache =
        max(task_resource_max.unmapped_page_cache, it->unmapped_page_cache);
      task_resource_max.total_page_cache =
        max(task_resource_max.total_page_cache, it->total_page_cache);
      task_resource_max.max_mem_usage =
        max(task_resource_max.max_mem_usage, it->max_mem_usage);
      task_resource_max.mean_disk_io_time =
        max(task_resource_max.mean_disk_io_time, it->mean_disk_io_time);
      task_resource_max.mean_local_disk_used =
        max(task_resource_max.mean_local_disk_used,
            it->mean_local_disk_used);
      task_resource_max.max_cpu_usage =
        max(task_resource_max.max_cpu_usage, it->max_cpu_usage);
      task_resource_max.max_disk_io_time =
        max(task_resource_max.max_disk_io_time, it->max_disk_io_time);
      task_resource_max.cpi = max(task_resource_max.cpi, it->cpi);
      task_resource_max.mai = max(task_resource_max.mai, it->mai);
    }
    return task_resource_max;
  }

  TaskResourceUsage GoogleTraceTaskProcessor::AvgTaskUsage(
      const vector<TaskResourceUsage>& resource_usage) {
    TaskResourceUsage task_resource_avg = {};
    uint64_t num_mean_cpu_usage = 0;
    uint64_t num_canonical_mem_usage = 0;
    uint64_t num_assigned_mem_usage = 0;
    uint64_t num_unmapped_page_cache = 0;
    uint64_t num_total_page_cache = 0;
    uint64_t num_mean_disk_io_time = 0;
    uint64_t num_mean_local_disk_used = 0;
    uint64_t num_cpi = 0;
    uint64_t num_mai = 0;
    for (vector<TaskResourceUsage>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      // Sum the resources.
      if (it->mean_cpu_usage >= 0.0) {
        task_resource_avg.mean_cpu_usage += it->mean_cpu_usage;
        num_mean_cpu_usage++;
      }
      if (it->canonical_mem_usage >= 0.0) {
        task_resource_avg.canonical_mem_usage += it->canonical_mem_usage;
        num_canonical_mem_usage++;
      }
      if (it->assigned_mem_usage >= 0.0) {
        task_resource_avg.assigned_mem_usage += it->assigned_mem_usage;
        num_assigned_mem_usage++;
      }
      if (it->unmapped_page_cache >= 0.0) {
        task_resource_avg.unmapped_page_cache += it->unmapped_page_cache;
        num_unmapped_page_cache++;
      }
      if (it->total_page_cache >= 0.0) {
        task_resource_avg.total_page_cache += it->total_page_cache;
        num_total_page_cache++;
      }
      if (it->mean_disk_io_time >= 0.0) {
        task_resource_avg.mean_disk_io_time += it->mean_disk_io_time;
        num_mean_disk_io_time++;
      }
      if (it->mean_local_disk_used >= 0.0) {
        task_resource_avg.mean_local_disk_used += it->mean_local_disk_used;
        num_mean_local_disk_used++;
      }
      if (it->cpi >= 0.0) {
        task_resource_avg.cpi += it->cpi;
        num_cpi++;
      }
      if (it->mai >= 0.0) {
        task_resource_avg.mai += it->mai;
        num_mai++;
      }
    }
    if (num_mean_cpu_usage > 0) {
      task_resource_avg.mean_cpu_usage /= num_mean_cpu_usage;
    } else {
      task_resource_avg.mean_cpu_usage = -1.0;
    }
    if (num_canonical_mem_usage > 0) {
      task_resource_avg.canonical_mem_usage /= num_canonical_mem_usage;
    } else {
      task_resource_avg.canonical_mem_usage = -1.0;
    }
    if (num_assigned_mem_usage > 0) {
      task_resource_avg.assigned_mem_usage /= num_assigned_mem_usage;
    } else {
      task_resource_avg.assigned_mem_usage = -1.0;
    }
    if (num_unmapped_page_cache > 0) {
      task_resource_avg.unmapped_page_cache /= num_unmapped_page_cache;
    } else {
      task_resource_avg.unmapped_page_cache = -1.0;
    }
    if (num_total_page_cache > 0) {
      task_resource_avg.total_page_cache /= num_total_page_cache;
    } else {
      task_resource_avg.total_page_cache = -1.0;
    }
    if (num_mean_disk_io_time > 0) {
      task_resource_avg.mean_disk_io_time /= num_mean_disk_io_time;
    } else {
      task_resource_avg.mean_disk_io_time = -1.0;
    }
    if (num_mean_local_disk_used > 0) {
      task_resource_avg.mean_local_disk_used /= num_mean_local_disk_used;
    } else {
      task_resource_avg.mean_local_disk_used = -1.0;
    }
    if (num_cpi > 0) {
      task_resource_avg.cpi /= num_cpi;
    } else {
      task_resource_avg.cpi = -1.0;
    }
    if (num_mai > 0) {
      task_resource_avg.mai /= num_mai;
    } else {
      task_resource_avg.mai = -1.0;
    }
    return task_resource_avg;
  }

  TaskResourceUsage GoogleTraceTaskProcessor::StandardDevTaskUsage(
      const vector<TaskResourceUsage>& resource_usage) {
    TaskResourceUsage task_resource_avg = AvgTaskUsage(resource_usage);
    TaskResourceUsage task_resource_sd = {};
    uint64_t num_mean_cpu_usage = 0;
    uint64_t num_canonical_mem_usage = 0;
    uint64_t num_assigned_mem_usage = 0;
    uint64_t num_unmapped_page_cache = 0;
    uint64_t num_total_page_cache = 0;
    uint64_t num_mean_disk_io_time = 0;
    uint64_t num_mean_local_disk_used = 0;
    uint64_t num_cpi = 0;
    uint64_t num_mai = 0;
    for (vector<TaskResourceUsage>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      if (it->mean_cpu_usage >= 0.0) {
        task_resource_sd.mean_cpu_usage +=
          pow(it->mean_cpu_usage - task_resource_avg.mean_cpu_usage, 2);
        num_mean_cpu_usage++;
      }
      if (it->canonical_mem_usage >= 0.0) {
        task_resource_sd.canonical_mem_usage +=
          pow(it->canonical_mem_usage -
              task_resource_avg.canonical_mem_usage, 2);
        num_canonical_mem_usage++;
      }
      if (it->assigned_mem_usage >= 0.0) {
        task_resource_sd.assigned_mem_usage +=
          pow(it->assigned_mem_usage -
              task_resource_avg.assigned_mem_usage, 2);
        num_assigned_mem_usage++;
      }
      if (it->unmapped_page_cache >= 0.0) {
        task_resource_sd.unmapped_page_cache +=
          pow(it->unmapped_page_cache -
              task_resource_avg.unmapped_page_cache, 2);
        num_unmapped_page_cache++;
      }
      if (it->total_page_cache >= 0.0) {
        task_resource_sd.total_page_cache +=
          pow(it->total_page_cache - task_resource_avg.total_page_cache, 2);
        num_total_page_cache++;
      }
      if (it->mean_disk_io_time >= 0.0) {
        task_resource_sd.mean_disk_io_time +=
          pow(it->mean_disk_io_time -
              task_resource_avg.mean_disk_io_time, 2);
        num_mean_disk_io_time++;
      }
      if (it->mean_local_disk_used >= 0.0) {
        task_resource_sd.mean_local_disk_used +=
          pow(it->mean_local_disk_used -
              task_resource_avg.mean_local_disk_used, 2);
        num_mean_local_disk_used++;
      }
      if (it->cpi >= 0.0) {
        task_resource_sd.cpi += pow(it->cpi - task_resource_avg.cpi, 2);
        num_cpi++;
      }
      if (it->mai >= 0.0) {
        task_resource_sd.mai += pow(it->mai - task_resource_avg.mai, 2);
        num_mai++;
      }
    }
    if (num_mean_cpu_usage > 0) {
      task_resource_sd.mean_cpu_usage =
        sqrt(task_resource_sd.mean_cpu_usage / num_mean_cpu_usage);
    } else {
      task_resource_sd.mean_cpu_usage = -1.0;
    }
    if (num_canonical_mem_usage > 0) {
      task_resource_sd.canonical_mem_usage =
        sqrt(task_resource_sd.canonical_mem_usage / num_canonical_mem_usage);
    } else {
      task_resource_sd.canonical_mem_usage = -1.0;
    }
    if (num_assigned_mem_usage > 0) {
      task_resource_sd.assigned_mem_usage =
        sqrt(task_resource_sd.assigned_mem_usage / num_assigned_mem_usage);
    } else {
      task_resource_sd.assigned_mem_usage = -1.0;
    }
    if (num_unmapped_page_cache > 0) {
      task_resource_sd.unmapped_page_cache =
        sqrt(task_resource_sd.unmapped_page_cache / num_unmapped_page_cache);
    } else {
      task_resource_sd.unmapped_page_cache = -1.0;
    }
    if (num_total_page_cache > 0) {
      task_resource_sd.total_page_cache =
        sqrt(task_resource_sd.total_page_cache / num_total_page_cache);
    } else {
      task_resource_sd.total_page_cache = -1.0;
    }
    if (num_mean_disk_io_time > 0) {
      task_resource_sd.mean_disk_io_time =
        sqrt(task_resource_sd.mean_disk_io_time / num_mean_disk_io_time);
    } else {
      task_resource_sd.mean_disk_io_time = -1.0;
    }
    if (num_mean_local_disk_used > 0) {
      task_resource_sd.mean_local_disk_used =
        sqrt(task_resource_sd.mean_local_disk_used / num_mean_local_disk_used);
    } else {
      task_resource_sd.mean_local_disk_used = -1.0;
    }
    if (num_cpi > 0) {
      task_resource_sd.cpi = sqrt(task_resource_sd.cpi / num_cpi);
    } else {
      task_resource_sd.cpi = -1.0;
    }
    if (num_mai > 0) {
      task_resource_sd.mai = sqrt(task_resource_sd.mai / num_mai);
    } else {
      task_resource_sd.mai = -1.0;
    }
    return task_resource_sd;
  }

  void GoogleTraceTaskProcessor::PrintStats(
      FILE* usage_stat_file, const TaskIdentifier& task_id,
      const vector<TaskResourceUsage>& task_resource) {
    TaskResourceUsage avg_task_usage = AvgTaskUsage(task_resource);
    TaskResourceUsage min_task_usage = MinTaskUsage(task_resource);
    TaskResourceUsage max_task_usage = MaxTaskUsage(task_resource);
    TaskResourceUsage sd_task_usage = StandardDevTaskUsage(task_resource);
    fprintf(usage_stat_file,
            "%" PRId64 " %" PRId64 " %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf "
            "%lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf "
            "%lf %lf %lf %lf %lf %lf %lf %lf %lf %lf\n",
            task_id.job_id, task_id.task_index,
            min_task_usage.mean_cpu_usage, max_task_usage.mean_cpu_usage,
            avg_task_usage.mean_cpu_usage, sd_task_usage.mean_cpu_usage,
            min_task_usage.canonical_mem_usage,
            max_task_usage.canonical_mem_usage,
            avg_task_usage.canonical_mem_usage,
            sd_task_usage.canonical_mem_usage,
            min_task_usage.assigned_mem_usage,
            max_task_usage.assigned_mem_usage,
            avg_task_usage.assigned_mem_usage,
            sd_task_usage.assigned_mem_usage,
            min_task_usage.unmapped_page_cache,
            max_task_usage.unmapped_page_cache,
            avg_task_usage.unmapped_page_cache,
            sd_task_usage.unmapped_page_cache,
            min_task_usage.total_page_cache,
            max_task_usage.total_page_cache,
            avg_task_usage.total_page_cache,
            sd_task_usage.total_page_cache,
            min_task_usage.mean_disk_io_time,
            max_task_usage.mean_disk_io_time,
            avg_task_usage.mean_disk_io_time,
            sd_task_usage.mean_disk_io_time,
            min_task_usage.mean_local_disk_used,
            max_task_usage.mean_local_disk_used,
            avg_task_usage.mean_local_disk_used,
            sd_task_usage.mean_local_disk_used,
            min_task_usage.cpi, max_task_usage.cpi,
            avg_task_usage.cpi, sd_task_usage.cpi,
            min_task_usage.mai, max_task_usage.mai,
            avg_task_usage.mai, sd_task_usage.mai);
  }

  void GoogleTraceTaskProcessor::AggregateTaskUsage() {
    unordered_map<uint64_t, uint64_t>* job_num_tasks =
      new unordered_map<uint64_t, uint64_t>();
    multimap<uint64_t, TaskSchedulingEvent>& scheduling_events =
      ReadTaskStateChangingEvents(job_num_tasks);
    job_num_tasks->clear();
    delete job_num_tasks;
    // Map job id to map of task id to vector of resource usage.
    unordered_map<TaskIdentifier, vector<TaskResourceUsage>,
        TaskIdentifierHasher> task_usage;
    char line[200];
    vector<string> line_cols;
    FILE* usage_file = NULL;
    FILE* usage_stat_file = NULL;
    string usage_file_name;
    spf(&usage_file_name, "%s/task_usage_stat/task_usage_stat.csv",
        trace_path_.c_str());
    if ((usage_stat_file = fopen(usage_file_name.c_str(), "w")) == NULL) {
      LOG(ERROR) << "Failed to open task_usage_stat file for writing";
    }
    uint64_t last_timestamp = 0;
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      LOG(INFO) << "Reading task_usage file " << file_num;
      string file_name;
      spf(&file_name, "%s/task_usage/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((usage_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(ERROR) << "Failed to open trace for reading of task "
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
            cur_task_id.job_id = lexical_cast<uint64_t>(line_cols[2]);
            cur_task_id.task_index = lexical_cast<uint64_t>(line_cols[3]);
            if (last_timestamp < start_timestamp) {
              ProcessSchedulingEvents(last_timestamp, &scheduling_events,
                                      &task_usage, usage_stat_file);
            }
            last_timestamp = start_timestamp;

            TaskResourceUsage task_resource_usage =
              BuildTaskResourceUsage(line_cols);
            // Add task_resource_usage to the usage map.
            if (task_usage.find(cur_task_id) == task_usage.end()) {
              // New task identifier.
              vector<TaskResourceUsage> resource_usage;
              resource_usage.push_back(task_resource_usage);
              InsertIfNotPresent(&task_usage, cur_task_id, resource_usage);
            } else {
              task_usage[cur_task_id].push_back(task_resource_usage);
            }
          }
        }
        num_line++;
      }
      fclose(usage_file);
    }
    // Process the scheduling events up to the last timestamp.
    ProcessSchedulingEvents(last_timestamp, &scheduling_events,
                            &task_usage, usage_stat_file);
    // Write stats for tasks that are still running.
    for (unordered_map<TaskIdentifier, vector<TaskResourceUsage>,
           TaskIdentifierHasher>::iterator it = task_usage.begin();
         it != task_usage.end(); it++) {
      PrintStats(usage_stat_file, it->first, it->second);
      it->second.clear();
    }
    task_usage.clear();
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
        LOG(ERROR) << "Failed to open trace for reading of job events.";
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
    task_runtime_ptr->scheduling_class = lexical_cast<uint64_t>(cols[7]);
    task_runtime_ptr->priority = lexical_cast<uint64_t>(cols[8]);
    task_runtime_ptr->cpu_request = lexical_cast<double>(cols[9]);
    task_runtime_ptr->ram_request = lexical_cast<double>(cols[10]);
    task_runtime_ptr->disk_request = lexical_cast<double>(cols[11]);
    task_runtime_ptr->machine_constraint = lexical_cast<int32_t>(cols[12]);
  }

  void GoogleTraceTaskProcessor::PrintTaskRuntime(
      FILE* out_events_file, const TaskRuntime& task_runtime,
      const TaskIdentifier& task_id, string logical_job_name,
      uint64_t runtime) {
    fprintf(out_events_file, "%" PRId64 " %" PRId64 " %s %" PRId64 " %" PRId64
            " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %lf %lf %lf %d\n",
            task_id.job_id, task_id.task_index, logical_job_name.c_str(),
            task_runtime.start_time, task_runtime.total_runtime, runtime,
            task_runtime.num_runs, task_runtime.scheduling_class,
            task_runtime.priority, task_runtime.cpu_request,
            task_runtime.ram_request, task_runtime.disk_request,
            task_runtime.machine_constraint);
  }

  void GoogleTraceTaskProcessor::ProcessSchedulingEvents(
      uint64_t timestamp,
      multimap<uint64_t, TaskSchedulingEvent>* scheduling_events,
      unordered_map<TaskIdentifier, vector<TaskResourceUsage>,
                    TaskIdentifierHasher>* task_usage,
      FILE* usage_stat_file) {
    multimap<uint64_t, TaskSchedulingEvent>::iterator it_to =
      scheduling_events->upper_bound(timestamp);
    multimap<uint64_t, TaskSchedulingEvent>::iterator it =
      scheduling_events->begin();
    for (; it != it_to; ++it) {
      TaskSchedulingEvent evt = it->second;
      if (evt.event_type == TASK_FINISH) {
        // Print statistics for finished tasks.
        TaskIdentifier task_id;
        task_id.job_id = evt.job_id;
        task_id.task_index = evt.task_index;
        PrintStats(usage_stat_file, task_id, (*task_usage)[task_id]);
        // Free task resource usage memory.
        (*task_usage)[task_id].clear();
        task_usage->erase(task_id);
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
    FILE* out_events_file = NULL;
    string out_file_name;
    spf(&out_file_name, "%s/task_runtime_events/task_runtime_events.csv",
        trace_path_.c_str());
    if ((out_events_file = fopen(out_file_name.c_str(), "w")) == NULL) {
      LOG(ERROR) << "Failed to open task_runtime_events file for writing";
    }
    for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
         file_num++) {
      LOG(INFO) << "Reading task_events file " << file_num;
      string file_name;
      spf(&file_name, "%s/task_events/part-%05d-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((events_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(ERROR) << "Failed to open trace for reading of task events.";
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
            task_id.job_id = lexical_cast<uint64_t>(line_cols[2]);
            task_id.task_index = lexical_cast<uint64_t>(line_cols[3]);
            int32_t event_type = lexical_cast<int32_t>(line_cols[5]);
            ExpandTaskEvent(timestamp, task_id, event_type, &tasks_runtime,
                            &job_id_to_name, out_events_file, line_cols);
          }
        }
        num_line++;
      }
      fclose(events_file);
    }
    for (unordered_map<TaskIdentifier, TaskRuntime,
           TaskIdentifierHasher>::iterator it = tasks_runtime.begin();
         it != tasks_runtime.end(); ++it) {
      TaskIdentifier task_id = it->first;
      string logical_job_name =  job_id_to_name[task_id.job_id];
      uint64_t runtime = 0;
      TaskRuntime task_runtime = it->second;
      if (task_runtime.num_runs == 0) {
        runtime = end_simulation_time - task_runtime.last_schedule_time;
        task_runtime.num_runs++;
        task_runtime.total_runtime = runtime;
      } else {
        uint64_t avg_runtime =
          task_runtime.total_runtime / task_runtime.num_runs;
        runtime = end_simulation_time - task_runtime.last_schedule_time;
        // If the average runtime to failure is bigger than the current
        // runtime then we assume that the task is going to run for avg
        // runtime to failure.
        if (avg_runtime > runtime) {
          runtime = avg_runtime;
        }
        task_runtime.num_runs++;
        task_runtime.total_runtime += runtime;
      }
      PrintTaskRuntime(out_events_file, task_runtime, task_id,
                       logical_job_name, runtime);
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

    FILE* out_file = NULL;
    string out_file_name;
    spf(&out_file_name, "%s/jobs_num_tasks/jobs_num_tasks.csv",
        trace_path_.c_str());
    if ((out_file = fopen(out_file_name.c_str(), "w")) == NULL) {
      LOG(FATAL) << "Failed to open jobs_num_tasks file for writing";
    }

    for (unordered_map<uint64_t, uint64_t>::iterator
           it = job_num_tasks->begin();
         it != job_num_tasks->end(); ++it) {
      fprintf(out_file, "%" PRId64 " %" PRId64 "\n", it->first, it->second);
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

} // namespace sim
} // namespace firmament
