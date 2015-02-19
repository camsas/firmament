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

DECLARE_bool(aggregate_task_usage);
DECLARE_bool(expand_task_events);
DECLARE_bool(jobs_num_tasks);

namespace firmament {
namespace sim {

  GoogleTraceTaskProcessor::GoogleTraceTaskProcessor(const string& trace_path):
    trace_path_(trace_path) {
  }

  multimap<uint64_t, TaskSchedulingEvent>&
    GoogleTraceTaskProcessor::ReadTaskSchedulingEvents(
      unordered_map<uint64_t, uint64_t>* job_num_tasks) {
    // Store the scheduling events for every timestamp.
    multimap<uint64_t, TaskSchedulingEvent> *scheduling_events =
      new multimap<uint64_t, TaskSchedulingEvent>();
    char line[200];
    vector<string> line_cols;
    FILE* events_file = NULL;
    for (uint64_t file_num = 0; file_num < 500; file_num++) {
      string file_name;
      spf(&file_name, "%s/task_events/part-%05ld-of-00500.csv",
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
    }
    return *scheduling_events;
  }

  TaskResourceUsage* GoogleTraceTaskProcessor::BuildTaskResourceUsage(
      vector<string>& line_cols) {
    TaskResourceUsage* task_resource_usage = new TaskResourceUsage();
    // Set resource value to -1 if not present. We can then later not take it
    // into account when we compute the statistics.
    for (uint32_t index = 5; index < 17; index++) {
      if (!line_cols[index].compare("")) {
        line_cols[index] = "-1";
      }
    }
    task_resource_usage->mean_cpu_usage = lexical_cast<double>(line_cols[5]);
    task_resource_usage->canonical_mem_usage =
      lexical_cast<double>(line_cols[6]);
    task_resource_usage->assigned_mem_usage =
      lexical_cast<double>(line_cols[7]);
    task_resource_usage->unmapped_page_cache =
      lexical_cast<double>(line_cols[8]);
    task_resource_usage->total_page_cache = lexical_cast<double>(line_cols[9]);
    task_resource_usage->max_mem_usage = lexical_cast<double>(line_cols[10]);
    task_resource_usage->mean_disk_io_time =
      lexical_cast<double>(line_cols[11]);
    task_resource_usage->mean_local_disk_used =
      lexical_cast<double>(line_cols[12]);
    task_resource_usage->max_cpu_usage = lexical_cast<double>(line_cols[13]);
    task_resource_usage->max_disk_io_time = lexical_cast<double>(line_cols[14]);
    task_resource_usage->cpi = lexical_cast<double>(line_cols[15]);
    task_resource_usage->mai = lexical_cast<double>(line_cols[16]);
    return task_resource_usage;
  }

  TaskResourceUsage* GoogleTraceTaskProcessor::MinTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage) {
    TaskResourceUsage* task_resource_min = new TaskResourceUsage();
    task_resource_min->mean_cpu_usage = numeric_limits<double>::max();
    task_resource_min->canonical_mem_usage = numeric_limits<double>::max();
    task_resource_min->assigned_mem_usage = numeric_limits<double>::max();
    task_resource_min->unmapped_page_cache = numeric_limits<double>::max();
    task_resource_min->total_page_cache = numeric_limits<double>::max();
    task_resource_min->max_mem_usage = numeric_limits<double>::max();
    task_resource_min->mean_disk_io_time = numeric_limits<double>::max();
    task_resource_min->mean_local_disk_used = numeric_limits<double>::max();
    task_resource_min->max_cpu_usage = numeric_limits<double>::max();
    task_resource_min->max_disk_io_time = numeric_limits<double>::max();
    task_resource_min->cpi = numeric_limits<double>::max();
    task_resource_min->mai = numeric_limits<double>::max();
    for (vector<TaskResourceUsage*>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      if ((*it)->mean_cpu_usage >= 0.0) {
        task_resource_min->mean_cpu_usage =
          min(task_resource_min->mean_cpu_usage, (*it)->mean_cpu_usage);
      }
      if ((*it)->canonical_mem_usage >= 0.0) {
        task_resource_min->canonical_mem_usage =
          min(task_resource_min->canonical_mem_usage,
              (*it)->canonical_mem_usage);
      }
      if ((*it)->assigned_mem_usage >= 0.0) {
        task_resource_min->assigned_mem_usage =
          min(task_resource_min->assigned_mem_usage, (*it)->assigned_mem_usage);
      }
      if ((*it)->unmapped_page_cache >= 0.0) {
        task_resource_min->unmapped_page_cache =
          min(task_resource_min->unmapped_page_cache,
              (*it)->unmapped_page_cache);
      }
      if ((*it)->total_page_cache >= 0.0) {
        task_resource_min->total_page_cache =
          min(task_resource_min->total_page_cache, (*it)->total_page_cache);
      }
      if ((*it)->max_mem_usage >= 0.0) {
        task_resource_min->max_mem_usage =
          min(task_resource_min->max_mem_usage, (*it)->max_mem_usage);
      }
      if ((*it)->mean_disk_io_time >= 0.0) {
        task_resource_min->mean_disk_io_time =
          min(task_resource_min->mean_disk_io_time, (*it)->mean_disk_io_time);
      }
      if ((*it)->mean_local_disk_used >= 0.0) {
        task_resource_min->mean_local_disk_used =
          min(task_resource_min->mean_local_disk_used,
              (*it)->mean_local_disk_used);
      }
      if ((*it)->max_cpu_usage >= 0.0) {
        task_resource_min->max_cpu_usage =
          min(task_resource_min->max_cpu_usage, (*it)->max_cpu_usage);
      }
      if ((*it)->max_disk_io_time >= 0.0) {
        task_resource_min->max_disk_io_time =
          min(task_resource_min->max_disk_io_time, (*it)->max_disk_io_time);
      }
      if ((*it)->cpi >= 0.0) {
        task_resource_min->cpi = min(task_resource_min->cpi, (*it)->cpi);
      }
      if ((*it)->mai >= 0.0) {
        task_resource_min->mai = min(task_resource_min->mai, (*it)->mai);
      }
    }
    if (fabs(task_resource_min->mean_cpu_usage -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->mean_cpu_usage = -1.0;
    }
    if (fabs(task_resource_min->canonical_mem_usage -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->canonical_mem_usage = -1.0;
    }
    if (fabs(task_resource_min->assigned_mem_usage -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->assigned_mem_usage = -1.0;
    }
    if (fabs(task_resource_min->unmapped_page_cache -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->unmapped_page_cache = -1.0;
    }
    if (fabs(task_resource_min->total_page_cache -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->total_page_cache = -1.0;
    }
    if (fabs(task_resource_min->max_mem_usage -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->max_mem_usage = -1.0;
    }
    if (fabs(task_resource_min->mean_disk_io_time -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->mean_disk_io_time = -1.0;
    }
    if (fabs(task_resource_min->mean_local_disk_used -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->mean_local_disk_used = -1.0;
    }
    if (fabs(task_resource_min->max_cpu_usage -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->max_cpu_usage = -1.0;
    }
    if (fabs(task_resource_min->max_disk_io_time -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->max_disk_io_time = -1.0;
    }
    if (fabs(task_resource_min->cpi -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->cpi = -1.0;
    }
    if (fabs(task_resource_min->mai -
             numeric_limits<double>::max()) < 0.00001) {
      task_resource_min->mai = -1.0;
    }
    return task_resource_min;
  }

  TaskResourceUsage* GoogleTraceTaskProcessor::MaxTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage) {
    TaskResourceUsage* task_resource_max = new TaskResourceUsage();
    task_resource_max->mean_cpu_usage = -1.0;
    task_resource_max->canonical_mem_usage = -1.0;
    task_resource_max->assigned_mem_usage = -1.0;
    task_resource_max->unmapped_page_cache = -1.0;
    task_resource_max->total_page_cache = -1.0;
    task_resource_max->max_mem_usage = -1.0;
    task_resource_max->mean_disk_io_time = -1.0;
    task_resource_max->mean_local_disk_used = -1.0;
    task_resource_max->max_cpu_usage = -1.0;
    task_resource_max->max_disk_io_time = -1.0;
    task_resource_max->cpi = -1.0;
    task_resource_max->mai = -1.0;
    for (vector<TaskResourceUsage*>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      task_resource_max->mean_cpu_usage =
        max(task_resource_max->mean_cpu_usage, (*it)->mean_cpu_usage);
      task_resource_max->canonical_mem_usage =
        max(task_resource_max->canonical_mem_usage, (*it)->canonical_mem_usage);
      task_resource_max->assigned_mem_usage =
        max(task_resource_max->assigned_mem_usage, (*it)->assigned_mem_usage);
      task_resource_max->unmapped_page_cache =
        max(task_resource_max->unmapped_page_cache, (*it)->unmapped_page_cache);
      task_resource_max->total_page_cache =
        max(task_resource_max->total_page_cache, (*it)->total_page_cache);
      task_resource_max->max_mem_usage =
        max(task_resource_max->max_mem_usage, (*it)->max_mem_usage);
      task_resource_max->mean_disk_io_time =
        max(task_resource_max->mean_disk_io_time, (*it)->mean_disk_io_time);
      task_resource_max->mean_local_disk_used =
        max(task_resource_max->mean_local_disk_used,
            (*it)->mean_local_disk_used);
      task_resource_max->max_cpu_usage =
        max(task_resource_max->max_cpu_usage, (*it)->max_cpu_usage);
      task_resource_max->max_disk_io_time =
        max(task_resource_max->max_disk_io_time, (*it)->max_disk_io_time);
      task_resource_max->cpi = max(task_resource_max->cpi, (*it)->cpi);
      task_resource_max->mai = max(task_resource_max->mai, (*it)->mai);
    }
    return task_resource_max;
  }

  TaskResourceUsage* GoogleTraceTaskProcessor::AvgTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage) {
    TaskResourceUsage* task_resource_avg = new TaskResourceUsage();
    uint64_t num_mean_cpu_usage = 0;
    uint64_t num_canonical_mem_usage = 0;
    uint64_t num_assigned_mem_usage = 0;
    uint64_t num_unmapped_page_cache = 0;
    uint64_t num_total_page_cache = 0;
    uint64_t num_mean_disk_io_time = 0;
    uint64_t num_mean_local_disk_used = 0;
    uint64_t num_cpi = 0;
    uint64_t num_mai = 0;
    for (vector<TaskResourceUsage*>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      // Sum the resources.
      if ((*it)->mean_cpu_usage >= 0.0) {
        task_resource_avg->mean_cpu_usage += (*it)->mean_cpu_usage;
        num_mean_cpu_usage++;
      }
      if ((*it)->canonical_mem_usage >= 0.0) {
        task_resource_avg->canonical_mem_usage += (*it)->canonical_mem_usage;
        num_canonical_mem_usage++;
      }
      if ((*it)->assigned_mem_usage >= 0.0) {
        task_resource_avg->assigned_mem_usage += (*it)->assigned_mem_usage;
        num_assigned_mem_usage++;
      }
      if ((*it)->unmapped_page_cache >= 0.0) {
        task_resource_avg->unmapped_page_cache += (*it)->unmapped_page_cache;
        num_unmapped_page_cache++;
      }
      if ((*it)->total_page_cache >= 0.0) {
        task_resource_avg->total_page_cache += (*it)->total_page_cache;
        num_total_page_cache++;
      }
      if ((*it)->mean_disk_io_time >= 0.0) {
        task_resource_avg->mean_disk_io_time += (*it)->mean_disk_io_time;
        num_mean_disk_io_time++;
      }
      if ((*it)->mean_local_disk_used >= 0.0) {
        task_resource_avg->mean_local_disk_used += (*it)->mean_local_disk_used;
        num_mean_local_disk_used++;
      }
      if ((*it)->cpi >= 0.0) {
        task_resource_avg->cpi += (*it)->cpi;
        num_cpi++;
      }
      if ((*it)->mai >= 0.0) {
        task_resource_avg->mai += (*it)->mai;
        num_mai++;
      }
    }
    if (num_mean_cpu_usage > 0) {
      task_resource_avg->mean_cpu_usage /= num_mean_cpu_usage;
    } else {
      task_resource_avg->mean_cpu_usage = -1.0;
    }
    if (num_canonical_mem_usage > 0) {
      task_resource_avg->canonical_mem_usage /= num_canonical_mem_usage;
    } else {
      task_resource_avg->canonical_mem_usage = -1.0;
    }
    if (num_assigned_mem_usage > 0) {
      task_resource_avg->assigned_mem_usage /= num_assigned_mem_usage;
    } else {
      task_resource_avg->assigned_mem_usage = -1.0;
    }
    if (num_unmapped_page_cache > 0) {
      task_resource_avg->unmapped_page_cache /= num_unmapped_page_cache;
    } else {
      task_resource_avg->unmapped_page_cache = -1.0;
    }
    if (num_total_page_cache > 0) {
      task_resource_avg->total_page_cache /= num_total_page_cache;
    } else {
      task_resource_avg->total_page_cache = -1.0;
    }
    if (num_mean_disk_io_time > 0) {
      task_resource_avg->mean_disk_io_time /= num_mean_disk_io_time;
    } else {
      task_resource_avg->mean_disk_io_time = -1.0;
    }
    if (num_mean_local_disk_used > 0) {
      task_resource_avg->mean_local_disk_used /= num_mean_local_disk_used;
    } else {
      task_resource_avg->mean_local_disk_used = -1.0;
    }
    if (num_cpi > 0) {
      task_resource_avg->cpi /= num_cpi;
    } else {
      task_resource_avg->cpi = -1.0;
    }
    if (num_mai > 0) {
      task_resource_avg->mai /= num_mai;
    } else {
      task_resource_avg->mai = -1.0;
    }
    return task_resource_avg;
  }

  TaskResourceUsage* GoogleTraceTaskProcessor::StandardDevTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage) {
    TaskResourceUsage* task_resource_avg = AvgTaskUsage(resource_usage);
    TaskResourceUsage* task_resource_sd = new TaskResourceUsage();
    uint64_t num_mean_cpu_usage = 0;
    uint64_t num_canonical_mem_usage = 0;
    uint64_t num_assigned_mem_usage = 0;
    uint64_t num_unmapped_page_cache = 0;
    uint64_t num_total_page_cache = 0;
    uint64_t num_mean_disk_io_time = 0;
    uint64_t num_mean_local_disk_used = 0;
    uint64_t num_cpi = 0;
    uint64_t num_mai = 0;
    for (vector<TaskResourceUsage*>::const_iterator it = resource_usage.begin();
         it != resource_usage.end(); ++it) {
      if ((*it)->mean_cpu_usage >= 0.0) {
        task_resource_sd->mean_cpu_usage +=
          pow((*it)->mean_cpu_usage - task_resource_avg->mean_cpu_usage, 2);
        num_mean_cpu_usage++;
      }
      if ((*it)->canonical_mem_usage >= 0.0) {
        task_resource_sd->canonical_mem_usage +=
          pow((*it)->canonical_mem_usage -
              task_resource_avg->canonical_mem_usage, 2);
        num_canonical_mem_usage++;
      }
      if ((*it)->assigned_mem_usage >= 0.0) {
        task_resource_sd->assigned_mem_usage +=
          pow((*it)->assigned_mem_usage -
              task_resource_avg->assigned_mem_usage, 2);
        num_assigned_mem_usage++;
      }
      if ((*it)->unmapped_page_cache >= 0.0) {
        task_resource_sd->unmapped_page_cache +=
          pow((*it)->unmapped_page_cache -
              task_resource_avg->unmapped_page_cache, 2);
        num_unmapped_page_cache++;
      }
      if ((*it)->total_page_cache >= 0.0) {
        task_resource_sd->total_page_cache +=
          pow((*it)->total_page_cache - task_resource_avg->total_page_cache, 2);
        num_total_page_cache++;
      }
      if ((*it)->mean_disk_io_time >= 0.0) {
        task_resource_sd->mean_disk_io_time +=
          pow((*it)->mean_disk_io_time -
              task_resource_avg->mean_disk_io_time, 2);
        num_mean_disk_io_time++;
      }
      if ((*it)->mean_local_disk_used >= 0.0) {
        task_resource_sd->mean_local_disk_used +=
          pow((*it)->mean_local_disk_used -
              task_resource_avg->mean_local_disk_used, 2);
        num_mean_local_disk_used++;
      }
      if ((*it)->cpi >= 0.0) {
        task_resource_sd->cpi += pow((*it)->cpi - task_resource_avg->cpi, 2);
        num_cpi++;
      }
      if ((*it)->mai >= 0.0) {
        task_resource_sd->mai += pow((*it)->mai - task_resource_avg->mai, 2);
        num_mai++;
      }
    }
    if (num_mean_cpu_usage > 0) {
      task_resource_sd->mean_cpu_usage =
        sqrt(task_resource_sd->mean_cpu_usage / num_mean_cpu_usage);
    } else {
      task_resource_sd->mean_cpu_usage = -1.0;
    }
    if (num_canonical_mem_usage > 0) {
      task_resource_sd->canonical_mem_usage =
        sqrt(task_resource_sd->canonical_mem_usage / num_canonical_mem_usage);
    } else {
      task_resource_sd->canonical_mem_usage = -1.0;
    }
    if (num_assigned_mem_usage > 0) {
      task_resource_sd->assigned_mem_usage =
        sqrt(task_resource_sd->assigned_mem_usage / num_assigned_mem_usage);
    } else {
      task_resource_sd->assigned_mem_usage = -1.0;
    }
    if (num_unmapped_page_cache > 0) {
      task_resource_sd->unmapped_page_cache =
        sqrt(task_resource_sd->unmapped_page_cache / num_unmapped_page_cache);
    } else {
      task_resource_sd->unmapped_page_cache = -1.0;
    }
    if (num_total_page_cache > 0) {
      task_resource_sd->total_page_cache =
        sqrt(task_resource_sd->total_page_cache / num_total_page_cache);
    } else {
      task_resource_sd->total_page_cache = -1.0;
    }
    if (num_mean_disk_io_time > 0) {
      task_resource_sd->mean_disk_io_time =
        sqrt(task_resource_sd->mean_disk_io_time / num_mean_disk_io_time);
    } else {
      task_resource_sd->mean_disk_io_time = -1.0;
    }
    if (num_mean_local_disk_used > 0) {
      task_resource_sd->mean_local_disk_used =
        sqrt(task_resource_sd->mean_local_disk_used / num_mean_local_disk_used);
    } else {
      task_resource_sd->mean_local_disk_used = -1.0;
    }
    if (num_cpi > 0) {
      task_resource_sd->cpi = sqrt(task_resource_sd->cpi / num_cpi);
    } else {
      task_resource_sd->cpi = -1.0;
    }
    if (num_mai > 0) {
      task_resource_sd->mai = sqrt(task_resource_sd->mai / num_mai);
    } else {
      task_resource_sd->mai = -1.0;
    }
    return task_resource_sd;
  }

  void GoogleTraceTaskProcessor::PrintStats(
      FILE* usage_stat_file, uint64_t job_id, uint64_t task_index,
      const vector<TaskResourceUsage*>& task_resource) {
    TaskResourceUsage* avg_task_usage = AvgTaskUsage(task_resource);
    TaskResourceUsage* min_task_usage = MinTaskUsage(task_resource);
    TaskResourceUsage* max_task_usage = MaxTaskUsage(task_resource);
    TaskResourceUsage* sd_task_usage = StandardDevTaskUsage(task_resource);
    fprintf(usage_stat_file,
            "%" PRId64 " %" PRId64 " %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf"
            "%lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf %lf"
            "%lf %lf %lf %lf %lf %lf %lf %lf %lf %lf\n",
            job_id, task_index,
            min_task_usage->mean_cpu_usage, max_task_usage->mean_cpu_usage,
            avg_task_usage->mean_cpu_usage, sd_task_usage->mean_cpu_usage,
            min_task_usage->canonical_mem_usage,
            max_task_usage->canonical_mem_usage,
            avg_task_usage->canonical_mem_usage,
            sd_task_usage->canonical_mem_usage,
            min_task_usage->assigned_mem_usage,
            max_task_usage->assigned_mem_usage,
            avg_task_usage->assigned_mem_usage,
            sd_task_usage->assigned_mem_usage,
            min_task_usage->unmapped_page_cache,
            max_task_usage->unmapped_page_cache,
            avg_task_usage->unmapped_page_cache,
            sd_task_usage->unmapped_page_cache,
            min_task_usage->total_page_cache,
            max_task_usage->total_page_cache,
            avg_task_usage->total_page_cache,
            sd_task_usage->total_page_cache,
            min_task_usage->mean_disk_io_time,
            max_task_usage->mean_disk_io_time,
            avg_task_usage->mean_disk_io_time,
            sd_task_usage->mean_disk_io_time,
            min_task_usage->mean_local_disk_used,
            max_task_usage->mean_local_disk_used,
            avg_task_usage->mean_local_disk_used,
            sd_task_usage->mean_local_disk_used,
            min_task_usage->cpi, max_task_usage->cpi,
            avg_task_usage->cpi, sd_task_usage->cpi,
            min_task_usage->mai, max_task_usage->mai,
            avg_task_usage->mai, sd_task_usage->mai);
    delete avg_task_usage;
    delete min_task_usage;
    delete max_task_usage;
    delete sd_task_usage;
  }

  void GoogleTraceTaskProcessor::AggregateTaskUsage() {
    unordered_map<uint64_t, uint64_t>* job_num_tasks =
      new unordered_map<uint64_t, uint64_t>();
    multimap<uint64_t, TaskSchedulingEvent>& scheduling_events =
      ReadTaskSchedulingEvents(job_num_tasks);
    job_num_tasks->clear();
    delete job_num_tasks;
    // Map job id to map of task id to vector of resource usage.
    map<uint64_t, map<uint64_t, vector<TaskResourceUsage*> > > task_usage;
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
    for (uint64_t file_num = 0; file_num < 500; file_num++) {
      string file_name;
      spf(&file_name, "%s/task_usage/part-%05ld-of-00500.csv",
          trace_path_.c_str(), file_num);
      if ((usage_file = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(ERROR) << "Failed to open trace for reading of task "
                   << "resource usage.";
      }
      int64_t num_line = 1;
      while (!feof(usage_file)) {
        if (fscanf(usage_file, "%[^\n]%*[\n]", &line[0]) > 0) {
          boost::split(line_cols, line, is_any_of(","), token_compress_off);
          if (line_cols.size() != 19) {
            LOG(ERROR) << "Unexpected structure of task usage on line "
                       << num_line << ": found " << line_cols.size()
                       << " columns.";
          } else {
            uint64_t start_timestamp = lexical_cast<uint64_t>(line_cols[0]);
            uint64_t job_id = lexical_cast<uint64_t>(line_cols[2]);
            uint64_t task_index = lexical_cast<uint64_t>(line_cols[3]);
            if (last_timestamp < start_timestamp) {
              multimap<uint64_t, TaskSchedulingEvent>::iterator it_to =
                scheduling_events.upper_bound(last_timestamp);
              multimap<uint64_t, TaskSchedulingEvent>::iterator it =
                scheduling_events.begin();
              for (; it != it_to; ++it) {
                TaskSchedulingEvent evt = it->second;
                // TODO(ionel): Which events should I handle here?
                if (evt.event_type == TASK_FINISH) {
                  uint64_t job_id = evt.job_id;
                  uint64_t task_index = evt.task_index;
                  PrintStats(usage_stat_file, job_id, task_index,
                             task_usage[job_id][task_index]);
                  // Free task resource usage memory.
                  vector<TaskResourceUsage*>::iterator res_it =
                    task_usage[job_id][task_index].begin();
                  vector<TaskResourceUsage*>::iterator end_it =
                    task_usage[job_id][task_index].end();
                  for (; res_it != end_it; res_it++) {
                    delete *res_it;
                  }
                  task_usage[job_id][task_index].clear();
                  task_usage[job_id].erase(task_index);
                  if (task_usage[job_id].empty()) {
                    task_usage.erase(job_id);
                  }
                }
              }
              // Free task scheduling events.
              scheduling_events.erase(it, it_to);
            }
            last_timestamp = start_timestamp;

            TaskResourceUsage* task_resource_usage =
              BuildTaskResourceUsage(line_cols);

            if (task_usage.find(job_id) == task_usage.end()) {
              // New job id.
              map<uint64_t, vector<TaskResourceUsage*> > task_to_usage;
              task_usage.insert(
                  pair<uint64_t, map<uint64_t, vector<TaskResourceUsage*> > >(
                      job_id, task_to_usage));
            }
            if (task_usage[job_id].find(task_index) ==
                task_usage[job_id].end()) {
              // New task index.
              vector<TaskResourceUsage*> resource_usage;
              resource_usage.push_back(task_resource_usage);
              task_usage[job_id].insert(
                  pair<uint64_t, vector<TaskResourceUsage*> >(task_index,
                                                              resource_usage));
            } else {
              task_usage[job_id][task_index].push_back(task_resource_usage);
            }
          }
        }
        num_line++;
      }
    }
    // Write stats for tasks that are still running.
    for (map<uint64_t, map<uint64_t, vector<TaskResourceUsage*> > >::iterator
           job_it = task_usage.begin();
         job_it != task_usage.end(); job_it++) {
      for (map<uint64_t, vector<TaskResourceUsage*> >::iterator
             task_it = job_it->second.begin();
           task_it != job_it->second.end(); task_it++) {
        PrintStats(usage_stat_file, job_it->first, task_it->first,
                   task_it->second);
      }
    }
    // TODO(ionel): Free the memory occupied by the jobs running at the end
    // of the trace.
    fclose(usage_stat_file);
  }

  // Returns a mapping job id to logical job name.
  map<uint64_t, string>& GoogleTraceTaskProcessor::ReadLogicalJobsName() {
    map<uint64_t, string> *job_id_to_name = new map<uint64_t, string>();
    char line[200];
    vector<string> line_cols;
    FILE* events_file = NULL;
    for (uint64_t file_num = 0; file_num < 500; file_num++) {
      string file_name;
      spf(&file_name, "%s/job_events/part-%05ld-of-00500.csv",
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
            job_id_to_name->insert(pair<uint64_t, string>(job_id,
                                                          line_cols[7]));
          }
        }
        num_line++;
      }
    }
    return *job_id_to_name;
  }

  void GoogleTraceTaskProcessor::PrintTaskRuntime(
      FILE* out_events_file, TaskRuntime* task_runtime, uint64_t job_id,
      uint64_t task_index, string logical_job_name, uint64_t runtime,
      vector<string>& cols) {
    for (uint32_t index = 7; index < 13; ++index) {
      if (!cols[index].compare("")) {
        cols[index] = "-1";
      }
    }
    int64_t scheduling_class = lexical_cast<uint64_t>(cols[7]);
    int64_t priority = lexical_cast<uint64_t>(cols[8]);
    double cpu_request = lexical_cast<double>(cols[9]);
    double ram_request = lexical_cast<double>(cols[10]);
    double disk_request = lexical_cast<double>(cols[11]);
    int32_t machine_constraint = lexical_cast<int32_t>(cols[12]);
    fprintf(out_events_file, "%" PRId64 " %" PRId64 " %s %" PRId64 " %" PRId64
            " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %lf %lf %lf %d\n",
            job_id, task_index, logical_job_name.c_str(),
            task_runtime->start_time, task_runtime->total_runtime, runtime,
            task_runtime->num_runs, scheduling_class, priority, cpu_request,
            ram_request, disk_request, machine_constraint);
  }

  void GoogleTraceTaskProcessor::ExpandTaskEvents() {
    map<uint64_t, string> job_id_to_name = ReadLogicalJobsName();
    map<uint64_t, map<uint64_t, TaskRuntime*> > tasks_runtime;
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
    for (uint64_t file_num = 0; file_num < 500; file_num++) {
      string file_name;
      spf(&file_name, "%s/task_events/part-%05ld-of-00500.csv",
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
            int32_t event_type = lexical_cast<int32_t>(line_cols[5]);
            if (tasks_runtime.find(job_id) == tasks_runtime.end()) {
              // New job id.
              map<uint64_t, TaskRuntime*> task_runtime;
              tasks_runtime.insert(
                  pair<uint64_t, map<uint64_t, TaskRuntime*> >(job_id,
                                                               task_runtime));
            }
            if (event_type == TASK_SCHEDULE) {
              if (tasks_runtime[job_id].find(task_index) ==
                  tasks_runtime[job_id].end()) {
                // New task.
                TaskRuntime* task_runtime = new TaskRuntime();
                task_runtime->start_time = timestamp;
                task_runtime->last_schedule_time = timestamp;
                tasks_runtime[job_id].insert(
                    pair<uint64_t, TaskRuntime*>(task_index, task_runtime));
              } else {
                tasks_runtime[job_id][task_index]->last_schedule_time =
                  timestamp;
              }
            } else if (event_type == TASK_EVICT || event_type == TASK_FAIL ||
                       event_type == TASK_KILL) {
              if (tasks_runtime[job_id].find(task_index) ==
                  tasks_runtime[job_id].end()) {
                // First event for this task.
                TaskRuntime* task_runtime = new TaskRuntime();
                task_runtime->start_time = 0;
                task_runtime->num_runs = 1;
                task_runtime->total_runtime = timestamp;
                tasks_runtime[job_id].insert(
                    pair<uint64_t, TaskRuntime*>(task_index, task_runtime));
              } else {
                TaskRuntime* task_runtime = tasks_runtime[job_id][task_index];
                task_runtime->num_runs++;
                task_runtime->total_runtime +=
                  timestamp - task_runtime->last_schedule_time;
              }
            } else if (event_type == TASK_FINISH) {
              string logical_job_name = job_id_to_name[job_id];
              if (tasks_runtime[job_id].find(task_index) ==
                  tasks_runtime[job_id].end()) {
                // First event for this task.
                TaskRuntime* task_runtime = new TaskRuntime();
                task_runtime->start_time = 0;
                task_runtime->num_runs = 1;
                task_runtime->total_runtime = timestamp;
                tasks_runtime[job_id].insert(
                    pair<uint64_t, TaskRuntime*>(task_index, task_runtime));
                PrintTaskRuntime(out_events_file, task_runtime, job_id,
                                 task_index, logical_job_name, timestamp,
                                 line_cols);
              } else {
                TaskRuntime* task_runtime = tasks_runtime[job_id][task_index];
                task_runtime->num_runs++;
                task_runtime->total_runtime +=
                  timestamp - task_runtime->last_schedule_time;
                PrintTaskRuntime(out_events_file, task_runtime,
                                 job_id, task_index,
                                 logical_job_name,
                                 timestamp - task_runtime->last_schedule_time,
                                 line_cols);
              }
            }
          }
        }
        num_line++;
      }
    }
    // TODO(ionel): Free memory.
    fclose(out_events_file);
  }

  void GoogleTraceTaskProcessor::JobsNumTasks() {
    unordered_map<uint64_t, uint64_t>* job_num_tasks =
      new unordered_map<uint64_t, uint64_t>();
    multimap<uint64_t, TaskSchedulingEvent>& scheduling_events =
      ReadTaskSchedulingEvents(job_num_tasks);

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
    if (FLAGS_aggregate_task_usage) {
      AggregateTaskUsage();
    }
    if (FLAGS_expand_task_events) {
      ExpandTaskEvents();
    }
    if (FLAGS_jobs_num_tasks) {
      JobsNumTasks();
    }
  }

} // namespace sim
} // namespace firmament
