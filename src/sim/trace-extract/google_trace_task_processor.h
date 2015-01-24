// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_TASK_PROCESSOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_TASK_PROCESSOR_H

#include <map>
#include <string>
#include <vector>

using namespace std;

namespace firmament {
namespace sim {

  typedef struct TaskSchedulingEvent_st {
    uint64_t job_id;
    uint64_t task_index;
    int32_t event_type;
  } TaskSchedulingEvent;

  typedef struct TaskResourceUsage_st {
    double mean_cpu_usage;
    double canonical_mem_usage;
    double assigned_mem_usage;
    double unmapped_page_cache;
    double total_page_cache;
    double max_mem_usage;
    double mean_disk_io_time;
    double mean_local_disk_used;
    double max_cpu_usage;
    double max_disk_io_time;
    double cpi;
    double mai;
  } TaskResourceUsage;

  class GoogleTraceTaskProcessor {

  public:
    explicit GoogleTraceTaskProcessor(string& trace_path);
    void Run();
    map<uint64_t, vector<TaskSchedulingEvent*> >& ReadTaskSchedulingEvents();
    TaskResourceUsage* BuildTaskResourceUsage(vector<string>& line_cols);
    TaskResourceUsage* AvgTaskUsage(vector<TaskResourceUsage*>& resource_usage);
    TaskResourceUsage* MaxTaskUsage(vector<TaskResourceUsage*>& resource_usage);
    TaskResourceUsage* MinTaskUsage(vector<TaskResourceUsage*>& resource_usage);
    TaskResourceUsage* StandardDevTaskUsage(vector<TaskResourceUsage*>& resource_usage);
    void PrintStats(FILE* usage_stat_file, uint64_t job_id, uint64_t task_index,
                    vector<TaskResourceUsage*>& task_resource);
    void AggregateTaskUsage();
    map<uint64_t, string>& ReadLogicalJobsName();
    void ExpandTaskEvents();

  private:
    string trace_path_;

  };

} // sim
} // firmament
#endif
