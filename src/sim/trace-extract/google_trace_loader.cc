// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#include "sim/trace-extract/google_trace_loader.h"

#include <fcntl.h>
#include <SpookyV2.h>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <cstdio>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "misc/utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

DEFINE_string(machine_tmpl_file, "../../../tests/testdata/machine_topo.pbin",
              "File specifying machine topology. (Note: the given path must be "
              "relative to the directory of the binary)");

namespace firmament {
namespace sim {

GoogleTraceLoader::GoogleTraceLoader(const string& trace_path) :
  trace_path_(trace_path) {
}

void GoogleTraceLoader::LoadJobsNumTasks(
    unordered_map<uint64_t, uint64_t>* job_num_tasks) {
  char line[200];
  vector<string> cols;
  FILE* jobs_tasks_file = NULL;
  string jobs_tasks_file_name = trace_path_ +
    "/jobs_num_tasks/jobs_num_tasks.csv";
  if ((jobs_tasks_file = fopen(jobs_tasks_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open jobs num tasks file.";
  }
  int64_t num_line = 1;
  while (!feof(jobs_tasks_file)) {
    if (fscanf(jobs_tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
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
    uint64_t max_event_id_to_retain,
    multimap<uint64_t, EventDescriptor>* events) {
  char line[200];
  vector<string> cols;
  FILE* machines_file;
  string machines_file_name = trace_path_ +
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
            max_event_id_to_retain) {
          // skip event
          continue;
        }

        EventDescriptor event_desc;
        event_desc.set_machine_id(lexical_cast<uint64_t>(cols[1]));
        event_desc.set_type(TranslateMachineEvent(
            lexical_cast<int32_t>(cols[2])));
        if (event_desc.type() == EventDescriptor::REMOVE_MACHINE ||
            event_desc.type() == EventDescriptor::ADD_MACHINE) {
          events->insert(pair<uint64_t, EventDescriptor>(timestamp,
                                                         event_desc));
        } else {
          // TODO(ionel): Handle machine update events.
        }
      }
    }
    num_line++;
  }
  fclose(machines_file);
}

void GoogleTraceLoader::LoadMachineTemplate(
    ResourceTopologyNodeDescriptor* machine_tmpl) {
  boost::filesystem::path machine_tmpl_path(FLAGS_machine_tmpl_file);
  if (machine_tmpl_path.is_relative()) {
    // lookup file relative to directory of binary, not CWD
    char binary_path[1024];
    size_t bytes = ExecutableDirectory(binary_path, sizeof(binary_path));
    CHECK(bytes < sizeof(binary_path));
    boost::filesystem::path binary_path_boost(binary_path);
    binary_path_boost.remove_filename();

    machine_tmpl_path = binary_path_boost / machine_tmpl_path;
  }

  string machine_tmpl_fname(machine_tmpl_path.string());
  LOG(INFO) << "Loading machine descriptor from " << machine_tmpl_fname;
  int fd = open(machine_tmpl_fname.c_str(), O_RDONLY);
  if (fd < 0) {
    PLOG(FATAL) << "Could not load " << machine_tmpl_fname;
  }
  machine_tmpl->ParseFromFileDescriptor(fd);
  close(fd);
}

void GoogleTraceLoader::LoadTaskUtilizationStats(
    unordered_map<TaskIdentifier, TaskStats,
      TaskIdentifierHasher>* task_id_to_stats) {
  char line[1000];
  vector<string> cols;
  FILE* usage_file = NULL;
  string usage_file_name = trace_path_ +
    "/task_usage_stat/task_usage_stat.csv";
  if ((usage_file = fopen(usage_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace task runtime stats file.";
  }
  int64_t num_line = 1;
  while (!feof(usage_file)) {
    if (fscanf(usage_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
      if (cols.size() != 38) {
        LOG(WARNING) << "Malformed task usage, " << cols.size()
                     << " != 38 columns at line " << num_line;
      } else {
        TaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);
        TaskStats task_stats;
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
    uint64_t max_event_id_to_retain,
    unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>*
      task_runtime) {
  char line[200];
  vector<string> cols;
  FILE* tasks_file = NULL;
  string tasks_file_name = trace_path_ +
    "/task_runtime_events/task_runtime_events.csv";
  if ((tasks_file = fopen(tasks_file_name.c_str(), "r")) == NULL) {
    LOG(FATAL) << "Failed to open trace runtime events file.";
  }

  int64_t num_line = 1;
  while (!feof(tasks_file)) {
    if (fscanf(tasks_file, "%[^\n]%*[\n]", &line[0]) > 0) {
      boost::split(cols, line, is_any_of(" "), token_compress_off);
      if (cols.size() != 13) {
        LOG(ERROR) << "Unexpected structure of task runtime row on line: "
                   << num_line;
      } else {
        TaskIdentifier task_id;
        task_id.job_id = lexical_cast<uint64_t>(cols[0]);
        task_id.task_index = lexical_cast<uint64_t>(cols[1]);

        // Sub-sample the trace if we only retain < 100% of tasks.
        if (SpookyHash::Hash64(&task_id, sizeof(task_id), kSeed) >
            max_event_id_to_retain) {
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

} // namespace sim
} // namespace firmament
