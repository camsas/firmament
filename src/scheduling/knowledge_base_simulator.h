// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_SIMULATOR_H
#define FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_SIMULATOR_H

#include "scheduling/knowledge_base.h"

namespace firmament {

class KnowledgeBaseSimulator : public KnowledgeBase {
 public:
  KnowledgeBaseSimulator();
  double GetAvgCPIForTEC(TaskEquivClass_t id);
  double GetAvgIPMAForTEC(TaskEquivClass_t id);
  double GetAvgRuntimeForTEC(TaskEquivClass_t id);
  double GetAvgMeanCpuUsage(TaskEquivClass_t id);
  double GetAvgCanonicalMemUsage(TaskEquivClass_t id);
  double GetAvgAssignedMemUsage(TaskEquivClass_t id);
  double GetAvgUnmappedPageCache(TaskEquivClass_t id);
  double GetAvgTotalPageCache(TaskEquivClass_t id);
  double GetAvgMeanDiskIOTime(TaskEquivClass_t id);
  double GetAvgMeanLocalDiskUsed(TaskEquivClass_t id);

  void SetAvgCPIForTEC(TaskEquivClass_t id, double avg_cpi);
  void SetAvgRuntimeForTEC(TaskEquivClass_t id, double avg_runtime);
  void SetAvgMeanCpuUsage(TaskEquivClass_t id, double avg_mean_cpu_usage);
  void SetAvgCanonicalMemUsage(TaskEquivClass_t id,
                               double avg_canonical_mem_usage);
  void SetAvgAssignedMemUsage(TaskEquivClass_t id,
                              double avg_assigned_mem_usage);
  void SetAvgUnmappedPageCache(TaskEquivClass_t id,
                               double avg_unmapped_page_cache);
  void SetAvgTotalPageCache(TaskEquivClass_t id,
                            double avg_total_page_cache);
  void SetAvgMeanDiskIOTime(TaskEquivClass_t id,
                            double avg_mean_disk_io_time);
  void SetAvgMeanLocalDiskUsed(TaskEquivClass_t id,
                               double avg_mean_local_disk_used);

 private:
  unordered_map<TaskEquivClass_t, double> tec_avg_cpi_;
  unordered_map<TaskEquivClass_t, double> tec_avg_runtime_;
  unordered_map<TaskEquivClass_t, double> tec_avg_mean_cpu_usage_;
  unordered_map<TaskEquivClass_t, double> tec_avg_canonical_mem_usage_;
  unordered_map<TaskEquivClass_t, double> tec_avg_assigned_mem_usage_;
  unordered_map<TaskEquivClass_t, double> tec_avg_unmapped_page_cache_;
  unordered_map<TaskEquivClass_t, double> tec_avg_total_page_cache_;
  unordered_map<TaskEquivClass_t, double> tec_avg_mean_disk_io_time_;
  unordered_map<TaskEquivClass_t, double> tec_avg_mean_local_disk_used_;
};

} // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_SIMULATOR_H
