// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_KNOWLEDGE_BASE_SIMULATOR_H
#define FIRMAMENT_SIM_KNOWLEDGE_BASE_SIMULATOR_H

#include "scheduling/knowledge_base.h"

namespace firmament {

class KnowledgeBaseSimulator : public KnowledgeBase {
 public:
  KnowledgeBaseSimulator();
  double GetAvgCPIForTEC(EquivClass_t id) override;
  double GetAvgIPMAForTEC(EquivClass_t id) override;
  double GetAvgRuntimeForTEC(EquivClass_t id) override;
  double GetAvgMeanCpuUsage(EquivClass_t id);
  double GetAvgCanonicalMemUsage(EquivClass_t id);
  double GetAvgAssignedMemUsage(EquivClass_t id);
  double GetAvgUnmappedPageCache(EquivClass_t id);
  double GetAvgTotalPageCache(EquivClass_t id);
  double GetAvgMeanDiskIOTime(EquivClass_t id);
  double GetAvgMeanLocalDiskUsed(EquivClass_t id);

  void SetAvgCPIForTEC(EquivClass_t id, double avg_cpi);
  void SetAvgIPMAForTEC(EquivClass_t id, double avg_ipma);
  void SetAvgRuntimeForTEC(EquivClass_t id, double avg_runtime);
  void SetAvgMeanCpuUsage(EquivClass_t id, double avg_mean_cpu_usage);
  void SetAvgCanonicalMemUsage(EquivClass_t id,
                               double avg_canonical_mem_usage);
  void SetAvgAssignedMemUsage(EquivClass_t id,
                              double avg_assigned_mem_usage);
  void SetAvgUnmappedPageCache(EquivClass_t id,
                               double avg_unmapped_page_cache);
  void SetAvgTotalPageCache(EquivClass_t id,
                            double avg_total_page_cache);
  void SetAvgMeanDiskIOTime(EquivClass_t id,
                            double avg_mean_disk_io_time);
  void SetAvgMeanLocalDiskUsed(EquivClass_t id,
                               double avg_mean_local_disk_used);

  void EraseStats(EquivClass_t id);

 private:
  unordered_map<EquivClass_t, double> tec_avg_cpi_;
  unordered_map<EquivClass_t, double> tec_avg_ipma_;
  unordered_map<EquivClass_t, double> tec_avg_runtime_;
  unordered_map<EquivClass_t, double> tec_avg_mean_cpu_usage_;
  unordered_map<EquivClass_t, double> tec_avg_canonical_mem_usage_;
  unordered_map<EquivClass_t, double> tec_avg_assigned_mem_usage_;
  unordered_map<EquivClass_t, double> tec_avg_unmapped_page_cache_;
  unordered_map<EquivClass_t, double> tec_avg_total_page_cache_;
  unordered_map<EquivClass_t, double> tec_avg_mean_disk_io_time_;
  unordered_map<EquivClass_t, double> tec_avg_mean_local_disk_used_;
};

} // namespace firmament

#endif  // FIRMAMENT_SIM_KNOWLEDGE_BASE_SIMULATOR_H
