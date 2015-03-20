// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of the simulator's knowledge base.

#include "misc/map-util.h"
#include "scheduling/knowledge_base_simulator.h"

namespace firmament {

KnowledgeBaseSimulator::KnowledgeBaseSimulator() {
}

double KnowledgeBaseSimulator::GetAvgCPIForTEC(TaskEquivClass_t id) {
  double* avg_cpi = FindOrNull(tec_avg_cpi_, id);
  CHECK_NOTNULL(avg_cpi);
  return *avg_cpi;
}

double KnowledgeBaseSimulator::GetAvgIPMAForTEC(TaskEquivClass_t id) {
  // TODO(ionel): Implement!
  return 0.0;
}

double KnowledgeBaseSimulator::GetAvgRuntimeForTEC(TaskEquivClass_t id) {
  double* avg_runtime = FindOrNull(tec_avg_runtime_, id);
  CHECK_NOTNULL(avg_runtime);
  return *avg_runtime;
}

double KnowledgeBaseSimulator::GetAvgMeanCpuUsage(TaskEquivClass_t id) {
  double* avg_mean_cpu_usage = FindOrNull(tec_avg_mean_cpu_usage_, id);
  CHECK_NOTNULL(avg_mean_cpu_usage);
  return *avg_mean_cpu_usage;
}

double KnowledgeBaseSimulator::GetAvgCanonicalMemUsage(TaskEquivClass_t id) {
  double* avg_canonical_mem_usage =
    FindOrNull(tec_avg_canonical_mem_usage_, id);
  CHECK_NOTNULL(avg_canonical_mem_usage);
  return *avg_canonical_mem_usage;
}

double KnowledgeBaseSimulator::GetAvgAssignedMemUsage(TaskEquivClass_t id) {
  double* avg_assigned_mem_usage = FindOrNull(tec_avg_assigned_mem_usage_, id);
  CHECK_NOTNULL(avg_assigned_mem_usage);
  return *avg_assigned_mem_usage;
}

double KnowledgeBaseSimulator::GetAvgUnmappedPageCache(TaskEquivClass_t id) {
  double* avg_unmapped_page_cache =
    FindOrNull(tec_avg_unmapped_page_cache_, id);
  CHECK_NOTNULL(avg_unmapped_page_cache);
  return *avg_unmapped_page_cache;
}

double KnowledgeBaseSimulator::GetAvgTotalPageCache(TaskEquivClass_t id) {
  double* avg_total_page_cache =
    FindOrNull(tec_avg_total_page_cache_, id);
  CHECK_NOTNULL(avg_total_page_cache);
  return *avg_total_page_cache;
}

double KnowledgeBaseSimulator::GetAvgMeanDiskIOTime(TaskEquivClass_t id) {
  double* avg_mean_disk_io_time =
    FindOrNull(tec_avg_mean_disk_io_time_, id);
  CHECK_NOTNULL(avg_mean_disk_io_time);
  return *avg_mean_disk_io_time;
}

double KnowledgeBaseSimulator::GetAvgMeanLocalDiskUsed(TaskEquivClass_t id) {
  double* avg_mean_local_disk_used =
    FindOrNull(tec_avg_mean_local_disk_used_, id);
  CHECK_NOTNULL(avg_mean_local_disk_used);
  return *avg_mean_local_disk_used;
}

void KnowledgeBaseSimulator::SetAvgCPIForTEC(TaskEquivClass_t id,
                                             double avg_cpi) {
  CHECK(InsertIfNotPresent(&tec_avg_cpi_, id, avg_cpi));
}

void KnowledgeBaseSimulator::SetAvgRuntimeForTEC(TaskEquivClass_t id,
                                                 double avg_runtime) {
  CHECK(InsertIfNotPresent(&tec_avg_runtime_, id, avg_runtime));
}

void KnowledgeBaseSimulator::SetAvgMeanCpuUsage(TaskEquivClass_t id,
                                                double avg_mean_cpu_usage) {
  CHECK(InsertIfNotPresent(&tec_avg_mean_cpu_usage_, id, avg_mean_cpu_usage));
}

void KnowledgeBaseSimulator::SetAvgCanonicalMemUsage(
    TaskEquivClass_t id, double avg_canonical_mem_usage) {
  CHECK(InsertIfNotPresent(&tec_avg_canonical_mem_usage_, id,
                           avg_canonical_mem_usage));
}

void KnowledgeBaseSimulator::SetAvgAssignedMemUsage(
    TaskEquivClass_t id, double avg_assigned_mem_usage) {
  CHECK(InsertIfNotPresent(&tec_avg_assigned_mem_usage_, id,
                           avg_assigned_mem_usage));
}

void KnowledgeBaseSimulator::SetAvgUnmappedPageCache(
    TaskEquivClass_t id, double avg_unmapped_page_cache) {
  CHECK(InsertIfNotPresent(&tec_avg_unmapped_page_cache_, id,
                           avg_unmapped_page_cache));
}

void KnowledgeBaseSimulator::SetAvgTotalPageCache(
    TaskEquivClass_t id, double avg_total_page_cache) {
  CHECK(InsertIfNotPresent(&tec_avg_total_page_cache_, id,
                           avg_total_page_cache));
}

void KnowledgeBaseSimulator::SetAvgMeanDiskIOTime(
    TaskEquivClass_t id, double avg_mean_disk_io_time) {
  CHECK(InsertIfNotPresent(&tec_avg_mean_disk_io_time_, id,
                           avg_mean_disk_io_time));
}

void KnowledgeBaseSimulator::SetAvgMeanLocalDiskUsed(
    TaskEquivClass_t id, double avg_mean_local_disk_used) {
  CHECK(InsertIfNotPresent(&tec_avg_mean_local_disk_used_, id,
                           avg_mean_local_disk_used));
}

} // namespace firmament
