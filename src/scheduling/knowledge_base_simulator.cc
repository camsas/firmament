// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of the simulator's knowledge base.

#include "base/common.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base_simulator.h"

namespace firmament {

KnowledgeBaseSimulator::KnowledgeBaseSimulator() {
}

double KnowledgeBaseSimulator::GetAvgCPIForTEC(EquivClass_t id) {
  double* avg_cpi = FindOrNull(tec_avg_cpi_, id);
  CHECK_NOTNULL(avg_cpi);
  return *avg_cpi;
}

double KnowledgeBaseSimulator::GetAvgIPMAForTEC(EquivClass_t id) {
  double* avg_ipma = FindOrNull(tec_avg_ipma_, id);
  CHECK_NOTNULL(avg_ipma);
  return *avg_ipma;
}

double KnowledgeBaseSimulator::GetAvgRuntimeForTEC(EquivClass_t id) {
  double* avg_runtime = FindOrNull(tec_avg_runtime_, id);
  if (!avg_runtime) {
    LOG(WARNING) << "Missing runtime for " << id;
    // XXX: Sample for random distribution? Or fix the missing runtimes?
    return 10000;
  } else {
    return *avg_runtime;
  }
}

double KnowledgeBaseSimulator::GetAvgMeanCpuUsage(EquivClass_t id) {
  double* avg_mean_cpu_usage = FindOrNull(tec_avg_mean_cpu_usage_, id);
  CHECK_NOTNULL(avg_mean_cpu_usage);
  return *avg_mean_cpu_usage;
}

double KnowledgeBaseSimulator::GetAvgCanonicalMemUsage(EquivClass_t id) {
  double* avg_canonical_mem_usage =
    FindOrNull(tec_avg_canonical_mem_usage_, id);
  CHECK_NOTNULL(avg_canonical_mem_usage);
  return *avg_canonical_mem_usage;
}

double KnowledgeBaseSimulator::GetAvgAssignedMemUsage(EquivClass_t id) {
  double* avg_assigned_mem_usage = FindOrNull(tec_avg_assigned_mem_usage_, id);
  CHECK_NOTNULL(avg_assigned_mem_usage);
  return *avg_assigned_mem_usage;
}

double KnowledgeBaseSimulator::GetAvgUnmappedPageCache(EquivClass_t id) {
  double* avg_unmapped_page_cache =
    FindOrNull(tec_avg_unmapped_page_cache_, id);
  CHECK_NOTNULL(avg_unmapped_page_cache);
  return *avg_unmapped_page_cache;
}

double KnowledgeBaseSimulator::GetAvgTotalPageCache(EquivClass_t id) {
  double* avg_total_page_cache =
    FindOrNull(tec_avg_total_page_cache_, id);
  CHECK_NOTNULL(avg_total_page_cache);
  return *avg_total_page_cache;
}

double KnowledgeBaseSimulator::GetAvgMeanDiskIOTime(EquivClass_t id) {
  double* avg_mean_disk_io_time =
    FindOrNull(tec_avg_mean_disk_io_time_, id);
  CHECK_NOTNULL(avg_mean_disk_io_time);
  return *avg_mean_disk_io_time;
}

double KnowledgeBaseSimulator::GetAvgMeanLocalDiskUsed(EquivClass_t id) {
  double* avg_mean_local_disk_used =
    FindOrNull(tec_avg_mean_local_disk_used_, id);
  CHECK_NOTNULL(avg_mean_local_disk_used);
  return *avg_mean_local_disk_used;
}

void KnowledgeBaseSimulator::SetAvgCPIForTEC(EquivClass_t id,
                                             double avg_cpi) {
  InsertIfNotPresent(&tec_avg_cpi_, id, avg_cpi);
}

void KnowledgeBaseSimulator::SetAvgIPMAForTEC(EquivClass_t id,
                                              double avg_ipma) {
  InsertIfNotPresent(&tec_avg_ipma_, id, avg_ipma);
}

void KnowledgeBaseSimulator::SetAvgRuntimeForTEC(EquivClass_t id,
                                                 double avg_runtime) {
  InsertIfNotPresent(&tec_avg_runtime_, id, avg_runtime);
}

void KnowledgeBaseSimulator::SetAvgMeanCpuUsage(EquivClass_t id,
                                                double avg_mean_cpu_usage) {
  InsertIfNotPresent(&tec_avg_mean_cpu_usage_, id, avg_mean_cpu_usage);
}

void KnowledgeBaseSimulator::SetAvgCanonicalMemUsage(
    EquivClass_t id, double avg_canonical_mem_usage) {
  InsertIfNotPresent(&tec_avg_canonical_mem_usage_, id,
                     avg_canonical_mem_usage);
}

void KnowledgeBaseSimulator::SetAvgAssignedMemUsage(
    EquivClass_t id, double avg_assigned_mem_usage) {
  InsertIfNotPresent(&tec_avg_assigned_mem_usage_, id,
                     avg_assigned_mem_usage);
}

void KnowledgeBaseSimulator::SetAvgUnmappedPageCache(
    EquivClass_t id, double avg_unmapped_page_cache) {
  InsertIfNotPresent(&tec_avg_unmapped_page_cache_, id,
                     avg_unmapped_page_cache);
}

void KnowledgeBaseSimulator::SetAvgTotalPageCache(
    EquivClass_t id, double avg_total_page_cache) {
  InsertIfNotPresent(&tec_avg_total_page_cache_, id,
                     avg_total_page_cache);
}

void KnowledgeBaseSimulator::SetAvgMeanDiskIOTime(
    EquivClass_t id, double avg_mean_disk_io_time) {
  InsertIfNotPresent(&tec_avg_mean_disk_io_time_, id,
                     avg_mean_disk_io_time);
}

void KnowledgeBaseSimulator::SetAvgMeanLocalDiskUsed(
    EquivClass_t id, double avg_mean_local_disk_used) {
  InsertIfNotPresent(&tec_avg_mean_local_disk_used_, id,
                     avg_mean_local_disk_used);
}

} // namespace firmament
