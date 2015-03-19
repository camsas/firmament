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
  return 0.0;
}

double KnowledgeBaseSimulator::GetAvgRuntimeForTEC(TaskEquivClass_t id) {
  double* avg_runtime = FindOrNull(tec_avg_runtime_, id);
  CHECK_NOTNULL(avg_runtime);
  return *avg_runtime;
}

} // namespace firmament
