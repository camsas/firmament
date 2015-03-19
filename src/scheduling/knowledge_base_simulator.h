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

 private:
  unordered_map<TaskEquivClass_t, double> tec_avg_cpi_;
  unordered_map<TaskEquivClass_t, double> tec_avg_runtime_;
};

} // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_SIMULATOR_H
