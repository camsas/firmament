/*
 * Poseidon
 * Copyright (c) The Poseidon Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#ifndef FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_POPULATOR_H
#define FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_POPULATOR_H

#include "base/resource_stats.pb.h"
#include "base/task_final_report.pb.h"
#include "base/task_stats.pb.h"
#include "misc/wall_time.h"
#include "scheduling/firmament_scheduler.pb.h"
#include "scheduling/knowledge_base.h"

namespace firmament {

class KnowledgeBasePopulator {
 public:
  KnowledgeBasePopulator(boost::shared_ptr<KnowledgeBase> knowledge_base);
  void PopulateTaskFinalReport(const TaskDescriptor& td,
                               TaskFinalReport* report);
 private:
  boost::shared_ptr<KnowledgeBase> knowledge_base_;
  WallTime time_manager_;
};

}  // namespace firmament
#endif  // FIRMAMENT_SCHEDULING_KNOWLEDGE_BASE_POPULATOR_H
