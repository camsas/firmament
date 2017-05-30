/*
 * Firmament
 * Copyright (c) The Firmament Authors.
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

#include "scheduling/knowledge_base_populator.h"

#include "base/units.h"

namespace firmament {

KnowledgeBasePopulator::KnowledgeBasePopulator(
    boost::shared_ptr<KnowledgeBase> knowledge_base) :
  knowledge_base_(knowledge_base) {
}

void KnowledgeBasePopulator::PopulateTaskFinalReport(const TaskDescriptor& td,
                                                     TaskFinalReport* report) {
  // TODO(ionel): Populate the other fields.
  report->set_task_id(td.uid());
  report->set_start_time(td.start_time());
  report->set_finish_time(td.finish_time());
  report->set_runtime(td.finish_time() - td.start_time());
  //report->set_instructions();
  //report->set_cycles();
  //report->set_llc_refs();
  //report->set_llc_misses();
}

}  // namespace poseidon
