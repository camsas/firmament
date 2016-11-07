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

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_NEW_ARC_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_NEW_ARC_H

#include <string>

#include "base/types.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/flow_graph_arc.h"

namespace firmament {

class DIMACSNewArc : public DIMACSChange {
 public:
  explicit DIMACSNewArc(const FlowGraphArc& arc);
  const string GenerateChange() const;

  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  int64_t cost_;
  FlowGraphArcType type_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_NEW_ARC_H
