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

#ifndef FIRMAMENT_SCHEDULING_LABEL_UTILS_H
#define FIRMAMENT_SCHEDULING_LABEL_UTILS_H

#include <unordered_map>
#include <unordered_set>

#include "base/common.h"
#include "base/label.pb.h"
#include "base/label_selector.pb.h"
#include "base/resource_desc.pb.h"

namespace firmament {
namespace scheduler {

bool SatisfiesLabelSelectors(const ResourceDescriptor& rd,
                             const RepeatedPtrField<LabelSelector>& selectors);
bool SatisfiesLabelSelector(const ResourceDescriptor& rd,
                            const LabelSelector& selector);
bool SatisfiesLabelSelector(const unordered_map<string, string>& rd_labels,
                            const LabelSelector& selector);
bool SatisfiesLabelSelector(const unordered_map<string, string>& rd_labels,
                            const unordered_set<string>& selector_values,
                            const LabelSelector& selector);
size_t HashSelectors(const RepeatedPtrField<LabelSelector>& selectors);
}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_LABEL_UTILS_H
