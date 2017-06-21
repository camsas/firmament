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

#include <boost/functional/hash.hpp>
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/label_utils.h"

namespace firmament {
namespace scheduler {

bool SatisfiesLabelSelectors(const ResourceDescriptor& rd,
                             const RepeatedPtrField<LabelSelector>& selectors) {
  unordered_map<string, string> rd_labels;
  for (const auto& label : rd.labels()) {
    InsertIfNotPresent(&rd_labels, label.key(), label.value());
  }
  for (auto& selector : selectors) {
    if (!SatisfiesLabelSelector(rd_labels, selector)) {
      return false;
    }
  }
  return true;
}

bool SatisfiesLabelSelector(const ResourceDescriptor& rd,
                            const LabelSelector& selector) {
  unordered_map<string, string> rd_labels;
  for (const auto& label : rd.labels()) {
    InsertIfNotPresent(&rd_labels, label.key(), label.value());
  }
  return SatisfiesLabelSelector(rd_labels, selector);
}

bool SatisfiesLabelSelector(const unordered_map<string, string>& rd_labels,
                            const LabelSelector& selector) {
  unordered_set<string> selector_values;
  for (const auto& value : selector.values()) {
    selector_values.insert(value);
  }
  return SatisfiesLabelSelector(rd_labels, selector_values, selector);
}

bool SatisfiesLabelSelector(const unordered_map<string, string>& rd_labels,
                            const unordered_set<string>& selector_values,
                            const LabelSelector& selector) {
  switch (selector.type()) {
    case LabelSelector::IN_SET: {
      const string* value = FindOrNull(rd_labels, selector.key());
      if (value != NULL) {
        if (selector_values.find(*value) != selector_values.end()) {
          return true;
        }
      }
      return false;
    }
    case LabelSelector::NOT_IN_SET: {
      const string* value = FindOrNull(rd_labels, selector.key());
      if (value != NULL) {
        if (selector_values.find(*value) != selector_values.end()) {
          return false;
        }
      }
      return true;
    }
    case LabelSelector::EXISTS_KEY: {
      return ContainsKey(rd_labels, selector.key());
    }
    case LabelSelector::NOT_EXISTS_KEY: {
      return !ContainsKey(rd_labels, selector.key());
    }
    default:
      LOG(FATAL) << "Unsupported selector type: " << selector.type();
  }
  return false;
}

size_t HashSelectors(const RepeatedPtrField<LabelSelector>& selectors) {
  size_t seed = 0;
  for (auto label_selector : selectors) {
    boost::hash_combine(seed, HashString(label_selector.key()));
    for (auto value : label_selector.values()) {
      boost::hash_combine(seed, HashString(value));
    }
  }
  return seed;
}

}  // namespace scheduler
}  // namespace firmament
