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

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_H

#include <string>

#include "base/types.h"

namespace firmament {

class DIMACSChange {
 public:
  virtual ~DIMACSChange() {
  }
  virtual const string& comment() const {
    return comment_;
  }
  virtual void set_comment(const char* comment) {
    if (comment) {
      comment_ = comment;
    }
  }

  const string GenerateChangeDescription() const {
    if (!comment_.empty()) {
      stringstream ss;
      ss << "c " << comment_ << "\n";
      return ss.str();
    } else {
      return "";
    }
  }

  virtual const std::string GenerateChange() const = 0;

 protected:
  string comment_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_CHANGE_H
