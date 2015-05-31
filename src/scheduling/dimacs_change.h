// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
#define FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H

#include <string>

#include "base/types.h"
#include "scheduling/dimacs_change_stats.h"

namespace firmament {

class DIMACSChange {
 public:
  virtual ~DIMACSChange() {
  }
  virtual const string& comment() const final {
    return comment_;
  }
  virtual void set_comment(const char* comment) final {
    if (comment) {
      comment_ = comment;
    }
  }
  inline const DIMACSChangeStats& stats() const {
    return stats_;
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
  DIMACSChangeStats stats_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
