// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
#define FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H

#include <string>

#include "base/types.h"

namespace firmament {

class DIMACSChange {
 public:
  virtual ~DIMACSChange() {
  }

  virtual const std::string &GetComment() const final {
  	return comment;
  }

  virtual void SetComment(const char *comment) final {
  	if (comment) {
  		this->comment = comment;
  	}
  }

  virtual const std::string GenerateChange() const {
  	if (!comment.empty()) {
  		stringstream ss;
			ss << "c " << comment << "\n";
			return ss.str();
  	} else {
  		return "";
  	}
  }
 private:
  std::string comment;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
