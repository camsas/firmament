// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
#define FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H

#include <string>

namespace firmament {

class DIMACSChange {
 public:
  virtual ~DIMACSChange() {
  }

  virtual void SetComment(string &comment) final {
  	this->comment = comment;
  }

  virtual const string GenerateChange() const {
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
