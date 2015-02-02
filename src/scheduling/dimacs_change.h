// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
#define FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H

namespace firmament {

  class DIMACSChange {

  public:
    virtual ~DIMACSChange() {
    };

    virtual const string GenerateChange() const = 0;

  };

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_CHANGE_H
