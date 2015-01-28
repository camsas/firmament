// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_CHANGE_ARC_H
#define FIRMAMENT_SCHEDULING_DIMACS_CHANGE_ARC_H

#include "base/types.h"
#include "scheduling/flow_graph_arc.h"

#include "scheduling/dimacs_change.h"

namespace firmament {

  class DIMACSChangeArc : public DIMACSChange {

  public:
  DIMACSChangeArc(const FlowGraphArc& arc): DIMACSChange(), arc_(arc) {
    }

    const string GenerateChange() const;

  private:
    const FlowGraphArc& arc_;

  };

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_CHANGE_ARC_H
