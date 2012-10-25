// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Machine topology manager. This class is responsible for gathering machine
// topology information (interfacing with the hwloc libraries), and allows it to
// be queried in various convenient ways.
// It also implements export hooks that allow this information to be
// communicated to other parts of the system (e.g. coordinators).o
//
// hwloc is under BSD license, permitting use and redistribution.

#ifndef FIRMAMENT_ENGINE_TOPOLOGY_MANAGER_H
#define FIRMAMENT_ENGINE_TOPOLOGY_MANAGER_H

#include <string>

extern "C" {
#include <hwloc.h>
}

#include "base/common.h"

namespace firmament {
namespace machine {
namespace topology {

class TopologyManager {
 public:
  TopologyManager();
  void LoadAndParseTopology();
  void LoadAndParseSyntheticTopology(const string& topology_desc);
  void DebugPrintRawTopology();
  uint32_t NumProcessingUnits();
 protected:
  hwloc_topology_t topology_;
  hwloc_cpuset_t cpuset_;
  uint32_t topology_depth_;
};

}  // namespace topology
}  // namespace machine
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
