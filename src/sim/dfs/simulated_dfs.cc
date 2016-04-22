// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "sim/dfs/simulated_dfs.h"

namespace firmament {
namespace sim {

// Racks contain "between 29 and 31 computers" in Quincy test setup
DEFINE_uint64(machines_per_rack, 30, "Number of machines per rack");

SimulatedDFS::SimulatedDFS() : unique_rack_id_(0) {
}

EquivClass_t SimulatedDFS::AddMachine(ResourceID_t machine_res_id) {
  EquivClass_t rack_ec;
  if (racks_with_spare_links_.size() > 0) {
    // Assign the machine to a rack that has spare links.
    rack_ec = *(racks_with_spare_links_.begin());
  } else {
    // Add a new rack.
    rack_ec = unique_rack_id_;
    unique_rack_id_++;
    CHECK(InsertIfNotPresent(
        &rack_to_machine_res_, rack_ec,
        unordered_set<ResourceID_t, boost::hash<ResourceID_t>>()));
    racks_with_spare_links_.insert(rack_ec);
  }
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  CHECK_NOTNULL(machines_in_rack);
  machines_in_rack->insert(machine_res_id);
  // Erase the rack from the spare_links set if the rack is now full.
  if (machines_in_rack->size() == FLAGS_machines_per_rack) {
    racks_with_spare_links_.erase(rack_ec);
  }
  CHECK(InsertIfNotPresent(&machine_to_rack_ec_, machine_res_id, rack_ec));
  return rack_ec;
}

bool SimulatedDFS::RemoveMachine(ResourceID_t machine_res_id) {
  bool rack_removed = false;
  EquivClass_t rack_ec = GetRackForMachine(machine_res_id);
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  CHECK_NOTNULL(machines_in_rack);
  ResourceID_t res_id_tmp = machine_res_id;
  machines_in_rack->erase(res_id_tmp);
  if (machines_in_rack->size() == 0) {
    // The rack doesn't have any machines left. Delete it!
    // We have to delete empty racks because we're using the number
    // of racks to efficiently find if there's a rack on which a task has no
    // data.
    rack_to_machine_res_.erase(rack_ec);
    rack_removed = true;
  } else {
    racks_with_spare_links_.insert(rack_ec);
    rack_removed = false;
  }
  machine_to_rack_ec_.erase(machine_res_id);
  return rack_removed;
}

} // namespace sim
} // namespace firmament
