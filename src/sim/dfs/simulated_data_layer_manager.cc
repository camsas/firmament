// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/dfs/simulated_data_layer_manager.h"

#include "base/units.h"
#include "misc/map-util.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_bounded_dfs.h"
#include "sim/dfs/simulated_uniform_dfs.h"
#include "sim/google_runtime_distribution.h"

// See google_runtime_distribution.h for explanation of these defaults
DEFINE_double(simulated_quincy_runtime_factor, 0.298,
              "Runtime power law distribution: factor parameter.");
DEFINE_double(simulated_quincy_runtime_power, -0.2627,
              "Runtime power law distribution: power parameter.");
// Distributed filesystem options
DEFINE_uint64(simulated_block_size, 512, "The size of a DFS block in MB");
DEFINE_uint64(simulated_dfs_blocks_per_machine, 12288,
              "Number of blocks each machine stores. "
              "Defaults to 12288, i.e. 6 TB for 512MB blocks.");
DEFINE_uint64(simulated_dfs_replication_factor, 3,
              "The number of times each block should be replicated.");
DEFINE_string(simulated_dfs_type, "bounded", "The type of DFS to simulated. "
              "Options: uniform | bounded");

DECLARE_uint64(quincy_machines_per_rack);

namespace firmament {
namespace sim {

SimulatedDataLayerManager::SimulatedDataLayerManager(
    TraceGenerator* trace_generator)
  : trace_generator_(trace_generator), unique_rack_id_(0) {
  input_block_dist_ = new GoogleBlockDistribution();
  runtime_dist_ =
    new GoogleRuntimeDistribution(FLAGS_simulated_quincy_runtime_factor,
                                  FLAGS_simulated_quincy_runtime_power);
  if (!FLAGS_simulated_dfs_type.compare("uniform")) {
    dfs_ = new SimulatedUniformDFS(trace_generator_);
  } else if (!FLAGS_simulated_dfs_type.compare("bounded")) {
    dfs_ = new SimulatedBoundedDFS(trace_generator_);
  } else {
    LOG(FATAL) << "Unexpected simulated DFS type: " << FLAGS_simulated_dfs_type;
  }
}

SimulatedDataLayerManager::~SimulatedDataLayerManager() {
  // trace_generator_ is not owned by SimulatedDataLayerManager.
  delete input_block_dist_;
  delete runtime_dist_;
  delete dfs_;
}

EquivClass_t SimulatedDataLayerManager::AddMachine(
    const string& hostname,
    ResourceID_t machine_res_id) {
  CHECK(InsertIfNotPresent(&hostname_to_res_id_, hostname, machine_res_id));
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
  if (machines_in_rack->size() == FLAGS_quincy_machines_per_rack) {
    racks_with_spare_links_.erase(rack_ec);
  }
  CHECK(InsertIfNotPresent(&machine_to_rack_ec_, machine_res_id, rack_ec));
  dfs_->AddMachine(machine_res_id);
  return rack_ec;
}

void SimulatedDataLayerManager::GetFileLocations(
    const string& file_path, list<DataLocation>* locations) {
  CHECK_NOTNULL(locations);
  dfs_->GetFileLocations(file_path, locations);
  // TODO(ionel): Remove the following code once rack_id_ is correctly set by
  // the file system.
  for (list<DataLocation>::iterator it = locations->begin();
       it != locations->end();
       ++it) {
    EquivClass_t* rack_ec =
      FindOrNull(machine_to_rack_ec_, it->machine_res_id_);
    CHECK_NOTNULL(rack_ec);
    it->rack_id_ = *rack_ec;
  }
}

bool SimulatedDataLayerManager::RemoveMachine(const string& hostname) {
  bool rack_removed = false;
  ResourceID_t* machine_res_id = FindOrNull(hostname_to_res_id_, hostname);
  CHECK_NOTNULL(machine_res_id);
  EquivClass_t rack_ec = GetRackForMachine(*machine_res_id);
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  CHECK_NOTNULL(machines_in_rack);
  ResourceID_t res_id_tmp = *machine_res_id;
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
  machine_to_rack_ec_.erase(*machine_res_id);
  dfs_->RemoveMachine(*machine_res_id);
  hostname_to_res_id_.erase(hostname);
  return rack_removed;
}

uint64_t SimulatedDataLayerManager::AddFilesForTask(
    const TaskDescriptor& td,
    uint64_t avg_runtime,
    bool long_running_service,
    uint64_t max_machine_spread) {
  if (!long_running_service) {
    double cumulative_probability =
      runtime_dist_->ProportionShorterTasks(avg_runtime);
    uint64_t input_size = input_block_dist_->Inverse(cumulative_probability);
    uint64_t num_blocks =
      input_size / FLAGS_simulated_block_size / MB_TO_BYTES;
    // Need to increase if there was a remainder, since integer division
    // truncates.
    if ((input_size / MB_TO_BYTES) % FLAGS_simulated_block_size != 0) {
      num_blocks++;
    }
    dfs_->AddBlocksForTask(td, num_blocks, max_machine_spread);
    return num_blocks * FLAGS_simulated_block_size;
  } else {
    return 0;
  }
}

void SimulatedDataLayerManager::RemoveFilesForTask(const TaskDescriptor& td) {
  dfs_->RemoveBlocksForTask(td.uid());
}

} // namespace sim
} // namespace firmament
