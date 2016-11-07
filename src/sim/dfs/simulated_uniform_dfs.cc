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

#include "sim/dfs/simulated_uniform_dfs.h"

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <SpookyV2.h>

#include "base/common.h"
#include "base/units.h"
#include "misc/map-util.h"

#define MAX_MACHINE_TO_SAMPLE_FOR_BLOCK_PLACEMENT 1000
#define SEED 42

DECLARE_uint64(simulated_dfs_blocks_per_machine);
DECLARE_uint64(simulated_dfs_replication_factor);
DECLARE_uint64(simulated_block_size);

namespace firmament {
namespace sim {

// justification for block parameters from Chen, et al (2012)
// blocks: 64 MB, max blocks 160 corresponds to 10 GB
SimulatedUniformDFS::SimulatedUniformDFS(TraceGenerator* trace_generator)
  : SimulatedDFS(trace_generator), rand_seed_(42) {
}

SimulatedUniformDFS::~SimulatedUniformDFS() {
  // trace_generator_ is not owned by SimulatedUniformDFS.
}

void SimulatedUniformDFS::AddBlocksForTask(const TaskDescriptor& td,
                                           uint64_t num_blocks,
                                           uint64_t max_machine_spread) {
  TaskID_t task_id = td.uid();
  for (uint64_t block_index = 0; block_index < num_blocks; ++block_index) {
    uint64_t block_id = GenerateBlockID(task_id, block_index);
    trace_generator_->AddTaskInputBlock(td, block_id);
    PlaceBlockOnMachines(task_id, block_id);
  }
}

EquivClass_t SimulatedUniformDFS::AddMachine(ResourceID_t machine_res_id) {
  CHECK(InsertIfNotPresent(&machine_num_free_blocks_, machine_res_id,
                           FLAGS_simulated_dfs_blocks_per_machine));
  CHECK(InsertIfNotPresent(&tasks_on_machine_, machine_res_id,
                           unordered_set<TaskID_t>()));
  machines_.push_back(machine_res_id);
  return SimulatedDFS::AddMachine(machine_res_id);
}

uint64_t SimulatedUniformDFS::GenerateBlockID(TaskID_t task_id,
                                              uint64_t block_index) {
  uint64_t hash = SpookyHash::Hash64(&task_id, sizeof(task_id), SEED);
  boost::hash_combine(hash, block_index);
  return hash;
}

void SimulatedUniformDFS::GetFileLocations(const string& file_path,
                                           list<DataLocation>* locations) {
  CHECK_NOTNULL(locations);
  // NOTE: we assume that each task has one input file whose path is equal
  // to the task id.
  TaskID_t task_id = boost::lexical_cast<TaskID_t>(file_path);
  pair<unordered_multimap<TaskID_t, DataLocation>::iterator,
       unordered_multimap<TaskID_t, DataLocation>::iterator> range_it =
    task_to_data_locations_.equal_range(task_id);
  for (; range_it.first != range_it.second; range_it.first++) {
    locations->push_back(range_it.first->second);
  }
}

ResourceID_t SimulatedUniformDFS::PlaceBlockOnRandomMachine() {
  ResourceID_t machine_res_id;
  uint64_t* num_free_blocks;
  // Get a machine on which to place the block. The machine must have
  // free space.
  uint64_t num_machines_selected = 0;
  do {
    uint32_t machine_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % machines_.size();
    machine_res_id = machines_[machine_index];
    num_free_blocks = FindOrNull(machine_num_free_blocks_, machine_res_id);
    CHECK_NOTNULL(num_free_blocks);
    ++num_machines_selected;
    if (num_machines_selected > MAX_MACHINE_TO_SAMPLE_FOR_BLOCK_PLACEMENT) {
      LOG(FATAL) << "There's not enough free space on the DFS";
    }
  } while (*num_free_blocks == 0);
  *num_free_blocks = *num_free_blocks - 1;
  return machine_res_id;
}

void SimulatedUniformDFS::PlaceBlockOnMachines(TaskID_t task_id,
                                               uint64_t block_id) {
  for (uint64_t replica_index = 0;
       replica_index < FLAGS_simulated_dfs_replication_factor;
       replica_index++) {
    ResourceID_t machine_res_id = PlaceBlockOnRandomMachine();
    unordered_set<TaskID_t>* tasks_machine =
      FindOrNull(tasks_on_machine_, machine_res_id);
    CHECK_NOTNULL(tasks_machine);
    tasks_machine->insert(task_id);
    DataLocation data_location(machine_res_id,
                               GetRackForMachine(machine_res_id),
                               block_id,
                               FLAGS_simulated_block_size);
    task_to_data_locations_.insert(pair<TaskID_t, DataLocation>(task_id,
                                                                data_location));
    trace_generator_->AddBlock(machine_res_id, block_id,
                               data_location.size_bytes_);
  }
}

void SimulatedUniformDFS::RemoveBlocksForTask(TaskID_t task_id) {
  pair<unordered_multimap<TaskID_t, DataLocation>::iterator,
       unordered_multimap<TaskID_t, DataLocation>::iterator> range_it =
    task_to_data_locations_.equal_range(task_id);
  for (; range_it.first != range_it.second; range_it.first++) {
    const DataLocation& data_location = range_it.first->second;
    uint64_t* num_free_blocks =
      FindOrNull(machine_num_free_blocks_, data_location.machine_res_id_);
    CHECK_NOTNULL(num_free_blocks);
    *num_free_blocks = *num_free_blocks + 1;
    unordered_set<TaskID_t>* tasks =
      FindOrNull(tasks_on_machine_, data_location.machine_res_id_);
    CHECK_NOTNULL(tasks);
    tasks->erase(task_id);
  }
  task_to_data_locations_.erase(task_id);
}

bool SimulatedUniformDFS::RemoveMachine(ResourceID_t machine_res_id) {
  ResourceID_t res_tmp = machine_res_id;
  // Remove the machine from the map of machines with storage space.
  machine_num_free_blocks_.erase(res_tmp);
  // Remove the machine from the machines vector.
  for (vector<ResourceID_t>::iterator it = machines_.begin();
       it != machines_.end(); ++it) {
    if (*it == machine_res_id) {
      // NOTE: It is fine to erase while iterating over the vector because
      // we're breaking from the iteration just after we erase the element.
      machines_.erase(it);
      break;
    }
  }
  unordered_set<TaskID_t>* tasks =
    FindOrNull(tasks_on_machine_, machine_res_id);
  CHECK_NOTNULL(tasks);
  for (auto& task_id : *tasks) {
    pair<unordered_multimap<TaskID_t, DataLocation>::iterator,
         unordered_multimap<TaskID_t, DataLocation>::iterator> range_it =
      task_to_data_locations_.equal_range(task_id);
    for (; range_it.first != range_it.second; range_it.first++) {
      const DataLocation& data_location = range_it.first->second;
      if (data_location.machine_res_id_ == machine_res_id) {
        trace_generator_->RemoveBlock(data_location.machine_res_id_,
                                      data_location.block_id_,
                                      data_location.size_bytes_);
        // Move the block to another random machine.
        ResourceID_t new_machine_res_id = PlaceBlockOnRandomMachine();
        range_it.first->second.machine_res_id_ = new_machine_res_id;
        range_it.first->second.rack_id_ = GetRackForMachine(new_machine_res_id);
        const DataLocation& new_data_location = range_it.first->second;
        unordered_set<TaskID_t>* tasks_machine =
          FindOrNull(tasks_on_machine_, new_machine_res_id);
        CHECK_NOTNULL(tasks_machine);
        tasks_machine->insert(task_id);
        trace_generator_->AddBlock(new_data_location.machine_res_id_,
                                   new_data_location.block_id_,
                                   new_data_location.size_bytes_);
      }
    }
  }
  // There are no more tasks with blocks on this machine.
  tasks_on_machine_.erase(machine_res_id);
  return SimulatedDFS::RemoveMachine(machine_res_id);
}

} // namespace sim
} // namespace firmament
