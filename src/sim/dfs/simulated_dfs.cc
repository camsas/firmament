// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#include "sim/dfs/simulated_dfs.h"

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <SpookyV2.h>

#include "base/common.h"
#include "base/units.h"
#include "misc/map-util.h"

#define MAX_MACHINE_TO_SAMPLE_FOR_BLOCK_PLACEMENT 1000
#define SEED 42

// Distributed filesystem options
DEFINE_uint64(simulated_dfs_blocks_per_machine, 98304,
              "Number of 64 MB blocks each machine stores. "
              "Defaults to 98304, i.e. 6 TB.");
DEFINE_uint64(simulated_dfs_replication_factor, 3,
              "The number of times each block should be replicated.");

DECLARE_uint64(simulated_quincy_block_size);

namespace firmament {
namespace sim {

// justification for block parameters from Chen, et al (2012)
// blocks: 64 MB, max blocks 160 corresponds to 10 GB
SimulatedDFS::SimulatedDFS(TraceGenerator* trace_generator)
  : rand_seed_(42), trace_generator_(trace_generator) {
}

SimulatedDFS::~SimulatedDFS() {
  // trace_generator_ is not owned by SimulatedDFS.
}

void SimulatedDFS::AddBlocksForTask(const TaskDescriptor& td,
                                    uint64_t num_blocks) {
  TaskID_t task_id = td.uid();
  for (uint64_t block_index = 0; block_index < num_blocks; ++block_index) {
    uint64_t block_id = GenerateBlockID(task_id, block_index);
    trace_generator_->AddTaskInputBlock(td, block_id);
    PlaceBlockOnMachines(task_id, block_id);
  }
}

void SimulatedDFS::AddMachine(ResourceID_t machine_res_id) {
  CHECK(InsertIfNotPresent(&machine_num_free_blocks_, machine_res_id,
                           FLAGS_simulated_dfs_blocks_per_machine));
  CHECK(InsertIfNotPresent(&tasks_on_machine_, machine_res_id,
                           unordered_set<TaskID_t>()));
}

uint64_t SimulatedDFS::GenerateBlockID(TaskID_t task_id, uint64_t block_index) {
  uint64_t hash = SpookyHash::Hash64(&task_id, sizeof(task_id), SEED);
  boost::hash_combine(hash, block_index);
  return hash;
}

void SimulatedDFS::GetFileLocations(const string& file_path,
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

ResourceID_t SimulatedDFS::PlaceBlockOnRandomMachine() {
  ResourceID_t machine_res_id;
  uint64_t* num_free_blocks;
  // Get a machine on which to place the block. The machine must have
  // free space.
  uint64_t num_machines_selected = 0;
  do {
    size_t bucket_index = 0;
    size_t bucket_size = 0;
    while (bucket_size == 0) {
      bucket_index =
        rand_r(&rand_seed_) % machine_num_free_blocks_.bucket_count();
      bucket_size = machine_num_free_blocks_.bucket_size(bucket_index);
    }
    size_t index_within_bucket = rand_r(&rand_seed_) % bucket_size;
    auto it = machine_num_free_blocks_.begin(bucket_index);
    advance(it, index_within_bucket);
    machine_res_id = it->first;
    num_free_blocks = &(it->second);
    ++num_machines_selected;
    if (num_machines_selected > MAX_MACHINE_TO_SAMPLE_FOR_BLOCK_PLACEMENT) {
      LOG(FATAL) << "There's not enough free space on the DFS";
    }
  } while (*num_free_blocks == 0);
  *num_free_blocks = *num_free_blocks - 1;
  return machine_res_id;
}

void SimulatedDFS::PlaceBlockOnMachines(TaskID_t task_id, uint64_t block_id) {
  for (uint64_t replica_index = 0;
       replica_index < FLAGS_simulated_dfs_replication_factor;
       replica_index++) {
    ResourceID_t machine_res_id = PlaceBlockOnRandomMachine();
    unordered_set<TaskID_t>* tasks_machine =
      FindOrNull(tasks_on_machine_, machine_res_id);
    CHECK_NOTNULL(tasks_machine);
    tasks_machine->insert(task_id);
    DataLocation data_location(machine_res_id, block_id,
                               FLAGS_simulated_quincy_block_size * MB_TO_BYTES);
    task_to_data_locations_.insert(pair<TaskID_t, DataLocation>(task_id,
                                                                data_location));
    trace_generator_->AddBlock(machine_res_id, block_id,
                               data_location.size_bytes_);
  }
}

void SimulatedDFS::RemoveBlocksForTask(TaskID_t task_id) {
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

void SimulatedDFS::RemoveMachine(ResourceID_t machine_res_id) {
  ResourceID_t res_tmp = machine_res_id;
  // Remove the machine from the map of machines with storage space.
  machine_num_free_blocks_.erase(res_tmp);
  unordered_set<TaskID_t>* tasks =
    FindOrNull(tasks_on_machine_, machine_res_id);
  CHECK_NOTNULL(tasks);
  for (auto& task_id : *tasks) {
    pair<unordered_multimap<TaskID_t, DataLocation>::iterator,
         unordered_multimap<TaskID_t, DataLocation>::iterator> range_it =
      task_to_data_locations_.equal_range(task_id);
    for (; range_it.first != range_it.second; range_it.first++) {
      if (range_it.first->second.machine_res_id_ == machine_res_id) {
        trace_generator_->RemoveBlock(range_it.first->second.machine_res_id_,
                                      range_it.first->second.block_id_,
                                      range_it.first->second.size_bytes_);
        // Move the block to another random machine.
        range_it.first->second.machine_res_id_ = PlaceBlockOnRandomMachine();
        trace_generator_->AddBlock(range_it.first->second.machine_res_id_,
                                   range_it.first->second.block_id_,
                                   range_it.first->second.size_bytes_);
      }
    }
  }
  // There are no more tasks with blocks on this machine.
  tasks_on_machine_.erase(machine_res_id);
}

} // namespace sim
} // namespace firmament
