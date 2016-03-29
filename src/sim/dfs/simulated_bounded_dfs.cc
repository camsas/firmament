// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "sim/dfs/simulated_bounded_dfs.h"

#include <SpookyV2.h>

#include "base/common.h"
#include "base/units.h"

#define MACHINE_POOL_SEED 42

DECLARE_uint64(simulated_dfs_replication_factor);
DECLARE_uint64(simulated_quincy_block_size);

namespace firmament {
namespace sim {

SimulatedBoundedDFS::SimulatedBoundedDFS(TraceGenerator* trace_generator)
  : SimulatedUniformDFS(trace_generator) {
}

SimulatedBoundedDFS::~SimulatedBoundedDFS() {
  // trace_generator_ is not owned by SimulatedBoundedDFS.
}

void SimulatedBoundedDFS::AddBlocksForTask(const TaskDescriptor& td,
                                           uint64_t num_blocks,
                                           uint64_t max_machine_spread) {
  vector<ResourceID_t> machines;
  // NOTE: This is inefficient because we compute the pool for every task.
  GetJobMachinePool(td.job_id(), max_machine_spread, &machines);
  TaskID_t task_id = td.uid();
  for (uint64_t block_index = 0; block_index < num_blocks; ++block_index) {
    uint64_t block_id = GenerateBlockID(task_id, block_index);
    trace_generator_->AddTaskInputBlock(td, block_id);
    for (uint64_t replica_index = 0;
         replica_index < FLAGS_simulated_dfs_replication_factor;
         replica_index++) {
      ResourceID_t machine_res_id =
        PlaceBlockOnMachinesPool(task_id, block_id, &machines);
      unordered_set<TaskID_t>* tasks_machine =
        FindOrNull(tasks_on_machine_, machine_res_id);
      CHECK_NOTNULL(tasks_machine);
      tasks_machine->insert(task_id);
      DataLocation data_location(machine_res_id, block_id,
                                 FLAGS_simulated_quincy_block_size *
                                 MB_TO_BYTES);
      task_to_data_locations_.insert(
          pair<TaskID_t, DataLocation>(task_id, data_location));
      trace_generator_->AddBlock(machine_res_id, block_id,
                                 data_location.size_bytes_);
    }
  }
}

ResourceID_t SimulatedBoundedDFS::PlaceBlockOnMachinesPool(
    TaskID_t task_id,
    uint64_t block_id,
    vector<ResourceID_t>* machines) {
  uint32_t machine_rand_seed = 42;
  uint64_t num_machines_selected = 0;
  ResourceID_t machine_res_id;
  uint64_t* num_free_blocks;
  do {
    uint32_t machine_index = rand_r(&machine_rand_seed) % machines->size();
    num_free_blocks =
      FindOrNull(machine_num_free_blocks_, (*machines)[machine_index]);
    CHECK_NOTNULL(num_free_blocks);
    ++num_machines_selected;
    if (num_machines_selected > 2 * machines->size()) {
      // We've sampled 2 * number machines in the pool. It's time to add another
      // machine because we've likely ran out space.
      machines->push_back(PlaceBlockOnRandomMachine());
      num_machines_selected = 0;
    }
    machine_res_id = (*machines)[machine_index];
  } while (*num_free_blocks == 0);
  *num_free_blocks = *num_free_blocks - 1;
  return machine_res_id;
}

void SimulatedBoundedDFS::GetJobMachinePool(const string& job_id,
                                            uint64_t num_tasks,
                                            vector<ResourceID_t>* machines) {
  uint32_t machine_rand_seed =
    SpookyHash::Hash32(&job_id, sizeof(job_id), MACHINE_POOL_SEED);
  while (machines->size() < num_tasks) {
    size_t bucket_index = 0;
    size_t bucket_size = 0;
    while (bucket_size == 0) {
      bucket_index =
        rand_r(&machine_rand_seed) % machine_num_free_blocks_.bucket_count();
      bucket_size = machine_num_free_blocks_.bucket_size(bucket_index);
    }
    size_t index_within_bucket = rand_r(&machine_rand_seed) % bucket_size;
    auto it = machine_num_free_blocks_.begin(bucket_index);
    advance(it, index_within_bucket);
    machines->push_back(it->first);
  }
}

} // namespace sim
} // namespace firmament
