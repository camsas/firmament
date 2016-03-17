// Defaults from Quincy paper
DEFINE_double(simulated_quincy_delta_preferred_machine, 0.1,
              "Threshold of proportion of data stored on machine for it to be "
              "on preferred list.");
DEFINE_double(simulated_quincy_delta_preferred_rack, 0.1,
              "Threshold of proportion of data stored on rack for it to be on "
              "preferred list.");
DEFINE_uint64(simulated_quincy_tor_transfer_cost, 1,
              "Cost per unit of data transferred in core switch.");
// Cost was 2 for most experiments, 20 for constrained network experiments
DEFINE_uint64(simulated_quincy_core_transfer_cost, 2,
              "Cost per unit of data transferred in core switch.");

SimulatedQuincyCostModel* SetupSimulatedQuincyCostModel() {
  return new SimulatedQuincyCostModel(
      resource_map, job_map, task_map, leaf_res_ids,
      knowledge_base, dfs, runtime_distn, input_block_distn,
      FLAGS_simulated_quincy_delta_preferred_machine,
      FLAGS_simulated_quincy_delta_preferred_rack,
      FLAGS_simulated_quincy_core_transfer_cost,
      FLAGS_simulated_quincy_tor_transfer_cost,
      FLAGS_simulated_quincy_input_percent_over_tolerance,
      FLAGS_simulated_quincy_machines_per_rack);
}
