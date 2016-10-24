Firmament has a simulator that replays cluster traces that respect the format
of the publicly available [Google Trace](https://github.com/google/cluster-data).

Binaries are in the build/src subdirectory of the project root, and all accept
the `--helpshort` argument to show their command line options.

Before you can run a simulation you need to preprocess the trace in order
to generate task runtime and task usage statistics used by the simulator.
You can do that do that by running:

```console
$ build/src/google_trace_processor \
   --trace_path={PATH_TO_GOOGLE_STYLE_TRACE} \
   --jobs_num_tasks \
   --jobs_runtime \
   --aggregate_task_usage
```

You can run a simulation by executing:

```console
$ build/src/simulator
   --simulation=google \
   --trace_path={PATH_TO_GOOGLE_STYLE_TRACE} \
   --runtime={MAX_TRACE_TIMESTAMP_TO_SIMULATE} \
   --num_files_to_process={NUM_EVENT_FILES_IN_THE_TRACE} \
   --scheduler=flow \
   --flow_scheduling_cost_model={COST_MODEL} \
   --preemption \
   --enable_hdfs_data_locality \
   --simulated_dfs_type=bounded \
   --simulated_block_size=1073741824 \
   --max_sample_queue_size=10 \
   --solver=cs2 \
   --log_solver_stderr \
   --max_solver_runtime=100000000000 \
   --machine_tmpl_file=../../tests/testdata/mach_16pus.pbin \
   --generate_trace \
   --generated_trace_path={OUTPUT_TRACE_PATH} \
   --generate_quincy_cost_model_trace \
   --log_dir={PATH_TO_LOG_DIR} \
   --quincy_no_scheduling_delay
```

The simulator will output for analysis a trace that has the same format as the
Google trace. This trace can be used to analyse scheduler runtime or task
placements.

## Replaying synthetic traces
By default, the simulator replays Google-style input traces. If you want to
instead generate and use a synthetic trace, pass the `--simulation=synthetic`
flag, and use the flags from `src/sim/synthetic_trace_loader.cc` or adjust
the class to meet your requirements.

## Extending the simulator with other schedulers
The simulator is not limited to only using Firmament's min-cost flow scheduler.
The `--scheduler={SCHEDULER_NAME}` can be used to control the scheduler to be
used to replay the trace. Currently, we support the min-cost flow scheduler
and a simple queue-based scheduler (i.e. pass `--scheduler=simple`).

If you want to add your own scheduler then you have to follow the same approach
as used by the `src/scheduling/simple/simple_scheduler.h`. You will have to
implement the abstract methods from `src/scheduling/event_driven_scheduler.h`
and extend `src/sim/simulator_bridge.cc` to instantiate your new scheduler.

## Contact

If you would like to contact us, please send an email to firmament@camsas.org,
or create an issue on GitHub.
