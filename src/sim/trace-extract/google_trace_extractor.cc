// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Google cluster trace extractor tool.

#include <cstdio>
#include <string>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <sys/stat.h>
#include <fcntl.h>

#include "sim/trace-extract/google_trace_extractor.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/flow_graph.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"
#include "scheduling/quincy_cost_model.h"
#include "misc/utils.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

namespace firmament {
namespace sim {

#define MACHINE_TMPL_FILE "../../../tests/testdata/machine_topo.pbin"
//#define MACHINE_TMPL_FILE "/tmp/mach_test.pbin"

// TODO: support these? makes less sense when replaying an entire trace
//DEFINE_int64(num_machines, -1, "Number of machines to extract; -1 for all.");
//DEFINE_int64(num_jobs, -1, "Number of initial jobs to extract; -1 for all.");
DEFINE_int64(runtime, 0, "Time to extract data for (from start of trace, in "
             "seconds); -1 for everything.");
DEFINE_string(output_dir, "", "Directory for output flow graphs.");
//DEFINE_bool(tasks_preemption_bins, false, "Compute bins of number of preempted tasks.");
//DEFINE_int32(bin_time_duration, 10, "Bin size in seconds.");
//DEFINE_string(task_bins_output, "bins.out", "The file in which the task bins are written.");
//DEFINE_bool(gen_graph_deltas, false, "Generate dimacs delta files. One for each bin.");

GoogleTraceExtractor::GoogleTraceExtractor(string& trace_path)
					: //max_machines_(FLAGS_num_machines),
					  trace_path_(trace_path), machine_parser_(trace_path_),
						job_parser_(trace_path_),	task_parser_(trace_path_),
					  current_time_(0), num_machines_seen_(0) { }

void GoogleTraceExtractor::reset_uuid(
    ResourceTopologyNodeDescriptor* rtnd,
    const string& hostname, const string& root_uuid) {
  string new_uuid;
  if (rtnd->has_parent_id()) {
    // This is an intermediate node, so translate the parent UUID via the lookup
    // table
    const string& old_parent_id = rtnd->parent_id();
    string* new_parent_id = FindOrNull(uuid_conversion_map_, rtnd->parent_id());
    CHECK_NOTNULL(new_parent_id);
    VLOG(2) << "Resetting parent UUID for " << rtnd->resource_desc().uuid()
            << ", parent was " << old_parent_id
            << ", is now " << *new_parent_id;
    rtnd->set_parent_id(*new_parent_id);
    // Grab a new UUID for the node itself
    new_uuid = to_string(GenerateResourceID());
  } else {
    // This is the top of a machine topology, so generate a first UUID for its
    // topology based on its hostname and link it into the root
    rtnd->set_parent_id(root_uuid);
    new_uuid = to_string(GenerateRootResourceID(hostname));
  }
  VLOG(2) << "Resetting UUID for " << rtnd->resource_desc().uuid()
          << " to " << new_uuid;
  InsertOrUpdate(&uuid_conversion_map_, rtnd->resource_desc().uuid(),
                 new_uuid);
  rtnd->mutable_resource_desc()->set_uuid(new_uuid);
}

// returns timestamp of earliest unprocessed event, UINT64_MAX if no more events
uint64_t GoogleTraceExtractor::ReplayMachineEvents(
		               ResourceTopologyNodeDescriptor& root, uint64_t max_runtime) {
	const MachineEvent *machine = &machine_parser_.getMachine();
	while (machine->timestamp <= max_runtime) {
		switch (machine->event_type) {
		// TODO(adam): handle machine events
		case MachineEvent::Types::ADD:
			AddMachineToTopology(machine_tmpl_, num_machines_seen_, root);
			num_machines_seen_++;
			break;
		case MachineEvent::Types::REMOVE:
			LOG(WARNING) << "Machine remove event unsupported: " << machine->timestamp;
			break;
		case MachineEvent::Types::UPDATE:
			LOG(WARNING) << "Machine update event unsupported: " << machine->timestamp;
			break;
		default:
			LOG(FATAL) << "Unexpected event type code " << machine->event_type;
		}
		if (!machine_parser_.nextRow()) {
			// no more machine events
			return UINT64_MAX;
		}
		machine = &machine_parser_.getMachine();
	}
	// return timestamp of unprocessed event
	return machine->timestamp;
}

uint64_t GoogleTraceExtractor::ReplayJobEvents(
		               ResourceTopologyNodeDescriptor& root, uint64_t max_runtime) {
	const JobEvent* job = &job_parser_.getJob();
	while (job->timestamp <= max_runtime) {
		switch (job->event_type) {
		// TODO(adam): support transition to RUNNING, DEAD
		case JobTaskEventTypes::SUBMIT:
		{
			// transition to PENDING
			uint64_t job_id = job->job_id;
			if (jobs_.count(job_id) > 0) {
				LOG(WARNING) << "Duplicate job ID " << job_id;
			} else {
				JobDescriptor* jd = new JobDescriptor();
				PopulateJob(jd, job_id);
				VLOG(1) << "Adding job " << job_id;
				jobs_[job_id] = jd;
				//flow_graph->AddJobNodes(jd);
			}
			break;
		}
		case JobTaskEventTypes::SCHEDULE:
			// transition to RUNNING
			VLOG(2) << "Job schedule unsupported";
			break;
		case JobTaskEventTypes::EVICT:
		case JobTaskEventTypes::FAIL:
		case JobTaskEventTypes::FINISH:
		case JobTaskEventTypes::KILL:
		case JobTaskEventTypes::LOST:
			// transition to DEAD
			VLOG(2) << "Job kill/evict/etc unsupported";
			break;
		case JobTaskEventTypes::UPDATE_PENDING:
		case JobTaskEventTypes::UPDATE_RUNNING:
			// stay in same state (PENDING or RUNNING). ignore this
			VLOG(3) << "ignoring " << job->event_type;
			break;
		default:
			LOG(FATAL) << "Unexpected event type code " << job->event_type;
			break;
		}
		if (!job_parser_.nextRow()) {
			// no more machine events
			return UINT64_MAX;
		}
		job = &job_parser_.getJob();
	}
	// return timestamp of unprocessed event
	return job->timestamp;
}

uint64_t GoogleTraceExtractor::ReplayTaskEvents(
					         ResourceTopologyNodeDescriptor& root, uint64_t max_runtime) {
	const TaskEvent* task = &task_parser_.getTask();
	while (task->timestamp <= max_runtime) {
		switch (task->event_type) {
		// TODO(adam): support transition to RUNNING, DEAD
		case JobTaskEventTypes::SUBMIT:
		{
			// transition to PENDING
			uint64_t job_id = task->job_id;
			JobDescriptor* jd = FindPtrOrNull(jobs_, job_id);
			if (!jd) {
				LOG(WARNING) << "Task with unrecognized job ID " << job_id
						         << " (out of order events?)";
			} else {
				VLOG(1) << "Adding task for " << job_id;
				TaskDescriptor* root_task = jd->mutable_root_task();
				TaskDescriptor* new_task = root_task->add_spawned();
				new_task->set_uid(GenerateTaskID(*root_task));
				new_task->set_state(TaskDescriptor::RUNNABLE);
			}
			break;
		}
		case JobTaskEventTypes::SCHEDULE:
			// transition to RUNNING
			VLOG(2) << "Task schedule unsupported";
			break;
		case JobTaskEventTypes::EVICT:
		case JobTaskEventTypes::FAIL:
		case JobTaskEventTypes::FINISH:
		case JobTaskEventTypes::KILL:
		case JobTaskEventTypes::LOST:
			// transition to DEAD
			VLOG(2) << "Task kill/evict/etc unsupported";
			break;
		case JobTaskEventTypes::UPDATE_PENDING:
		case JobTaskEventTypes::UPDATE_RUNNING:
			// stay in same state (PENDING or RUNNING). ignore this
			VLOG(3) << "ignoring " << task->event_type;
			break;
		default:
			LOG(FATAL) << "Unexpected event type code " << task->event_type;
			break;
		}
		if (!task_parser_.nextRow()) {
			// no more machine events
			return UINT64_MAX;
		}
		task = &task_parser_.getTask();;
	}
	// return timestamp of unprocessed event
	return task->timestamp;
}

void GoogleTraceExtractor::ReplayTrace(ResourceTopologyNodeDescriptor& root,
						                           uint64_t max_runtime) {
	// TODO(adam): incremental output
	// TODO(adam): periodic snapshots

	// Replay{Machine,Job,Task}Events all expect there to be an event loaded
	// TODO(adam): there will be a machine event, but it will already have been
	// processed by LoadInitialMachines.
	// Currently ReplayMachineEvents does nothing, however.
	CHECK(machine_parser_.nextRow()) << "no machine events in trace";
	CHECK(job_parser_.nextRow()) << "no job events in trace";
	CHECK(task_parser_.nextRow()) << "no task events in trace";

	uint64_t next_machine_time, next_job_time, next_task_time;
	while (current_time_ <= max_runtime && current_time_ < UINT64_MAX) {
		VLOG(2) << "Current time: " << current_time_;
		// max_runtime may be UINT64_MAX, in which case 1st inequality always holds
		next_machine_time = ReplayMachineEvents(root, current_time_);
		next_job_time = ReplayJobEvents(root, current_time_);
		next_task_time = ReplayTaskEvents(root, current_time_);

		current_time_ = std::min({next_machine_time, next_job_time, next_task_time});
	}
	LOG(INFO) << "Terminating at " << current_time_;
}

void GoogleTraceExtractor::PopulateJob(JobDescriptor* jd, uint64_t job_id) {
  // XXX(malte): job_id argument is discareded and replaced by randomly
  // generated ID at the moment.
  JobID_t new_job_id = GenerateJobID();
  CHECK(InsertOrUpdate(&job_id_conversion_map_, job_id, new_job_id));
	// job ID is new
	jd->set_uuid(to_string(new_job_id));
	TaskDescriptor* rt = jd->mutable_root_task();
	string bin;
	// XXX(malte): hack, should use logical job name
	spf(&bin, "%jd", job_id);
	rt->set_binary(bin);
	rt->set_uid(GenerateRootTaskID(*jd));
	rt->set_state(TaskDescriptor::RUNNABLE);
}

void GoogleTraceExtractor::AddMachineToTopology(
    const ResourceTopologyNodeDescriptor& machine_tmpl,
    uint64_t machine_id,
    ResourceTopologyNodeDescriptor &rtn_root) {
  ResourceTopologyNodeDescriptor* child = rtn_root.add_children();
  child->CopyFrom(machine_tmpl);
  const string& root_uuid = rtn_root.resource_desc().uuid();
  char hn[100];
  sprintf(hn, "h%ju", machine_id);
  TraverseResourceProtobufTreeReturnRTND(
      child, boost::bind(&GoogleTraceExtractor::reset_uuid, this, _1,
                         string(hn), root_uuid));
  child->mutable_resource_desc()->set_friendly_name(hn);
}

ResourceTopologyNodeDescriptor&
GoogleTraceExtractor::LoadInitialTopology() {
  // Import a fictional machine resource topology
  int fd = open(MACHINE_TMPL_FILE, O_RDONLY);
  machine_tmpl_.ParseFromFileDescriptor(fd);
  close(fd);

  // Create the root node
  ResourceTopologyNodeDescriptor* rtn_root = new
      ResourceTopologyNodeDescriptor();
  ResourceID_t root_uuid = GenerateRootResourceID("XXXgoogleXXX");
  rtn_root->mutable_resource_desc()->set_uuid(to_string(root_uuid));
  LOG(INFO) << "Root res ID is " << to_string(root_uuid);
  InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                     to_string(root_uuid));

  return *rtn_root;
}

void GoogleTraceExtractor::Run() {
  LOG(INFO) << "Starting Google Trace extraction!";
  //LOG(INFO) << "Number of machines to extract: " << max_machines_;
  LOG(INFO) << "Time to extract for: " << FLAGS_runtime << " seconds.";

  VLOG(1) << "Initializing resource topology";
  ResourceTopologyNodeDescriptor& initial_resource_topology =
      LoadInitialTopology();

  uint64_t max_runtime = FLAGS_runtime >= 0 ? FLAGS_runtime : UINT64_MAX;
  ReplayTrace(initial_resource_topology, max_runtime);

  QuincyCostModel* cost_model = new QuincyCostModel();
  FlowGraph g(cost_model);
  // Add resources and job to flow graph
  g.AddResourceTopology(initial_resource_topology);
  for (unordered_map<uint64_t, JobDescriptor*>::const_iterator
  		iter = jobs_.begin();
  		iter != jobs_.end();
  		++iter) {
		VLOG(1) << "Add job with " << iter->second->root_task().spawned_size()
		<< " child tasks of root task";
		g.AddJobNodes(iter->second);
  }

  // Export initial graph
  DIMACSExporter exp;
  exp.Export(g);
  string outname = FLAGS_output_dir + "/test.dm";
  VLOG(1) << "Output written to " << outname;
  exp.Flush(outname);
}

}  // namespace sim
}  // namespace firmament
