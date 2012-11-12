// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class implementation. This is subclassed by
// the platform-specific coordinator classes.

#include "engine/coordinator.h"

#include <string>
#include <utility>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#endif

#include "base/resource_desc.pb.h"
#include "engine/stub_object_store.h"
#include "messages/base_message.pb.h"
#include "misc/protobuf_envelope.h"
#include "misc/map-util.h"
#include "misc/utils.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");
DEFINE_string(listen_uri, "tcp://localhost:9998",
              "The name/address/port to listen on.");
#ifdef __HTTP_UI__
DEFINE_bool(http_ui, true, "Enable HTTP interface");
DEFINE_int32(http_ui_port, 8080,
             "The port that the HTTP UI will be served on; -1 to disable.");
#endif

namespace firmament {

using store::StubObjectStore;

// Initial value of exit_ toggle
bool Coordinator::exit_ = false;

Coordinator::Coordinator(PlatformID platform_id)
  : platform_id_(platform_id),
    topology_manager_(new TopologyManager()),
    associated_resources_(new ResourceMap_t),
    job_table_(new JobMap_t),
    object_table_(new DataObjectMap_t),
    uuid_(GenerateUUID()),
    scheduler_(new SimpleScheduler(job_table_, associated_resources_,
                                   FLAGS_listen_uri)),
    object_store_(new StubObjectStore) {
  // Start up a coordinator ccording to the platform parameter
  // platform_ = platform::GetByID(platform_id);
  string desc_name = "";  // platform_.GetDescriptiveName();
  resource_desc_.set_uuid(to_string(uuid_));

  // Log information
  LOG(INFO) << "Coordinator starting on host " << FLAGS_listen_uri
            << ", platform " << platform_id << ", uuid " << uuid_;

  switch (platform_id) {
    case PL_UNIX: {
      m_adapter_.reset(
          new platform_unix::streamsockets::
          StreamSocketsAdapter<BaseMessage>());
        SignalHandler handler;
        handler.ConfigureSignal(SIGINT, Coordinator::HandleSignal, this);
        handler.ConfigureSignal(SIGTERM, Coordinator::HandleSignal, this);
      break;
    }
    default:
      LOG(FATAL) << "Unimplemented!";
  }
}


Coordinator::~Coordinator() {
  // TODO(malte): check destruction order in C++; c_http_ui_ may already have
  // been destructed when we get here.
/*#ifdef __HTTP_UI__
  if (FLAGS_http_ui && c_http_ui_)
    c_http_ui_->Shutdown(false);
#endif*/
}

void Coordinator::DetectLocalResources() {
  // TODO(malte): This is somewhat of a hack currently; instead of taking a
  // flat vector of resources, we obviuosly want to take a proper topology.
  uint64_t num_local_pus = topology_manager_->NumProcessingUnits();
  LOG(INFO) << "Found " << num_local_pus << " local PUs.";
  vector<ResourceDescriptor> local_resources =
      topology_manager_->FlatResourceSet();
  uint32_t i = 0;
  for (vector<ResourceDescriptor>::iterator lr_iter = local_resources.begin();
       lr_iter != local_resources.end();
       ++lr_iter) {
    CHECK(InsertIfNotPresent(associated_resources_.get(),
                             JobIDFromString(lr_iter->uuid()),
                             pair<ResourceDescriptor, uint64_t>(
                                 *lr_iter, GetCurrentTimestamp())));
    VLOG(1) << "Added local resource " << lr_iter->uuid() << " (PU #" << i
            << "): " << lr_iter->DebugString();
    ++i;
  }
}

void Coordinator::Run() {
  // Test topology detection
  LOG(INFO) << "Detecting resource topology:";
  topology_manager_->DebugPrintRawTopology();
  DetectLocalResources();

  // Coordinator starting -- set up and wait for workers to connect.
  m_adapter_->Listen(FLAGS_listen_uri);
  m_adapter_->RegisterAsyncMessageReceiptCallback(
      boost::bind(&Coordinator::HandleIncomingMessage, this, _1));
  m_adapter_->RegisterAsyncErrorPathCallback(
      boost::bind(&Coordinator::HandleIncomingReceiveError, this,
                  boost::asio::placeholders::error, _2));

#ifdef __HTTP_UI__
  InitHTTPUI();
#endif

  // Main loop
  while (!exit_) {
    // Wait for events (i.e. messages from workers.
    // TODO(malte): we need to think about any actions that the coordinator
    // itself might need to take, and how they can be triggered
    VLOG(3) << "Hello from main loop!";
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
  Shutdown("dropped out of main loop");
}

void Coordinator::AwaitNextMessage() {
  VLOG(3) << "Waiting for next message from adapter...";
  m_adapter_->AwaitNextMessage();
  boost::this_thread::sleep(boost::posix_time::seconds(1));
}

void Coordinator::HandleRecv(const boost::system::error_code& error,
                             size_t bytes_transferred,
                             Envelope<BaseMessage>* env) {
  if (error) {
    LOG(WARNING) << "Asynchronous receive call returned an error: "
                 << error.message();
    return;
  }
  VLOG(3) << "Received " << bytes_transferred << " bytes asynchronously, "
          << "in envelope at " << env << ", representing message " << *env;
  BaseMessage *bm = env->data();
  HandleIncomingMessage(bm);
  delete env;
}


const JobDescriptor* Coordinator::DescriptorForJob(const string& job_id) {
  JobID_t job_uuid = JobIDFromString(job_id);
  JobDescriptor *jd = FindOrNull(*job_table_, job_uuid);
  return jd;
}

void Coordinator::HandleIncomingMessage(BaseMessage *bm) {
  // Registration message
  if (bm->HasExtension(register_extn)) {
    const RegistrationMessage& msg = bm->GetExtension(register_extn);
    HandleRegistrationRequest(msg);
  }
  // Heartbeat message
  if (bm->HasExtension(heartbeat_extn)) {
    const HeartbeatMessage& msg = bm->GetExtension(heartbeat_extn);
    HandleHeartbeat(msg);
  }
  if (bm->HasExtension(task_state_extn)) {
    const TaskStateMessage& msg = bm->GetExtension(task_state_extn);
    HandleTaskStateChange(msg);
  }
}

void Coordinator::HandleIncomingReceiveError(
    const boost::system::error_code& error,
    const string& remote_endpoint) {
  // Notify of receive error
  // TODO(malte): since we are taking no arguments, we actually don't have the
  // faintest idea what message this error relates to. We should try to remedy
  // this; however, it is not trivial, since we destroy the channel before
  // making the callback (and we do not want to have the callback on the
  // critical path to unlocking the receive lock, as it may take a long time to
  // run).
  if (error.value() == boost::asio::error::eof) {
    // Connection terminated, handle accordingly
    LOG(INFO) << "Connection to " << remote_endpoint << " closed.";
    // XXX(malte): Need to figure out if this relates to a resource, and if so,
    // if we should declare it failed; or whether this is an expected job
    // completion.
  } else {
    LOG(WARNING) << "Failed to complete a message receive cycle from "
                 << remote_endpoint << ". The message was discarded, or the "
                 << "connection failed (error: " << error.message() << ", "
                 << "code " << error.value() << ").";
  }
}

void Coordinator::HandleHeartbeat(const HeartbeatMessage& msg) {
  boost::uuids::string_generator gen;
  boost::uuids::uuid uuid = gen(msg.uuid());
  pair<ResourceDescriptor, uint64_t>* rdp =
      FindOrNull(*associated_resources_, uuid);
  if (!rdp) {
    LOG(WARNING) << "HEARTBEAT from UNKNOWN resource (uuid: "
                 << msg.uuid() << ")!";
  } else {
    LOG(INFO) << "HEARTBEAT from resource " << msg.uuid()
              << " (last seen at " << rdp->second << ")";
    // Update timestamp
    rdp->second = GetCurrentTimestamp();
  }
}

void Coordinator::HandleRegistrationRequest(
    const RegistrationMessage& msg) {
  boost::uuids::string_generator gen;
  boost::uuids::uuid uuid = gen(msg.uuid());
  pair<ResourceDescriptor, uint64_t>* rdp =
      FindOrNull(*associated_resources_, uuid);
  if (!rdp) {
    LOG(INFO) << "REGISTERING NEW RESOURCE (uuid: " << msg.uuid() << ")";
    // N.B.: below will copy the resource descriptor
    CHECK(InsertIfNotPresent(associated_resources_.get(), uuid,
                             pair<ResourceDescriptor, uint64_t>(
                                 msg.res_desc(),
                                 GetCurrentTimestamp())));
  } else {
    LOG(INFO) << "REGISTRATION request from resource " << msg.uuid()
              << " that we already know about. "
              << "Checking if this is a recovery.";
    // TODO(malte): Implement checking logic, deal with recovery case
    // Update timestamp (registration request is an implicit heartbeat)
    rdp->second = GetCurrentTimestamp();
  }
}

void Coordinator::HandleTaskStateChange(
    const TaskStateMessage& msg) {
  switch (msg.new_state()) {
    case TaskDescriptor::COMPLETED:
      VLOG(1) << "Task " << msg.id() << " now in state COMPLETED.";
      // XXX(malte): tear down the respective connection, cleanup
      break;
    default:
      VLOG(1) << "Task " << msg.id() << "'s state changed to "
              << static_cast<uint64_t>(msg.new_state());
      break;
  }
}


void Coordinator::HandleSignal(int signum) {
  // TODO(malte): stub
  if (signum == SIGTERM || signum == SIGINT)
    exit_ = true;
}

#ifdef __HTTP_UI__
void Coordinator::InitHTTPUI() {
  // Start up HTTP interface
  if (FLAGS_http_ui && FLAGS_http_ui_port > 0) {
    // TODO(malte): This is a hack to avoid failure of shared_from_this()
    // because we do not have a shared_ptr to this object yet. Not sure if this
    // is safe, though.... (I think it is, as long as the Coordinator's main()
    // still holds a shared_ptr to the Coordinator).
    //shared_ptr<Coordinator> dummy(this);
    c_http_ui_.reset(new CoordinatorHTTPUI(shared_from_this()));
    c_http_ui_->Init(FLAGS_http_ui_port);
  }
}
#endif

const string Coordinator::SubmitJob(const JobDescriptor& job_descriptor) {
  // Generate a job ID
  // TODO(malte): This should become deterministic, and based on the
  // inputs/outputs somehow, maybe.
  JobID_t new_job_id = GenerateJobID();
  LOG(INFO) << "NEW JOB: " << new_job_id;
  VLOG(2) << "Details:\n" << job_descriptor.DebugString();
  // Clone the JD and update it with some information
  JobDescriptor new_jd = job_descriptor;
  new_jd.set_uuid(to_string(new_job_id));
  new_jd.mutable_root_task()->set_uid(GenerateTaskID(new_jd.root_task()));
  // Add job to local job table
  CHECK(InsertIfNotPresent(job_table_.get(), new_job_id, new_jd));
  // Adds its outputs to the object table and generate future references for
  // them.
  for (RepeatedPtrField<ReferenceDescriptor>::const_iterator output_iter =
       new_jd.root_task().outputs().begin();
       output_iter != new_jd.root_task().outputs().end();
       ++output_iter) {
    VLOG(1) << "Considering root task output " << output_iter->id() << ", "
            << "adding to local object table";
    CHECK(InsertIfNotPresent(object_table_.get(), output_iter->id(),
                             *output_iter));
  }
  for (RepeatedField<DataObjectID_t>::const_iterator output_iter =
       new_jd.output_ids().begin();
       output_iter != new_jd.output_ids().end();
       ++output_iter) {
    VLOG(1) << "Considering job output " << *output_iter;
    // The root task must produce all of the non-existent job outputs, so they
    // should all be in the object table now.
    CHECK(FindOrNull(*object_table_.get(), *output_iter));
  }
#ifdef __SIMULATE_SYNTHETIC_DTG__
  LOG(INFO) << "SIMULATION MODE -- generating synthetic task graph!";
  sim_dtg_generator_.reset(new sim::SimpleDTGGenerator(FindOrNull(*job_table_,
                                                                  new_job_id)));
  boost::thread t(boost::bind(&sim::SimpleDTGGenerator::Run,
                              sim_dtg_generator_));
#endif
  // Kick off the scheduler for this job.
  scheduler_->ScheduleJob(shared_ptr<JobDescriptor>(
      FindOrNull(*job_table_, new_job_id)));
  // Finally, return the new job's ID
  return to_string(new_job_id);
}

void Coordinator::Shutdown(const string& reason) {
  LOG(INFO) << "Coordinator shutting down; reason: " << reason;
#ifdef __HTTP_UI__
  if (FLAGS_http_ui && c_http_ui_ && c_http_ui_->active())
    c_http_ui_->Shutdown(false);
#endif
  m_adapter_->StopListen();
  VLOG(1) << "All connections shut down; now exiting...";
  // Toggling the exit flag will make the Coordinator drop out of its main loop.
  exit_ = true;
}

}  // namespace firmament
