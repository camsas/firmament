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
#include "base/resource_topology_node_desc.pb.h"
#include "storage/simple_object_store.h"
#include "messages/base_message.pb.h"
#include "misc/protobuf_envelope.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "messages/storage_registration_message.pb.h"
#include "messages/storage_message.pb.h"


// It is necessary to declare listen_uri here, since "node.o" comes after
// "coordinator.o" in linking order (I *think*).
DECLARE_string(listen_uri);
DEFINE_string(parent_uri, "", "The URI of the parent coordinator to register "
        "with.");
#ifdef __HTTP_UI__
DEFINE_bool(http_ui, true, "Enable HTTP interface");
DEFINE_int32(http_ui_port, 8080,
        "The port that the HTTP UI will be served on; -1 to disable.");
#endif

namespace firmament {


Coordinator::Coordinator(PlatformID platform_id)
  : Node(platform_id, GenerateUUID()),
    associated_resources_(new ResourceMap_t),
    local_resource_topology_(new ResourceTopologyNodeDescriptor),
    job_table_(new JobMap_t),
    task_table_(new TaskMap_t),
    topology_manager_(new TopologyManager()),
    object_store_(new store::SimpleObjectStore(uuid_)),
    scheduler_(new SimpleScheduler(job_table_, associated_resources_,
                                   object_store_, task_table_,
                                   topology_manager_,
                                   FLAGS_listen_uri))
   
 { 
        // Start up a coordinator according to the platform parameter
        string hostname = boost::asio::ip::host_name();
        string desc_name = "Coordinator on " + hostname;
        resource_desc_.set_uuid(to_string(uuid_));
        resource_desc_.set_friendly_name(desc_name);
        resource_desc_.set_type(ResourceDescriptor::RESOURCE_MACHINE);
        local_resource_topology_->mutable_resource_desc()->CopyFrom(resource_desc_);
        // Log information
        LOG(INFO) << "Coordinator starting on host " << FLAGS_listen_uri
                << ", platform " << platform_id << ", uuid " << uuid_;
        LOG(INFO) << "Storage Engine is listening on interface : " << object_store_->get_listening_interface();
        switch (platform_id) {
            case PL_UNIX:
            {
                break;
            }
            default:
                LOG(FATAL) << "Unimplemented!";
        }

        // Do we have a parent? If so, register with it now.
        if (FLAGS_parent_uri != "") {
            shared_ptr<StreamSocketsChannel<BaseMessage> > chan(
                    new StreamSocketsChannel<BaseMessage > (
                    StreamSocketsChannel<BaseMessage>::SS_TCP));
            CHECK(ConnectToRemote(FLAGS_parent_uri, chan))
                    << "Failed to connect to parent!";

            InformStorageEngineNewResource(&resource_desc_); 
            
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

    bool Coordinator::RegisterWithCoordinator(
            shared_ptr<StreamSocketsChannel<BaseMessage> > chan) {
        BaseMessage bm;
        ResourceDescriptor* rd = bm.MutableExtension(
                register_extn)->mutable_res_desc();
        *rd = resource_desc_; // copies current local RD!
        bm.MutableExtension(register_extn)->set_uuid(
                boost::uuids::to_string(uuid_));
        // wrap in envelope
        VLOG(2) << "Sending registration message...";
        // send heartbeat message
        return SendMessageToRemote(chan, &bm);
    }

    void Coordinator::DetectLocalResources() {
        // TODO(malte): This is somewhat of a hack currently; instead of taking a
        // flat vector of resources, we obviuosly want to take a proper topology.
        uint64_t num_local_pus = topology_manager_->NumProcessingUnits();
        LOG(INFO) << "Found " << num_local_pus << " local PUs.";
        // Get local resource topology and save it to the topology protobuf
        // TODO(malte): Figure out how this interacts with dynamically added
        // resources; currently, we only run detection (i.e. the DetectLocalResources
        // method) once at startup.
        ResourceTopologyNodeDescriptor* root_node =
                local_resource_topology_->add_children();
        topology_manager_->AsProtobuf(root_node);
        local_resource_topology_->set_parent_id(to_string(uuid_));
        topology_manager_->TraverseProtobufTree(
                local_resource_topology_,
                boost::bind(&Coordinator::AddLocalResource, this, _1));
    }
    
    void Coordinator::AddLocalResource(ResourceDescriptor* resource_desc) {
        CHECK(resource_desc);
        // Compute resource ID
        ResourceID_t res_id = ResourceIDFromString(resource_desc->uuid());
        // Add resource to local resource set
        CHECK(InsertIfNotPresent(associated_resources_.get(), res_id,
                pair<ResourceDescriptor*, uint64_t > (
                resource_desc, GetCurrentTimestamp())));
        // Register with scheduler if this resource is schedulable
        if (resource_desc->type() == ResourceDescriptor::RESOURCE_PU) {
            // TODO(malte): We make the assumption here that any local PU resource is
            // exclusively owned by this coordinator, and set its state to IDLE if it is
            // currently unknown. If coordinators were to ever shared PUs, we'd need
            // something more clever here.
            resource_desc->set_schedulable(true);
            if (resource_desc->state() == ResourceDescriptor::RESOURCE_UNKNOWN)
                resource_desc->set_state(ResourceDescriptor::RESOURCE_IDLE);
            scheduler_->RegisterResource(res_id);
            VLOG(1) << "Added local resource " << resource_desc->uuid()
                    << " [" << resource_desc->friendly_name()
                    << "] to scheduler.";
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

    const JobDescriptor* Coordinator::DescriptorForJob(const string& job_id) {
        JobID_t job_uuid = JobIDFromString(job_id);
        JobDescriptor *jd = FindOrNull(*job_table_, job_uuid);
        return jd;
    }

    void Coordinator::HandleIncomingMessage(BaseMessage *bm) {
          uint32_t handled_extensions = 0;
        // Registration message
        if (bm->HasExtension(register_extn)) {
            const RegistrationMessage& msg = bm->GetExtension(register_extn);
            HandleRegistrationRequest(msg);
            handled_extensions++; 
        }
        // Resource Heartbeat message
        if (bm->HasExtension(heartbeat_extn)) {
            const HeartbeatMessage& msg = bm->GetExtension(heartbeat_extn);
            HandleHeartbeat(msg);
            handled_extensions++;
        }
        // Task heartbeat message
        if (bm->HasExtension(task_heartbeat_extn)) {
            const TaskHeartbeatMessage& msg = bm->GetExtension(task_heartbeat_extn);
            HandleTaskHeartbeat(msg);
            handled_extensions++;
        }
        // Task state change message
        if (bm->HasExtension(task_state_extn)) {
            const TaskStateMessage& msg = bm->GetExtension(task_state_extn);
            HandleTaskStateChange(msg);
            handled_extensions++;
        }
        // Task spawn message
        if (bm->HasExtension(task_spawn_extn)) {
            const TaskSpawnMessage& msg = bm->GetExtension(task_spawn_extn);
            HandleTaskSpawn(msg);
            handled_extensions++;
        }
        // Task info request message
        if (bm->HasExtension(task_info_req_extn)) {
          const TaskInfoRequestMessage& msg = bm->GetExtension(task_info_req_extn);
          HandleTaskInfoRequest(msg);
          handled_extensions++;
        }
        /* Storage Engine*/
        if (bm->HasExtension(register_storage_extn)) {
            const StorageRegistrationMessage& msg = bm->GetExtension(register_storage_extn);
            HandleStorageRegistrationRequest(msg);
        }
        if (bm->HasExtension(storage_discover_message_extn)) {
            const StorageDiscoverMessage& msg = bm->GetExtension(storage_discover_message_extn);
            HandleStorageDiscoverRequest(msg);

        }
        // Check that we have handled at least one sub-message
        if (handled_extensions == 0)
          LOG(ERROR) << "Ignored incoming message, no known extension present, "
                     << "so cannot handle it: " << bm->DebugString();
    }

    void Coordinator::HandleHeartbeat(const HeartbeatMessage& msg) {
        boost::uuids::string_generator gen;
        boost::uuids::uuid uuid = gen(msg.uuid());
        pair<ResourceDescriptor*, uint64_t>* rdp =
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
        pair<ResourceDescriptor*, uint64_t>* rdp =
                FindOrNull(*associated_resources_, uuid);
        if (!rdp) {
            LOG(INFO) << "REGISTERING NEW RESOURCE (uuid: " << msg.uuid() << ")";
            // N.B.: below creates a new resource descriptor
            // XXX(malte): We should be adding to the topology tree instead here!
            ResourceDescriptor* rd = new ResourceDescriptor(msg.res_desc()); 
            CHECK(InsertIfNotPresent(associated_resources_.get(), uuid,
                    pair<ResourceDescriptor*, uint64_t > (
                    rd,
                    GetCurrentTimestamp())));
            
            InformStorageEngineNewResource(rd); 


        } else {
            LOG(INFO) << "REGISTRATION request from resource " << msg.uuid()
                    << " that we already know about. "
                    << "Checking if this is a recovery.";
            // TODO(malte): Implement checking logic, deal with recovery case
            // Update timestamp (registration request is an implicit heartbeat)
            rdp->second = GetCurrentTimestamp();
        }
    }
    
    void Coordinator::HandleTaskHeartbeat(const TaskHeartbeatMessage& msg) {
  TaskID_t task_id = msg.task_id();
  TaskDescriptor** tdp = FindOrNull(*task_table_, task_id);
  if (!tdp) {
    LOG(WARNING) << "HEARTBEAT from UNKNOWN task (ID: "
                 << task_id << ")!";
  } else {
    LOG(INFO) << "HEARTBEAT from task " << task_id;
  }
}

void Coordinator::HandleTaskInfoRequest(const TaskInfoRequestMessage& msg) {
  // Send response: the task descriptor if the task is known to this
  // coordinator
  VLOG(1) << "Resource " << msg.requesting_resource_id()
          << " requests task information for " << msg.task_id();
  TaskDescriptor** task_desc_ptr = FindOrNull(*task_table_, msg.task_id());
  CHECK_NOTNULL(task_desc_ptr);
  BaseMessage resp;
  // XXX(malte): ugly hack!
  SUBMSG_WRITE(resp, task_info_resp, task_id, msg.task_id());
  resp.MutableExtension(task_info_resp_extn)->
      mutable_task_desc()->CopyFrom(**task_desc_ptr);
  m_adapter_->SendMessageToEndpoint(msg.requesting_endpoint(), resp);
}

void Coordinator::HandleTaskSpawn(const TaskSpawnMessage& msg) {
  // Get the descriptor for the spawning task
  TaskDescriptor** spawner;
  CHECK(spawner = FindOrNull(*task_table_, msg.creating_task_id()));
  // Extract new task descriptor from the message received
  TaskDescriptor* spawnee = new TaskDescriptor;
  spawnee->CopyFrom(msg.spawned_task_desc());
  // Find root task for job
  // Add task to task graph
  // task_graph.AddChildTask(spawner, spawnee)
  // Run the scheduler for this job
  // scheduler_->ScheduleJob(job);
}

    void Coordinator::HandleTaskStateChange(
            const TaskStateMessage& msg) {
        switch (msg.new_state()) {
            case TaskDescriptor::COMPLETED:
            {
                VLOG(1) << "Task " << msg.id() << " now in state COMPLETED.";
                // XXX(malte): tear down the respective connection, cleanup
                TaskDescriptor** td_ptr = FindOrNull(*task_table_, msg.id());
                CHECK(td_ptr) << "Received completion message for unknown task "
                        << msg.id();
                (*td_ptr)->set_state(TaskDescriptor::COMPLETED);
                scheduler_->HandleTaskCompletion(*td_ptr);
                break;
            }
            default:
                VLOG(1) << "Task " << msg.id() << "'s state changed to "
                        << static_cast<uint64_t> (msg.new_state());
                break;
        }
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

void Coordinator::AddJobsTasksToTaskTable(
    RepeatedPtrField<TaskDescriptor>* tasks) {
  for (RepeatedPtrField<TaskDescriptor>::iterator task_iter =
       tasks->begin();
       task_iter != tasks->end();
       ++task_iter) {
    VLOG(1) << "Adding task " << task_iter->uid() << " to task table.";
    CHECK(InsertIfNotPresent(task_table_.get(), task_iter->uid(),
                             &(*task_iter)));
    AddJobsTasksToTaskTable(task_iter->mutable_spawned());
  }
}

  const string Coordinator::SubmitJob(const JobDescriptor& job_descriptor) {
  // Generate a job ID
  // TODO(malte): This should become deterministic, and based on the
  // inputs/outputs somehow, maybe.
  JobID_t new_job_id = GenerateJobID();
  LOG(INFO) << "NEW JOB: " << new_job_id;
  VLOG(2) << "Details:\n" << job_descriptor.DebugString();
  // Clone the JD and update it with some information
  JobDescriptor* new_jd = new JobDescriptor;
  new_jd->CopyFrom(job_descriptor);
  new_jd->set_uuid(to_string(new_job_id));
  // Set the root task ID (which is 0 or unset on submission)
  new_jd->mutable_root_task()->set_uid(GenerateRootTaskID(*new_jd));
  // Add job to local job table
  CHECK(InsertIfNotPresent(job_table_.get(), new_job_id, *new_jd));
  // Add its root task to the task table
  if (!InsertIfNotPresent(task_table_.get(), new_jd->root_task().uid(),
                          new_jd->mutable_root_task())) {
    VLOG(1) << "Task " << new_jd->root_task().uid() << " already exists in "
            << "task table, so not adding it again.";
  }
  // Add its spawned tasks (if any) to the local task table
  RepeatedPtrField<TaskDescriptor>* spawned_tasks =
      new_jd->mutable_root_task()->mutable_spawned();
  AddJobsTasksToTaskTable(spawned_tasks);
  // Adds its outputs to the object table and generate future references for
  // them.
  for (RepeatedPtrField<ReferenceDescriptor>::iterator output_iter =
       new_jd->mutable_root_task()->mutable_outputs()->begin();
       output_iter != new_jd->mutable_root_task()->mutable_outputs()->end();
       ++output_iter) {
    // First set the producing task field on the root task output, since the
    // task ID has only just been determined (so it cannot be set yet)
    output_iter->set_producing_task(new_jd->root_task().uid());
    VLOG(1) << "Considering root task output " << output_iter->id() << ", "
            << "adding to local object table";
    if(object_store_->addReference(output_iter->id(), &(*output_iter))) { 
      VLOG(1) << "Output " << output_iter->id() << " already exists in "
              << "local object table. Not adding again.";
    }
  }
  for (RepeatedField<DataObjectID_t>::const_iterator output_iter =
       new_jd->output_ids().begin();
       output_iter != new_jd->output_ids().end();
       ++output_iter) {
    VLOG(1) << "Considering job output " << *output_iter;
    // The root task must produce all of the non-existent job outputs, so they
    // should all be in the object table now.
    CHECK(object_store_->GetReference(*output_iter));
  }
#ifdef __SIMULATE_SYNTHETIC_DTG__
  LOG(INFO) << "SIMULATION MODE -- generating synthetic task graph!";
  sim_dtg_generator_.reset(new sim::SimpleDTGGenerator(FindOrNull(*job_table_,
                                                                  new_job_id)));
  boost::thread t(boost::bind(&sim::SimpleDTGGenerator::Run,
                              sim_dtg_generator_));
#endif
  // Kick off the scheduler for this job.
  uint64_t num_scheduled = scheduler_->ScheduleJob(
      FindOrNull(*job_table_, new_job_id));
  VLOG(1) << "Attempted to schedule job " << new_job_id << ", successfully "
          << "scheduled " << num_scheduled << " tasks.";
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

    /* Storage Engine Interface - The storage engine is notified of the location
     * of new storage engines in one of three ways: 
     * 1) if a new resource is added to the coordinator, then all the local
     * storage engines (which share the same coordinator) should be informed.
     * Automatically add this to the list of peers
     * 2) if receive a StorageRegistrationRequest, this signals the desire of a
     * storage engine to be registered with (either) all of the storage engines 
     * managed by this coordinator, or its local engine. StorageRegistrationRequests
     * can also arrive directly to the storage engine. The coordinator supports this
     * because outside resources/coordinators may not know the whole topology within
     * the coordinator. I *suspect* this technique will be faster
     * TODO: improve efficiency */

    void Coordinator::InformStorageEngineNewResource(ResourceDescriptor* rd_new) {

        VLOG(2) << "Inform Storage Engine of New Resources ";

        /* Create Storage Registration Message */

        BaseMessage base;
        StorageRegistrationMessage* message = new StorageRegistrationMessage();
        message->set_peer(true);
        message->set_storage_interface(rd_new->storage_engine());
        message->set_uuid(rd_new->uuid());

        StorageRegistrationMessage& message_ref = *message ; 
        
        vector<ResourceDescriptor*> rd_vec = associated_resources();

        /* Handle Local Engine First */

        object_store_->HandleStorageRegistrationRequest(message_ref);

        /* Handle other resources' engines*/
        for (vector<ResourceDescriptor*>::iterator it = rd_vec.begin(); it != rd_vec.end(); ++it) {

            ResourceDescriptor* rd = *it;
            const string& uri = rd->storage_engine();
            if (!m_adapter_->GetChannelForEndpoint(uri)) {
                shared_ptr<StreamSocketsChannel<BaseMessage> > chan(
                        new StreamSocketsChannel<BaseMessage > (
                        StreamSocketsChannel<BaseMessage>::SS_TCP));
                Coordinator::ConnectToRemote(uri, chan);
            } else m_adapter_->SendMessageToEndpoint(uri, (BaseMessage&)message_ref);
        }
    }

    /* Wrapper Method - Forward to Object Store. This method is useful
     * for other storage engines which simply talk to the coordinator,
     * letting it to the broadcast to its resources 
     */
    void Coordinator::HandleStorageRegistrationRequest(
            const StorageRegistrationMessage& msg) {

        if (object_store_ == NULL) { /* In theory, each node should have an object
                              store which has already been instantiated. S
                              this case should never happen */
            VLOG(1) << "No object store detected for this node. Storage Registration"
                    "Message discarded";
        } else {
            object_store_->HandleStorageRegistrationRequest(msg);
        }

    }
    
    /* This is a message sent by TaskLib which seeks to discover
     where the storage engine is - this is only important if the
     storage engine is not guaranteed to be local */
    void Coordinator::HandleStorageDiscoverRequest(
            const StorageDiscoverMessage& msg) {

        ResourceID_t uuid = ResourceIDFromString(msg.uuid()) ; 
        ResourceDescriptor* rd = (FindOrNull(*associated_resources_,uuid))->first ; 
        const string& uri = rd->storage_engine();
        
        StorageDiscoverMessage* reply = new StorageDiscoverMessage() ; 
        reply->set_uuid(msg.uuid()); 
        reply->set_uri(msg.uri()) ; 
        reply->set_storage_uri(uri) ; 
        
        /* Send Message*/

    }

        


} // namespace firmament
