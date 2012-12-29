// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Main task library class.
// TODO(malte): This should really be made platform-independent, so that we can
// have platform-specific libraries.

#include "engine/task_lib.h"

#include <vector>

#include "base/common.h"
#include "messages/registration_message.pb.h"
#include "messages/task_heartbeat_message.pb.h"
#include "messages/task_info_message.pb.h"
#include "messages/task_spawn_message.pb.h"
#include "messages/task_state_message.pb.h"
#include "misc/utils.h"
#include "platforms/common.h"
#include "platforms/common.pb.h"


DEFINE_string(coordinator_uri, "", "The URI to contact the coordinator at.");
DEFINE_string(resource_id, "",
              "The resource ID that is running this task.");
DEFINE_string(task_id, "", "The ID of this task.");
DEFINE_int32(heartbeat_interval, 1,
             "The interval, in seconds, between heartbeats sent to the"
             "coordinator.");

namespace firmament {

TaskLib::TaskLib()
  : m_adapter_(new StreamSocketsAdapter<BaseMessage>()),
    chan_(new StreamSocketsChannel<BaseMessage>(
        StreamSocketsChannel<BaseMessage>::SS_TCP)),
    coordinator_uri_(FLAGS_coordinator_uri),
    resource_id_(ResourceIDFromString(FLAGS_resource_id)),
    task_error_(false),
    task_running_(false),
    heartbeat_seq_number_(0) {
  const char* task_id_env;
  if (FLAGS_task_id.empty())
    task_id_env = getenv("TASK_ID");
  else
    task_id_env = FLAGS_task_id.c_str();
  VLOG(1) << "Task ID is " << task_id_env;
  CHECK_NOTNULL(task_id_env);
  task_id_ = TaskIDFromString(task_id_env);
}

void TaskLib::AwaitNextMessage() {
  // Finally, call back into ourselves.
  //AwaitNextMessage();
}

bool TaskLib::ConnectToCoordinator(const string& coordinator_uri) {
  return m_adapter_->EstablishChannel(
      coordinator_uri, shared_ptr<StreamSocketsChannel<BaseMessage> >(chan_));
}

void TaskLib::Spawn(const ConcreteReference& code,
                     vector<FutureReference>* outputs) {
  VLOG(1) << "Spawning a new task; code reference is " << code.desc().id();
  // Craft a task spawn message for the new task, using a newly created task
  // descriptor.
  BaseMessage msg;
  SUBMSG_WRITE(msg, task_spawn, creating_task_id, task_id_);
  TaskDescriptor* new_task =
      msg.MutableExtension(task_spawn_extn)->mutable_spawned_task_desc();
  new_task->set_uid(1234);
  new_task->set_name("");
  new_task->set_state(TaskDescriptor::CREATED);
  SendMessageToCoordinator(&msg);
}

void TaskLib::Publish(const vector<ConcreteReference>& references) {
  LOG(ERROR) << "Output publication currently unimplemented!";
}

void TaskLib::ConvertTaskArgs(int argc, char *argv[], vector<char*>* arg_vec) {
  VLOG(1) << "Stripping Firmament default arguments...";
  for (int64_t i = 0; i < argc; ++i) {
    if (strstr(argv[i], "--tryfromenv") || strstr(argv[i], "--v")) {
      // Ignore standard arguments that refer to Firmament's task_lib, rather
      // than being passed through to the user code.
      continue;
    } else {
      arg_vec->push_back(argv[i]);
    }
  }
  VLOG(1) << arg_vec->size() << " out of " << argc << " original arguments "
          << "remain.";
}

void TaskLib::HandleWrite(const boost::system::error_code& error,
                         size_t bytes_transferred) {
  VLOG(1) << "In HandleWrite, thread is " << boost::this_thread::get_id();
  if (error)
    LOG(ERROR) << "Error returned from async write: " << error.message();
  else
    VLOG(1) << "bytes_transferred: " << bytes_transferred;
}

bool TaskLib::PullTaskInformationFromCoordinator(TaskID_t task_id,
                                                 TaskDescriptor* desc) {
  // Send request for task information to coordinator
  BaseMessage msg;
  SUBMSG_WRITE(msg, task_info_req, task_id, task_id);
  SUBMSG_WRITE(msg, task_info_req, requesting_resource_id,
               to_string(resource_id_));
  SUBMSG_WRITE(msg, task_info_req, requesting_endpoint,
               chan_->LocalEndpointString());
  Envelope<BaseMessage> envelope(&msg);
  CHECK(chan_->SendS(envelope));
  // Wait for a response
  BaseMessage response;
  Envelope<BaseMessage> recv_envelope(&response);
  chan_->RecvS(&recv_envelope);
  CHECK(response.HasExtension(task_info_resp_extn));
  desc->CopyFrom(SUBMSG_READ(response, task_info_resp, task_desc));
  VLOG(1) << "Received TD: " << desc->DebugString();
  return true;
}

void TaskLib::Run(int argc, char *argv[]) {
  // First, connect to the coordinator
  CHECK(ConnectToCoordinator(coordinator_uri_))
      << "Failed to connect to coordinator; is it reachable?";

  // Pull task information from coordinator if we do not have it already
  PullTaskInformationFromCoordinator(task_id_, &task_descriptor_);

  // Async receive -- the handler is responsible for invoking this again.
  AwaitNextMessage();

  // Run task -- this will only return when the task thread has finished.
  task_running_ = true;
  RunTask(argc, argv);

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; terminate running
  // tasks etc.
  VLOG(1) << "Dropped out of main loop -- cleaning up...";
  // task_error_ will be set if the task failed for some reason.
  SendFinalizeMessage(!task_error_);
  chan_->Close();
}

void TaskLib::RunTask(int argc, char *argv[]) {
  //CHECK(task_desc_.code_dependency.is_consumable());
  LOG(INFO) << "Invoking task code...";
  // Convert the arguments
  vector<char*>* task_arg_vec = new vector<char*>;
  ConvertTaskArgs(argc, argv, task_arg_vec);
  // task_main blocks until the task has exited
  //  exec(task_desc_.code_dependency());

  /* Set up Storage Engine here, returning a void* to the Cache */
  /* Alternate way of doing this: set up cache here and just pass the
   various pointers. */
  
  setUpStorageEngine() ; 
  boost::thread task_thread(boost::bind(task_main, this, task_id_,
  task_running_ = true;
  // This will check if the task thread has joined once every heartbeat
  // interval, and go back to sleep if it has not.
  // TODO(malte): think about whether we'd like some kind of explicit
  // notification scheme in case the heartbeat interval is large.
  while (!task_thread.timed_join(
      boost::posix_time::seconds(FLAGS_heartbeat_interval))) {
    // TODO(malte): Check if we've exited with an error
    // if(error)
    //   task_error_ = true;
    // Notify the coordinator that we're still running happily
    VLOG(1) << "Task thread has not yet joined, sending heartbeat...";
    SendHeartbeat();
    // TODO(malte): We'll need to receive any potential messages from the
    // coordinator here, too. This is probably best done by a simple RecvA on
    // the channel.
  }
  task_running_ = false;
  delete task_arg_vec;
  // The finalizing message, reporting task success or failure, will be sent by
  // the main loop once we drop out here.
}

void TaskLib::SendFinalizeMessage(bool success) {
  BaseMessage bm;
  SUBMSG_WRITE(bm, task_state, id, task_id_);
  if (success)
    SUBMSG_WRITE(bm, task_state, new_state, TaskDescriptor::COMPLETED);
  else
    LOG(FATAL) << "Unimplemented error path!";
  VLOG(1) << "Sending finalize message (task state change to "
          << (success ? "COMPLETED" : "FAILED") << ")!";
  //SendMessageToCoordinator(&bm);
  Envelope<BaseMessage> envelope(&bm);
  CHECK(chan_->SendS(envelope));
  VLOG(1) << "Done sending message, sleeping before quitting";
  boost::this_thread::sleep(boost::posix_time::seconds(1));
}

void TaskLib::SendHeartbeat() {
  BaseMessage bm;
  SUBMSG_WRITE(bm, task_heartbeat, task_id, task_id_);
  // TODO(malte): we do not always need to send the location string; it
  // sufficies to send it if our location changed (which should be rare).
  SUBMSG_WRITE(bm, task_heartbeat, location, chan_->LocalEndpointString());
  SUBMSG_WRITE(bm, task_heartbeat, sequence_number, heartbeat_seq_number_++);
  VLOG(1) << "Sending heartbeat message!";
  SendMessageToCoordinator(&bm);
}

bool TaskLib::SendMessageToCoordinator(BaseMessage* msg) {
  Envelope<BaseMessage> envelope(msg);
  return chan_->SendA(
      envelope, boost::bind(&TaskLib::HandleWrite,
                            this,
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
}

void TaskLib::setUpStorageEngine() { 
    
    /* Contact coordinator to ask where storage engine is for this resource
     As currently, only assume that is local, don't currently need it
     Also need */
    
    const char* name = ("Cache" + to_string(resource_id_)).c_str(); 
    
//    StorageDiscoverMessage* msg = new StorageDiscoverMessage() ; 
//    
//    msg->set_uuid(resource_id_) ; 
//    msg->uri(""); 
//    
//    SendMessageToCoordinator(msg); 
//    Expect reply with URI 
    
    /* If local*/
   
    managed_shared_memory segment(open_only,name );
    Cache_t* cache = new Cache_t ; 
    
    cache->object_list = segment.find<SharedVector_t>("objects");
    cache->capacity = segment.find<size_t>("size"); 
   
    
    named_mutex mutex_(open_only, cache_name);
    mutex = &mutex_; 
    WriteLock_t cache_lock_(*mutex);
   
     reference_not_t = 
                    segment->construct<ReferenceNotification_t>("refnot")();
     
    
    //TODO: error handling
    if (cache==NULL) VLOG(1) << "Error: no cache found"; 
    
    
    
    /* Else if not local - not implemented*/


    
}

 /* Finds data from cache and returns pointer. Acquires shared
   read lock. Contacts storage engine if data is not in cache */
  void* TaskLib::GetObjectStart(DataObjectID_t id ) { 
      VLOG(3) << "GetObjectStart " << id << endl ; 
      
      vector<DataObjectID_t>::iterator result =
              find(cache->object_list->begin(), cache->object_list->end(), id);
      /* Should only be one result in theory*/
      if (!result->empty()) { 
         named_mutex mut(open_only, StringFromDataObjectIdMut(id));
         ReadLock_t lock(mut);  
         lock.lock() ; 
         file_mapping m_file(id, read_only);
         mapped_region region(m_file, read_only);
         void* object = region.getAddress() ;
         return object ; 
      }
      else { 
          VLOG(1) << "Object " << id << " was not found in cache. Contacting"
                  "Object Store " << endl ; 
          
          return null ; 
          /* Data is not in cache - Let object store do the work */
          
          
          
          /* Local set up named conditions - wait until data is in
           cache */
          
          
          /* Non local - Not implemented */
      }
      
  }
  
  /* Releases shared read lock */
   void TaskLib::GetObjectEnd(DataObjectID_t id ) { 
       VLOG(3) << "GetObjectEnd " << endl ;  
       /* TODO : very inefficient*/
       named_mutex mut(open_only, StringFromDataObjectIdMut(id));
       ReadLock_t lock(mut);  
       file_mapping::remove(id); 
       lock.unlock(); 

      }

  
   /* Return null if cache is full */
  void* TaskLib::PutObjectStart(DataObjectID_t id, size_t size) { 
      VLOG(3) << "PutObjectStart " << endl ;
      cache_lock->lock(); 
      if (cache->capacity > size ) cache->capacity-=size; 
      else { 
          VLOG(3) << "Fail to create object << id << ". Cache full "; 
          cache_lock->unlock() ; 
          return NULL ; 
      }
      cache_lock->unlock() ; 
      
    /* Create file */
    std::ofstream ofs("id", std::ios::binary | std::ios::out);
    ofs.seekp(size - 1);
    ofs.write("", 1);
    /* Map it*/
    file_mapping m_file(id, read_write);
    mapped_region region(m_file, read_write);  
    void* address = region.getAddress() ;
    named_mutex mut(open_only, StringFromDataObjectIdMut(id));
    WriteLock_t lock(mut);  
    lock.lock() ; 
   /* Add it to cache */
    cache->object_list->push_back(id); 
 
    return address ; 
  }
  
  /* Tries to extend file by "extend_by" bytes. Returns void if failed 
   ex: cache full. Find a way to do this without unmapping/remapping the
   file */
  void* TaskLib::Extend(DataObjectID_t id, size_t old_size, size_t new_size) {
      void* address= NULL ;
      cache_lock->lock(); 
      size_t extend_by = new_size - old_size; 
      if (cache->capacity > extend_by ) cache->capacity-=extend_by; 
      else { 
          VLOG(3) << "Fail to extend. Cache full "; 
          cache_lock->unlock() ; 
          return address ; 
      }
      cache_lock->unlock() ; 
      file_mapping m_file(id, read_write);
      mapped_region region(m_file, read_write);  
      region.flush() ;
      file_mapping::remove(id); 
      std::ofstream ofs("id", std::ios::binary);
      ofs.seekp(new_size - 1);
      ofs.write("", 1);
     file_mapping m_file(id, read_write);
     mapped_region region(m_file, read_write);  
     address = region.getAddress() ;
     
     return address ; 
      
  }
  
  
  /* Release exclusive lock  - value of size actually written */
  void TaskLib::PutObjectEnd(DataObjectID_t id, size_t size) { 
      VLOG(3) << "PutObjectEnd " << endl ; 
      file_mapping m_file(id, read_write);
      mapped_region region(m_file, read_write);  
      region.flush() ;
      file_mapping::remove(id); 
      named_mutex mut(open_only, StringFromDataObjectIdMut(id));
      WriteLock_t lock(mut);  
      lock.unlock() ; 
      
      /* Notify Engine that reference is now concrete */
      WriteLock_t lock_ref(reference_not_t->mutex);
      while(!reference_not_t->writable) 
          reference_not_t->cond_read.wait(lock_ref); 
      CHECK(writable == true ); 
      reference_not_t->id = id ; 
      reference_not_t->writable = false ; 
      reference_not_t->size = size; 
      lock_ref.unlock() ;  /* Not sure if this is necessary */
      reference_not_t->cond_added.notify_one() ; /* Storage engine will be 
                                      * only one waiting on this */             
      }  
 
  

  


}  // namespace firmament
