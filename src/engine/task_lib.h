// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The task library class is part of libfirmament, the library that gets linked
// into task binaries. Eventually, we should make this an interface, and support
// platform-specific classes.

#ifndef FIRMAMENT_ENGINE_TASK_LIB_H
#define FIRMAMENT_ENGINE_TASK_LIB_H

#include <string>
#include <vector>
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build task library!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "storage/reference_types.h"
#include "base/task_interface.h"
#include "messages/base_message.pb.h"
// XXX(malte): include order dependency
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_adapter-inl.h"
#include "platforms/unix/stream_sockets_channel-inl.h"
#include "storage/types.h"
#include "boost/interprocess/segment_manager.hpp"
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>

namespace firmament {

using platform_unix::streamsockets::StreamSocketsAdapter;
using platform_unix::streamsockets::StreamSocketsChannel;
using namespace store; 

class TaskLib {
 public:
  TaskLib();
  void Run(int argc, char *argv[]);
  void AwaitNextMessage();
  bool ConnectToCoordinator(const string& coordinator_uri);
  void RunTask(int argc, char *argv[]);
  // CIEL programming model
  //virtual const string Construct(const DataObject& object);
  void Spawn(const ConcreteReference& code,
             vector<FutureReference>* outputs);
  void Publish(const vector<ConcreteReference>& references);
  //virtual void TailSpawn(const ConcreteReference& code);

  void* GetObjectStart(DataObjectID_t id );
  void GetObjectEnd(DataObjectID_t id ); 
  void* PutObjectStart(DataObjectID_t id, size_t size); 
  void PutObjectEnd(DataObjectID_t id, size_t size);
  void* Extend(DataObjectID_t id, size_t old_size, size_t new_size);
    
  Cache_t* getCache() { 
    return cache ; 
  }


  
  
  
  
 protected:
  shared_ptr<StreamSocketsAdapter<BaseMessage> > m_adapter_;
  shared_ptr<StreamSocketsChannel<BaseMessage> > chan_;
  bool exit_;
  // TODO(malte): transform this into a better representation
  string coordinator_uri_;
  ResourceID_t resource_id_;
  TaskID_t task_id_;

  void ConvertTaskArgs(int argc, char *argv[], vector<char*>* arg_vec);
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
  void SendFinalizeMessage(bool success);
  void SendHeartbeat();
  bool SendMessageToCoordinator(BaseMessage* msg);

  void setUpStorageEngine() ; 
  
  

 private:
  bool task_error_;
  bool task_running_;
  uint64_t heartbeat_seq_number_;
  
  
  Cache_t* cache ; 
  string storage_uri ;  
  managed_shared_memory* segment; 
  named_mutex* mutex;  
  scoped_lock<named_mutex>* cache_lock; 
  ReferenceNotification_t* reference_not_t ; 
  
  
        
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_TASK_LIB_H
