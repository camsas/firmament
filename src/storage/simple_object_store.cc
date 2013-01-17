// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub object store class.

#include "storage/simple_object_store.h"
#include "storage/Cache.h"
#include "storage/StorageInfo.h"


namespace firmament {
  namespace store {

    //TACH: make non unix specific.

    SimpleObjectStore::SimpleObjectStore(ResourceID_t uuid_) :
    ObjectStoreInterface() {
      uuid = uuid_;
      message_adapter_.reset(new platform_unix::streamsockets::StreamSocketsAdapter<BaseMessage > ());
      object_table_.reset(new DataObjectMap_t);
      VLOG(3) << "Setting up communications ";

      setUpCommunication();

      createSharedBuffer(10000);




    }

    SimpleObjectStore::~SimpleObjectStore() {

      VLOG(2) << "Shutting Storage Engine Down";
      cache.reset();
      message_adapter_.reset();
      object_table_.reset();

      //TO IMPLEMENT

    }

    void SimpleObjectStore::HandleStorageRegistrationRequest(const StorageRegistrationMessage& msg) {
      VLOG(3) << "HandleStorageRegistrationRequest " << endl;

      /*TODO: currently assume that no two instances of the storage engine
       are local. Therefore must communicate over TCP */

      shared_ptr<StreamSocketsChannel<BaseMessage> > chan(
              new StreamSocketsChannel<BaseMessage > (
              StreamSocketsChannel<BaseMessage>::SS_TCP));
      message_adapter_->EstablishChannel(msg.storage_interface(), chan);


      //   StorageInfo* node = new StorageInfo(msg.storage_interface(), msg.uuid(),  msg.has_coordinator_uuid()? msg.coordinator_uuid() : "", chan ) ; 
      StorageInfo* node = new StorageInfo();

      if (msg.peer()) peers.push_back(node);
      nodes.push_back(node);

    }

    void SimpleObjectStore::HandleIncomingMessage(BaseMessage *bm) {
      VLOG(3) << "Storage Engine: HandleIncomingMessage";


      /* Handle HeartBeatMessage */
      if (bm->HasExtension(storage_heartbeat_message_extn)) {
        const StorageHeartBeatMessage& msg = bm->GetExtension(storage_heartbeat_message_extn);
        HandleIncomingHeartBeat(msg);
      }

    }

    void SimpleObjectStore::HandleIncomingHeartBeat(const StorageHeartBeatMessage& msg) {

      VLOG(1) << "Handle Incoming HeartBeat Message ";

      /* Update object map efficiently*/


    }

    void SimpleObjectStore::HandleIncomingReceiveError(
            const boost::system::error_code& error,
            const string& remote_endpoint) {
      if (error.value() == boost::asio::error::eof) {
        // Connection terminated, handle accordingly
        LOG(INFO) << "Connection to " << remote_endpoint << " closed.";
      } else {
        LOG(WARNING) << "Failed to complete a message receive cycle from "
                << remote_endpoint << ". The message was discarded, or the "
                << "connection failed (error: " << error.message() << ", "
                << "code " << error.value() << ").";
      }
    }

    /* TODO: Hacky and not portable*/
    void SimpleObjectStore::setUpCommunication() {
      VLOG(3) << "Storage Engine: Set Up Communications ";

      listening_interface_ = message_adapter_->Listen();

      VLOG(3) << "Successfully initialised " << listening_interface_;


      message_adapter_->RegisterAsyncMessageReceiptCallback(
              boost::bind(&SimpleObjectStore::HandleIncomingMessage, this, _1));
      message_adapter_->RegisterAsyncErrorPathCallback(
              boost::bind(&SimpleObjectStore::HandleIncomingReceiveError, this,
              boost::asio::placeholders::error, _2));
      VLOG(3) << "Finished creating Adapter ";
    }

    void SimpleObjectStore::printTopology() {

      VLOG(3) << "Peers " << endl;
      for (vector<StorageInfo*>::iterator it = peers.begin(); it != peers.end(); ++it) {
        StorageInfo* inf = *it;
        VLOG(3) << "Resource ID : " + inf->get_resource_uuid() + " URI " + inf->get_node_uri() << endl;

      }
      VLOG(3) << "Nodes " << endl;

      for (vector<StorageInfo*>::iterator it = nodes.begin(); it != nodes.end(); ++it) {
        StorageInfo* inf = *it;
        VLOG(3) << "Resource ID : " + inf->get_resource_uuid() + " URI " + inf->get_node_uri();

      }

    }

    void SimpleObjectStore::createSharedBuffer(size_t size) {
      string str = ("Cache");
      //      string str = ("Cache" + to_string(uuid));
      cache.reset(new Cache(this, size, str));

    }

    void SimpleObjectStore::obtain_object_remotely(DataObjectID_t id) {
      VLOG(3) << "Obtaining object remotely " << endl;
      /* Find out location of object */
      /* Send request (ObtainObjectMessage) with id and no data field*/
      /* Message will be received in MessageAdapter. Maybe block on condition here? */
    }

    void SimpleObjectStore::HeartBeatTask() {
      VLOG(3) << "Setting up heartbeat thread  " << endl;

      boost::thread t(&SimpleObjectStore::sendHeartbeat, *this);

    }

    void SimpleObjectStore::sendHeartbeat() {

      /* Random back-off */
      boost::this_thread::sleep(boost::posix_time::milliseconds(rand() % 10));

      while (true) {

        for (vector<StorageInfo*>::iterator it = peers.begin(); it != peers.end(); ++it) {

          StorageInfo* node = *it;

          /* Create heartbeat message, which includes modifications of map */

          string uri = node->get_node_uri();

          /* Send it */


        }


      }


    }







  } // namespace store
} // namespace firmament
