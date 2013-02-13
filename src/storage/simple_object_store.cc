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

            /* TODO Find out what public interface is.*/
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
             are local.
             */

            shared_ptr<StreamSocketsChannel<BaseMessage> > chan(
                    new StreamSocketsChannel<BaseMessage > (
                    StreamSocketsChannel<BaseMessage>::SS_TCP));
            message_adapter_->EstablishChannel(msg.storage_interface(), chan);


            StorageInfo* node = new StorageInfo(msg.storage_interface(), msg.uuid(),  msg.has_coordinator_uuid()? msg.coordinator_uuid() : "", chan ) ; 
         //   StorageInfo* node = new StorageInfo();

            if (msg.peer()) InsertIfNotPresent(&peers, node->get_node_uri(), node);
            InsertIfNotPresent(&nodes, node->get_node_uri(), node); 

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
            for (NodeTable_t::iterator it = peers.begin(); it != peers.end(); ++it) {
                StorageInfo* inf = it->second;
                VLOG(3) << "Resource ID : " + inf->get_resource_uuid() + " URI " + inf->get_node_uri() << endl;

            }
            VLOG(3) << "Nodes " << endl;

            for (NodeTable_t::iterator it = nodes.begin(); it != nodes.end(); ++it) {
                StorageInfo* inf = it->second;
                VLOG(3) << "Resource ID : " + inf->get_resource_uuid() + " URI " + inf->get_node_uri();

            }

        }

        void SimpleObjectStore::createSharedBuffer(size_t size) {
            VLOG(3) << "Creating Shared Buffer Size " << size << endl;
            /* NB: currently assuming only one object store per machine */
            string str = ("Cache");
            cache.reset(new Cache(this, size, str));
        }

        void SimpleObjectStore::obtain_object_remotely(DataObjectID_t id) {
            VLOG(3) << "Obtaining object remotely " << endl;

            /* Find out location of object (could be on disk)*/

            ReferenceDescriptor* rd = GetReference(id);

            /* Either know of location in which case contact directly 
             * If more than one location infer which one is best 
             * Otherwise contact peers and master. */

            if (rd == NULL) {
                /* Location not known, send message to master first as likely that
                 if peers had had it, would have known about it.  Master will broadcast messaage
                 to all (if object is being used, likely that will be used again,
                 so good that master knows where object is*/


                /* Maybe contact peers? */
            }
            else {
                /* Location exists. Infer most efficient way to obtain object
                 May just be local */
                StorageInfo* info = infer_best_location(rd);
                if (info==NULL) { 
                    LOG(ERROR) << "Error: Object Table is inconsistent " << endl; 
                    /* Fall back to master */
                   
                }
                else { 
                  /* Send an object request message */                    
                    ObtainObjectMessage* msg = new ObtainObjectMessage() ; 
                    msg->set_sender_uri(listening_interface_); 
                    msg->set_sender_uuid(lexical_cast<string>(uuid)); 
                    msg->set_object_id(id); 
                }
            }


            /* Message will be received in MessageAdapter. Maybe block on condition here? */

        }

        StorageInfo* SimpleObjectStore::infer_best_location(ReferenceDescriptor* rd) {
            VLOG(3) << "Infer_best_location " << endl;
            VLOG(3) << "Unimplemented, returning first location " << endl;
            
            string uri = rd->location(0); 
            
            StorageInfo* inf = *FindOrNull(nodes,uri); 
            return inf; 
        }

        void SimpleObjectStore::HeartBeatTask() {
            VLOG(3) << "Setting up heartbeat thread  " << endl;

            boost::thread t(&SimpleObjectStore::sendHeartbeat, *this);

        }

        void SimpleObjectStore::HeartBeatMasterTask() {
            boost::thread t(&SimpleObjectStore::sendHeartBeatMasterTask, *this);

        }

        void SimpleObjectStore::sendHeartBeatMasterTask() {
            VLOG(3) << "Unimplemented " << endl;
        }

        void SimpleObjectStore::sendHeartbeat() {

            /* Random back-off */
            boost::this_thread::sleep(boost::posix_time::milliseconds(rand() % 10));

            while (true) {
                /* Create heartbeat message, which includes modifications of map */
                    StorageHeartBeatMessage hb;
                    hb.set_uri(listening_interface_);
                    /* Is this cast safe? */
                    hb.set_uuid(lexical_cast<string>(uuid));        
                for (DataObjectMap_t::iterator it = object_table_->begin(); it != object_table_->end(); ++it) {

                    ReferenceDescriptor* rd = it->second ; 
                    if (!rd->is_modified()) continue ; 
                    else { 
                        ObjectInfoMessage msg;
                        msg.set_obj_name(rd->id()); 
                        int loc_size = rd->location_size() ; 
                        int i = 0; 
                        while (i<loc_size) { 
                            msg.add_locations(rd->location(i++)); 
                        } 
                        /* Check */
                        ObjectInfoMessage* m = hb.add_object(); 
                        m = &msg ; 
                    }
                    
                }

                for (NodeTable_t::iterator it = peers.begin(); it != peers.end(); ++it) {

                    StorageInfo* node = it->second;

                    string uri = node->get_node_uri();

                    VLOG(3) << "Creating Heartbeat for  node " << uri << endl;
                    
                    message_adapter_->SendMessageToEndpoint(uri, (BaseMessage&)hb) ; 

                    /* TODO: optimisations 1) ensure that not sending redundant data 
                     to peer (if it was peer that sent us the new data, etc.) 2) Find better
                     way to encode message*/

                    /* TODO, modulate sleep time best on usage, etc?*/
                    boost::this_thread::sleep(boost::posix_time::milliseconds(500));
                }

            }

        }







    } // namespace store
} // namespace firmament
