// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub object store class.


#include "storage/simple_object_store.h"



namespace firmament {
    namespace store {

        //TACH: make non unix specific.

        SimpleObjectStore::SimpleObjectStore(ResourceID_t uuid_): uuid(uuid_)
 {
            message_adapter_.reset(new platform_unix::streamsockets::StreamSocketsAdapter<BaseMessage > ());
            object_table_.reset(new DataObjectMap_t);
            VLOG(1) << "Setting up communications ";

            setUpCommunication();
            
            Cache* cache = new Cache(this, 1024, "Cache" + to_string(uuid));

            VLOG(1) << "End of Test ";

        }

        SimpleObjectStore::~SimpleObjectStore()
 {

            VLOG(2) << "Shutting Storage Engine Down";
            message_adapter_.reset();
            object_table_.reset();

        }

        /* Method occurs when the data was lot located in the cache */
        bool SimpleObjectStore::GetObject(DataObjectID_t id, void* data, size_t* len) {
            VLOG(1) << "Retrieving object " << id << " from store.";
            memset(data, 42, 1);
            *len = sizeof (uint32_t);
            return true;
        }

        /* There is in theory no need for this method. Only ever use 
         shared memory (if cache is full when try to write, then 
         * remove item from cache). Only occurs when size of data is bigger
         * than the whole size of the cache, in which case
         * write directly to disk. 
         */
        void SimpleObjectStore::PutObject(DataObjectID_t id, void* data, size_t len) {
            VLOG(1) << "Adding object " << id << " (size " << len
                    << " bytes) to store.";
        }

        void SimpleObjectStore::HandleStorageRegistrationRequest(const StorageRegistrationMessage& msg) {
            VLOG(1) << "HandleStorageRegistrationRequest " << endl ; 

            /*TODO: currently assume that no two instances of the storage engine
             are local. Therefore must communicate over TCP */
            
            shared_ptr<StreamSocketsChannel<BaseMessage> > chan(
                    new StreamSocketsChannel<BaseMessage > (
                    StreamSocketsChannel<BaseMessage>::SS_TCP));
            message_adapter_->EstablishChannel(msg.storage_interface(), chan) ; 
                    
                    
         //   StorageInfo* node = new StorageInfo(msg.storage_interface(), msg.uuid(),  msg.has_coordinator_uuid()? msg.coordinator_uuid() : "", chan ) ; 
            StorageInfo* node = new StorageInfo() ; 
            
            if (msg.peer()) peers.push_back(node); 
            nodes.push_back(node); 
            
        }

        ReferenceDescriptor* ObjectStoreInterface::GetReference(DataObjectID_t id) {
            return FindOrNull(DataObjectMap_t,id); 
           
        }

        void SimpleObjectStore::HandleIncomingMessage(BaseMessage *bm) {
            VLOG(1) << "Storage Engine: HandleIncomingMessage";
            
            /* Handle Put */
            /* Handle Get */
            /* Handle Request */
            /* Handle Obtain */
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
            VLOG(1) << "Storage Engine: Set Up Communications ";

            listening_interface_ = message_adapter_->Listen();

            VLOG(1) << "Successfully initialised " << listening_interface_;


            message_adapter_->RegisterAsyncMessageReceiptCallback(
                    boost::bind(&SimpleObjectStore::HandleIncomingMessage, this, _1));
            message_adapter_->RegisterAsyncErrorPathCallback(
                    boost::bind(&SimpleObjectStore::HandleIncomingReceiveError, this,
                    boost::asio::placeholders::error, _2));
            VLOG(1) << "Finished creating Adapter ";
        }


        void SimpleObjectStore::printTopology() {
            
            VLOG(1) << "Peers " << endl ; 
            for (vector<StorageInfo*>::iterator it = peers.begin(); it != peers.end(); ++it)  {
                StorageInfo* inf = *it ; 
                VLOG(1) << "Resource ID : " + inf->get_resource_uuid()  +  " URI " + inf->get_node_uri()  << endl ; 
                
            }
            VLOG(1) << "Nodes " << endl ; 

             for (vector<StorageInfo*>::iterator it = nodes.begin(); it != nodes.end(); ++it)  {
                StorageInfo* inf = *it ; 
                VLOG(1) << "Resource ID : " + inf->get_resource_uuid()  +  " URI " + inf->get_node_uri() ; 
                
            }
            
        }
        
        void SimpleObjectStore::createSharedBuffer(size_t size) { 
            
            
        }
        
        void* SimpleObjectStore::obtain_object_remotely(DataObjectID_t id) {
            VLOG(1) << "Obtaining object remotely " << endl ; 
            return (void*) 0 ; 
        }
        


    } // namespace store
} // namespace firmament
