/* 
 * File:   StorageInfo.cpp
 * Author: nscc2
 * 
 * Created on 04 December 2012, 15:14
 */

#include "StorageInfo.h"
#include "platforms/unix/stream_sockets_channel.h"
#include "messages/base_message.pb.h"

namespace firmament {
    
    namespace store {
        
        using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsAdapter;


StorageInfo::StorageInfo():node_uri (""),
                uuid(""), 
        coordinator_uuid(""),
        average_rtt(0)
        {
    chan.reset() ; 
}

StorageInfo::StorageInfo(const string& node_uri_, const string& uuid_,
        const string& coo_uuid_, 
        shared_ptr<StreamSocketsChannel<BaseMessage> > chan_):
        node_uri(node_uri_),
        uuid(uuid_),
        coordinator_uuid(coo_uuid_),
        average_rtt(0), 
    chan(chan_) 
        { 
}

StorageInfo::StorageInfo(const StorageInfo& orig) {
}

StorageInfo::~StorageInfo() {
}

    } //namespace store
} // namespace firmament
