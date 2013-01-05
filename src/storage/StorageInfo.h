/* 
 * File:   StorageInfo.h
 * Author: nscc2
 *
 * Created on 04 December 2012, 15:14
 * 
 * Class which includes information about other storage nodes
 * Latency / Bandwidth (if necessary)
 * Whether same coordinator or not 
 * Whether had to look for objects there already 
 * 
 */

#ifndef STORAGEINFO_H
#define	STORAGEINFO_H

#include <string>
#include <map>

#include "base/common.h"
#include "base/types.h"

#include "storage/types.h"
#include "storage/StorageInfo.h"

#include "base/reference_desc.pb.h"

#include "platforms/unix/common.h"


#include "misc/messaging_interface.h"

#include "platforms/common.h"

#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_adapter-inl.h"

#include "platforms/unix/stream_sockets_channel.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

#include "messages/base_message.pb.h"
#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/reference_desc.pb.h"
#include "base/resource_desc.pb.h"
#include "base/resource_topology_node_desc.pb.h"



namespace firmament {
  namespace store {

    using namespace std;
    using platform_unix::streamsockets::StreamSocketsChannel;
    using platform_unix::streamsockets::StreamSocketsAdapter;

    class StorageInfo {
    public:
      StorageInfo();
      StorageInfo(const string& node_uri, const string& uuid, const string& coo_uuid, shared_ptr<StreamSocketsChannel<BaseMessage> > chan);
      StorageInfo(const StorageInfo& orig);
      ~StorageInfo();

      inline string get_node_uri() {
        return node_uri;
      }

      inline string get_coordinator_uuid() {
        return coordinator_uuid;
      }

      inline long get_average_rtt() {
        return average_rtt;
      }

      inline string get_resource_uuid() {
        return uuid;
      }

    private:

      string node_uri; /* Address of node*/

      string uuid; /* Resource ID  */

      string coordinator_uuid; /* UUID of coordinator if known */

      long int average_rtt; /* Average RTT to contact node */

      boost::shared_ptr<StreamSocketsChannel<BaseMessage> > chan;


    };

  } //namespace store
} //namespace firmament

#endif	/* STORAGEINFO_H */

