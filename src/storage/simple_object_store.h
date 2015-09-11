// The Firmament project
// Copyright (c) 2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Header for object store. This currently supports a very simple in-memory
// key-value store.

#ifndef FIRMAMENT_ENGINE_SIMPLE_OBJECT_STORE_H
#define FIRMAMENT_ENGINE_SIMPLE_OBJECT_STORE_H

#include <set>
#include <string>
#include <map>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/lexical_cast.hpp>
#endif

#include "base/common.h"
#include "base/job_desc.pb.h"
#include "base/reference_desc.pb.h"
#include "base/resource_desc.pb.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/types.h"
#include "messages/base_message.pb.h"
#include "messages/storage_message.pb.h"
#include "misc/map-util.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_channel.h"
#include "storage/types.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace store {

using boost::lexical_cast;
using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsAdapter;

class SimpleObjectStore : public ObjectStoreInterface {
 public:
  explicit SimpleObjectStore(ResourceID_t uuid);
  ~SimpleObjectStore();

  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<SimpleObjectStore, containing "
                   << object_table_->size() << " objects>";
  }
};

} // namespace store
} // namespace firmament

#endif  // FIRMAMENT_ENGINE_SIMPLE_OBJECT_STORE_H
