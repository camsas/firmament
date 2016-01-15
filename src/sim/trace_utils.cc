// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "sim/trace_utils.h"

#include <fcntl.h>

#include <boost/filesystem.hpp>

#include <algorithm>

#include "base/units.h"
#include "misc/utils.h"

DEFINE_string(machine_tmpl_file, "../../tests/testdata/machine_topo.pbin",
              "File specifying machine topology. (Note: the given path must be "
              "relative to the directory of the binary)");

namespace firmament {
namespace sim {

void LoadMachineTemplate(ResourceTopologyNodeDescriptor* machine_tmpl) {
  boost::filesystem::path machine_tmpl_path(FLAGS_machine_tmpl_file);
  if (machine_tmpl_path.is_relative()) {
    // lookup file relative to directory of binary, not CWD
    char binary_path[1024];
    size_t bytes = ExecutableDirectory(binary_path, sizeof(binary_path));
    CHECK(bytes < sizeof(binary_path));
    boost::filesystem::path binary_path_boost(binary_path);
    binary_path_boost.remove_filename();

    machine_tmpl_path = binary_path_boost / machine_tmpl_path;
  }

  string machine_tmpl_fname(machine_tmpl_path.string());
  LOG(INFO) << "Loading machine descriptor from " << machine_tmpl_fname;
  int fd = open(machine_tmpl_fname.c_str(), O_RDONLY);
  if (fd < 0) {
    PLOG(FATAL) << "Could not load " << machine_tmpl_fname;
  }
  machine_tmpl->ParseFromFileDescriptor(fd);
  close(fd);
}

EventDescriptor_EventType TranslateMachineEvent(
    int32_t machine_event) {
  if (machine_event == MACHINE_ADD) {
    return EventDescriptor::ADD_MACHINE;
  } else if (machine_event == MACHINE_REMOVE) {
    return EventDescriptor::REMOVE_MACHINE;
  } else if (machine_event == MACHINE_UPDATE) {
    return EventDescriptor::UPDATE_MACHINE;
  } else {
    LOG(FATAL) << "Unexpected machine event type: " << machine_event;
  }
}

}  // namespace sim
}  // namespace firmament
