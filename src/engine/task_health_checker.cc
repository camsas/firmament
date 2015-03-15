// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
//
#include "engine/task_health_checker.h"

#include <vector>

#include "misc/map-util.h"
#include "misc/utils.h"

namespace firmament {

TaskHealthChecker::TaskHealthChecker(
    const unordered_map<TaskID_t, boost::thread*>* handler_thread_map,
    boost::shared_mutex* handler_map_lock)
  : handler_thread_map_(handler_thread_map),
    handler_map_lock_(handler_map_lock) {
}

bool TaskHealthChecker::Run(vector<TaskID_t>* failed_tasks) {
  bool all_good = true;
  boost::shared_lock<boost::shared_mutex> map_lock(*handler_map_lock_);
  for (unordered_map<TaskID_t, boost::thread*>::const_iterator
       it = handler_thread_map_->begin();
       it != handler_thread_map_->end();
       ++it) {
    VLOG(2) << "Checking liveness of task " << it->first;
    if (!CheckTaskLiveness(it->first, it->second)) {
      all_good = false;
      LOG(ERROR) << "Task " << it->first << " has failed!";
      failed_tasks->push_back(it->first);
    }
  }
  return all_good;
}

bool TaskHealthChecker::CheckTaskLiveness(TaskID_t task_id,
                                          boost::thread* handler_thread) {
  if (!handler_thread)
    return false;
  return true;
}

}  // namespace firmament
