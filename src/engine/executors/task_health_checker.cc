/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include "engine/executors/task_health_checker.h"

#include <vector>

#if BOOST_VERSION <= 104800
#include <boost/chrono.hpp>
#endif

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
#if BOOST_VERSION <= 104800
  if (handler_thread->timed_join(boost::posix_time::seconds(1)))
    return false;
#else
  if (handler_thread->try_join_for(boost::chrono::seconds(1)))
    return false;
#endif
  return true;
}

}  // namespace firmament
