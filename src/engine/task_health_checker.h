// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_ENGINE_TASK_HEALTH_CHECKER_H
#define FIRMAMENT_ENGINE_TASK_HEALTH_CHECKER_H

#include <string>
#include <map>
#include <vector>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#include <boost/thread/lockable_concepts.hpp>
#else
#error Boost not available!
#endif

#include "base/common.h"
#include "base/types.h"

namespace firmament {

class TaskHealthChecker {
 public:
  TaskHealthChecker(
      const unordered_map<TaskID_t, boost::thread*>* handler_thread_map,
      boost::shared_mutex* handler_map_lock);
  bool Run(vector<TaskID_t>* failed_tasks);

 protected:
  bool CheckTaskLiveness(TaskID_t task_id, boost::thread* handler_thread);

  const unordered_map<TaskID_t, boost::thread*>* handler_thread_map_;
  boost::shared_mutex* handler_map_lock_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_TASK_HEALTH_CHECKER_H
