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

#ifndef FIRMAMENT_ENGINE_EXECUTORS_TASK_HEALTH_CHECKER_H
#define FIRMAMENT_ENGINE_EXECUTORS_TASK_HEALTH_CHECKER_H

#include <string>
#include <map>
#include <vector>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#if BOOST_VERSION <= 104800
#include <boost/thread/shared_mutex.hpp>
#else
#include <boost/thread/lockable_concepts.hpp>
#endif
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

#endif  // FIRMAMENT_ENGINE_EXECUTORS_TASK_HEALTH_CHECKER_H
