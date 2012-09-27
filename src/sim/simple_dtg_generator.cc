// TODO

#include "sim/simple_dtg_generator.h"

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#endif

namespace firmament {
namespace sim {

SimpleDTGGenerator::SimpleDTGGenerator(JobDescriptor* jd)
    : spawner_distribution_(0, 100),
      spawner_gen_(gen_, spawner_distribution_),
      task_spawn_distribution_(5, 0.5),
      task_spawn_gen_(gen_, task_spawn_distribution_),
      jd_(jd)
{
}

void SimpleDTGGenerator::Run() {
  LOG(INFO) << "Starting simple DTG generator!";
  CHECK(jd_->has_root_task());
  vector<TaskDescriptor*> tasks;
  tasks.push_back(jd_->mutable_root_task());
  double delay = 0;
  for (uint32_t i = 1; i < 101; ++i) {
    uint32_t spawner_id = spawner_gen_() % i;
    TaskDescriptor* spawner = tasks.at(spawner_id);
    VLOG(1) << "Spawning a new task at " << spawner_id << ", which already "
            << "has " << spawner->spawned_size() << " children.";
    TaskDescriptor* new_task = spawner->add_spawned();
    new_task->set_uid(i);
    new_task->set_name("");
    new_task->set_state(CREATED);
    tasks.push_back(new_task);
    double next_spawn = task_spawn_gen_();
    delay += next_spawn;
    VLOG(1) << "Next task in " << next_spawn << " seconds.";
    boost::this_thread::sleep(boost::posix_time::seconds(next_spawn));
  }
  VLOG(1) << "Mean inter-spawn time: " << (delay / 100);
}

}  // namespace sim
}  // namespace firmament
