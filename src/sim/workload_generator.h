// TODO

#ifndef FIRMAMENT_SIM_WORKLOAD_GENERATOR_H
#define FIRMAMENT_SIM_WORKLOAD_GENERATOR_H

#include <glog/logging.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_smallint.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <boost/random/lognormal_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace firmament {

class WorkloadGenerator {
 public:
  WorkloadGenerator();
  double GetNextInterarrivalTime();
  uint64_t GetNextJobSize();
  uint32_t GetNextJobType();
  double GetNextTaskDuration();
 private:
  typedef boost::mt19937 base_generator_type;
  typedef boost::exponential_distribution<> job_arrival_dist_type;
  typedef boost::exponential_distribution<> job_size_dist_type;
  typedef boost::lognormal_distribution<> task_duration_dist_type;
  base_generator_type gen_;

  boost::exponential_distribution<> job_arrival_distribution_;
  boost::variate_generator<base_generator_type&,
      job_arrival_dist_type> job_arrival_gen_;

  boost::exponential_distribution<> job_size_distribution_;
  boost::variate_generator<base_generator_type&,
      job_size_dist_type> job_size_gen_;

  boost::uniform_smallint<> job_type_distribution_;
  boost::variate_generator<base_generator_type&,
      boost::uniform_smallint<> > job_type_gen_;

  boost::lognormal_distribution<> task_duration_distribution_;
  boost::variate_generator<base_generator_type&,
      task_duration_dist_type> task_duration_gen_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_WORKLOAD_GENERATOR_H
