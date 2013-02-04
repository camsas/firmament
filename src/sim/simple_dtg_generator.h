// TODO

#ifndef FIRMAMENT_SIM_SIMPLE_DTG_GENERATOR_H
#define FIRMAMENT_SIM_SIMPLE_DTG_GENERATOR_H

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/lognormal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <boost/random/uniform_smallint.hpp>
/*#include <boost/random/exponential_distribution.hpp>*/

#include "base/common.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"

namespace firmament {
namespace sim {

class SimpleDTGGenerator {
 public:
  explicit SimpleDTGGenerator(JobDescriptor* jd);
  void Run();
 private:
  typedef boost::mt19937 base_generator_type;
  //typedef boost::exponential_distribution<> job_size_dist_type;
  typedef boost::lognormal_distribution<> task_spawn_dist_type;

  /*boost::exponential_distribution<> job_size_distribution_;
  boost::variate_generator<base_generator_type&,
      job_size_dist_type> job_size_gen_;*/

  boost::uniform_smallint<> output_id_distribution_;
  boost::variate_generator<base_generator_type&,
      boost::uniform_smallint<> > output_id_gen_;

  boost::uniform_smallint<> spawner_distribution_;
  boost::variate_generator<base_generator_type&,
      boost::uniform_smallint<> > spawner_gen_;

  boost::lognormal_distribution<> task_spawn_distribution_;
  boost::variate_generator<base_generator_type&,
      task_spawn_dist_type> task_spawn_gen_;
  JobDescriptor* jd_;
  base_generator_type gen_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMPLE_DTG_GENERATOR_H
