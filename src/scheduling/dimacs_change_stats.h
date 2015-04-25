#ifndef SRC_SCHEDULING_DIMACS_CHANGE_STATS_H_
#define SRC_SCHEDULING_DIMACS_CHANGE_STATS_H_

#include <vector>

#include "scheduling/dimacs_change.h"

namespace firmament {

struct DIMACSChangeStats {
public:
	unsigned int total;
	unsigned int new_node, remove_node;
	unsigned int new_arc, change_arc, remove_arc;

	DIMACSChangeStats(std::vector<DIMACSChange *> &changes);
	virtual ~DIMACSChangeStats();
};

} /* namespace firmament */

#endif /* SRC_SCHEDULING_DIMACS_CHANGE_STATS_H_ */
