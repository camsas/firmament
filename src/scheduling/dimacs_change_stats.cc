#include "scheduling/dimacs_change_stats.h"

#include "scheduling/dimacs_add_node.h"
#include "scheduling/dimacs_remove_node.h"

#include "scheduling/dimacs_new_arc.h"
#include "scheduling/dimacs_change_arc.h"

namespace firmament {

template<typename Base>
bool isInstanceOf(DIMACSChange* x) {
	Base *base = dynamic_cast<Base *>(x);
	return base != nullptr;
}

DIMACSChangeStats::DIMACSChangeStats(std::vector<DIMACSChange *> &changes) {
	total = changes.size();
	new_node = remove_node = new_arc = change_arc = remove_arc = 0;
	for (DIMACSChange *chg : changes) {
		if (isInstanceOf<DIMACSAddNode>(chg)) {
			new_node++;
		} else if (isInstanceOf<DIMACSRemoveNode>(chg)) {
			remove_node++;
		} else if (isInstanceOf<DIMACSNewArc>(chg)) {
			new_arc++;
		} else if (isInstanceOf<DIMACSChangeArc>(chg)) {
			DIMACSChangeArc *arc_chg = dynamic_cast<DIMACSChangeArc *>(chg);
			if (arc_chg->upper_bound() > 0) {
				change_arc++;
			} else {
				remove_arc++;
			}
		} else {
			LOG(WARNING) << "Unrecognised DIMACS change.";
		}
	}
}

DIMACSChangeStats::~DIMACSChangeStats() { }

} /* namespace firmament */
