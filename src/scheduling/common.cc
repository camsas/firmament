// The Firmament project
// Copyright (c) 2014 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/common.h"

DEFINE_int64(flow_max_arc_cost, 1000, "The maximum cost of an arc");
DEFINE_uint64(num_pref_arcs_task_to_res, 1,
             "Number of preference arcs from task to resources");
DEFINE_uint64(num_pref_arcs_agg_to_res, 2,
             "Number of preference arcs from equiv class to resources");

namespace firmament {

}
