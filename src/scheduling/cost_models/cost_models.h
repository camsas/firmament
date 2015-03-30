// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Handy convenience header that includes all known cost models.

#ifndef FIRMAMENT_SCHEDULING_COST_MODELS_H
#define FIRMAMENT_SCHEDULING_COST_MODELS_H

// Cost model interface
#include "scheduling/cost_models/flow_scheduling_cost_model_interface.h"
// Concrete cost models
#include "scheduling/cost_models/coco_cost_model.h"
#include "scheduling/cost_models/octopus_cost_model.h"
#include "scheduling/cost_models/quincy_cost_model.h"
#include "scheduling/cost_models/random_cost_model.h"
#include "scheduling/cost_models/sjf_cost_model.h"
#include "scheduling/cost_models/trivial_cost_model.h"
#include "scheduling/cost_models/wharemap_cost_model.h"

#endif
