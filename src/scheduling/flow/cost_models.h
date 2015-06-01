// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Handy convenience header that includes all known cost models.

#ifndef FIRMAMENT_SCHEDULING_FLOW_COST_MODELS_H
#define FIRMAMENT_SCHEDULING_FLOW_COST_MODELS_H

// Cost model interface
#include "scheduling/flow/cost_model_interface.h"
// Concrete cost models
#include "scheduling/flow/coco_cost_model.h"
#include "scheduling/flow/octopus_cost_model.h"
#include "scheduling/flow/quincy_cost_model.h"
#include "scheduling/flow/random_cost_model.h"
#include "scheduling/flow/simulated_quincy_cost_model.h"
#include "scheduling/flow/sjf_cost_model.h"
#include "scheduling/flow/trivial_cost_model.h"
#include "scheduling/flow/void_cost_model.h"
#include "scheduling/flow/wharemap_cost_model.h"

#endif  // FIRMAMENT_SCHEDULING_FLOW_COST_MODELS_H
