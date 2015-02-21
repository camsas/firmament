// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Handy convenience header that includes all known cost models.

#ifndef FIRMAMENT_SCHEDULING_COST_MODELS_H
#define FIRMAMENT_SCHEDULING_COST_MODELS_H

// Cost model interface
#include "scheduling/flow_scheduling_cost_model_interface.h"
// Concrete cost models
#include "scheduling/coco_cost_model.h"
#include "scheduling/quincy_cost_model.h"
#include "scheduling/random_cost_model.h"
#include "scheduling/sjf_cost_model.h"
#include "scheduling/trivial_cost_model.h"
#include "scheduling/wharemap_cost_model.h"

#endif
