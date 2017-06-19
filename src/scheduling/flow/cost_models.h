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

// Handy convenience header that includes all known cost models.

#ifndef FIRMAMENT_SCHEDULING_FLOW_COST_MODELS_H
#define FIRMAMENT_SCHEDULING_FLOW_COST_MODELS_H

// Cost model interface
#include "scheduling/flow/cost_model_interface.h"
// Concrete cost models
#include "scheduling/flow/coco_cost_model.h"
#include "scheduling/flow/net_cost_model.h"
#include "scheduling/flow/octopus_cost_model.h"
#include "scheduling/flow/quincy_cost_model.h"
#include "scheduling/flow/quincy_interference_cost_model.h"
#include "scheduling/flow/random_cost_model.h"
#include "scheduling/flow/sjf_cost_model.h"
#include "scheduling/flow/trivial_cost_model.h"
#include "scheduling/flow/void_cost_model.h"
#include "scheduling/flow/wharemap_cost_model.h"

#endif  // FIRMAMENT_SCHEDULING_FLOW_COST_MODELS_H
