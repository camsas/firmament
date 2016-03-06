// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <gtest/gtest.h>

#include "base/common.h"
#include "misc/map-util.h"
#include "misc/wall_time.h"
#include "misc/utils.h"
#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/flow/flow_graph_node.h"
#include "scheduling/flow/trivial_cost_model.h"

DECLARE_string(flow_scheduling_solver);
DECLARE_uint64(num_pref_arcs_task_to_res);

namespace firmament {

class FlowGraphManagerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FlowGraphManagerTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
    resource_map_ = shared_ptr<ResourceMap_t>(new ResourceMap_t);
    task_map_ = shared_ptr<TaskMap_t>(new TaskMap_t);
    leaf_res_ids_ =
      new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
    tg_ = new TraceGenerator(&wall_time_);
    graph_manager_ = new FlowGraphManager(
        new TrivialCostModel(resource_map_, task_map_, leaf_res_ids_),
        leaf_res_ids_, &wall_time_, tg_, &dimacs_stats_);
  }

  virtual ~FlowGraphManagerTest() {
    // You can do clean-up work that doesn't throw exceptions here.
    delete leaf_res_ids_;
    delete tg_;
    delete graph_manager_;
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests.
  void CreateSimpleResourceTopo(ResourceTopologyNodeDescriptor *rtn_root) {
    string root_id = to_string(GenerateResourceID("test"));
    rtn_root->mutable_resource_desc()->set_uuid(root_id);
    rtn_root->mutable_resource_desc()->set_type(
        ResourceDescriptor::RESOURCE_MACHINE);
    ResourceTopologyNodeDescriptor* rtn_c1 = rtn_root->add_children();
    string c1_uid = to_string(GenerateResourceID("test-c1"));
    rtn_c1->mutable_resource_desc()->set_uuid(c1_uid);
    rtn_c1->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
    rtn_c1->set_parent_id(root_id);
    ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root->add_children();
    string c2_uid = to_string(GenerateResourceID("test-c2"));
    rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
    rtn_c2->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
    rtn_c2->set_parent_id(root_id);
  }

  shared_ptr<ResourceMap_t> resource_map_;
  shared_ptr<TaskMap_t> task_map_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  DIMACSChangeStats dimacs_stats_;
  WallTime wall_time_;
  TraceGenerator* tg_;
  FlowGraphManager* graph_manager_;
};

TEST_F(FlowGraphManagerTest, AddOrUpdateJobNodes) {
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  graph_manager_->AddResourceTopology(&rtn_root);
  // Now generate a job and add it
  JobID_t jid = GenerateJobID();
  JobDescriptor test_job;
  test_job.set_uuid(to_string(jid));
  TaskDescriptor* rt = test_job.mutable_root_task();
  rt->set_state(TaskDescriptor::RUNNABLE);
  rt->set_uid(GenerateRootTaskID(test_job));
  rt->set_job_id(test_job.uuid());
  CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
  uint32_t num_changes = graph_manager_->graph_changes_.size();
  vector<JobDescriptor*> jd_ptr_vect;
  jd_ptr_vect.push_back(&test_job);
  graph_manager_->AddOrUpdateJobNodes(jd_ptr_vect);
  // This should add one new node for the agg, one arc change from agg to
  // sink, one new node for the root task, one new node for the
  // equivalence class and one new node for the cluster equiv class.
  CHECK_EQ(graph_manager_->graph_changes_.size(), num_changes + 6);
  DIMACSAddNode* unsched_agg =
    static_cast<DIMACSAddNode*>(graph_manager_->graph_changes_[num_changes]);
  DIMACSChangeArc* arc_to_sink =
    static_cast<DIMACSChangeArc*>(
      graph_manager_->graph_changes_[num_changes + 1]);
  DIMACSAddNode* root_task =
    static_cast<DIMACSAddNode*>(graph_manager_->graph_changes_[num_changes + 2]);
  DIMACSAddNode* equiv_class =
    static_cast<DIMACSAddNode*>(graph_manager_->graph_changes_[num_changes + 3]);

  CHECK_EQ(arc_to_sink->cap_upper_bound_, 1);
  // Arc to sink.
  CHECK_EQ(unsched_agg->arc_additions_.size(), 1);
  // Arc to unscheduled aggregator and to topology.
  CHECK_EQ(root_task->arc_additions_.size(), 2);
  // The equiv class won't have any arcs
  CHECK_EQ(equiv_class->arc_additions_.size(), 0);
}

TEST_F(FlowGraphManagerTest, AddOrUpdateResourceNode) {
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  graph_manager_->AddResourceTopology(&rtn_root);
  string root_id = rtn_root.mutable_resource_desc()->uuid();
  uint32_t num_changes = graph_manager_->graph_changes_.size();
  // Add a new core node.
  ResourceTopologyNodeDescriptor* core_node = rtn_root.add_children();
  string core_uid = to_string(GenerateResourceID());
  core_node->mutable_resource_desc()->set_uuid(core_uid);
  core_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  core_node->set_parent_id(root_id);
  graph_manager_->AddOrUpdateResourceNode(core_node);
  // Check if the number of changes is correct. One change for adding the new
  // node, another change for adding an arc from the node to the sink and
  // another change for update the arc from the parent to the core.
  CHECK_EQ(graph_manager_->graph_changes_.size(), num_changes + 3);
}

TEST_F(FlowGraphManagerTest, DeleteReAddResourceTopoAndJob) {
  // We have to set the solver not to be cs2 so that we actually adjust
  // NumNodes when we delete a node.
  FLAGS_flow_scheduling_solver = "flowlessly";
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  uint64_t num_nodes =
    graph_manager_->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager_->graph_change_manager_->flow_graph().NumArcs();
  graph_manager_->AddResourceTopology(&rtn_root);
  // Now generate a job and add it
  JobID_t jid = GenerateJobID();
  JobDescriptor test_job;
  test_job.set_uuid(to_string(jid));
  TaskDescriptor* rt = test_job.mutable_root_task();
  rt->set_state(TaskDescriptor::RUNNABLE);
  rt->set_uid(GenerateRootTaskID(test_job));
  rt->set_job_id(test_job.uuid());
  CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
  vector<JobDescriptor*> jd_ptr_vect;
  jd_ptr_vect.push_back(&test_job);
  graph_manager_->AddOrUpdateJobNodes(jd_ptr_vect);
  // Three resource nodes, plus one task, plus unsched aggregator,
  // plus cluster aggregator EC, plus task EC
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes + 7);
  // Two internal to topology, two to sink, one from cluster agg EC to
  // root resource, one from task to cluster agg, one from task to
  // preferred ec, one from prefered ec to resource, one from task to
  // unscheduled aggregator, one from unscheduled aggregator to sink
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs + 10);
  // Job "finishes"
  rt->set_state(TaskDescriptor::COMPLETED);
  graph_manager_->JobCompleted(jid);
  // Three resource nodes (cluster agg EC has been deleted, as it no longer
  // has any outgoing arcs).
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes + 3);
  // Two internal to topology, two to sink
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs + 4);
  for (auto it = rtn_root.mutable_children()->begin();
       it != rtn_root.mutable_children()->end();
       ++it) {
    FlowGraphNode* n =
      graph_manager_->NodeForResourceID(
          ResourceIDFromString(it->resource_desc().uuid()));
    graph_manager_->DeleteResourceNode(n);
  }
  FlowGraphNode* n =
    graph_manager_->NodeForResourceID(
        ResourceIDFromString(rtn_root.resource_desc().uuid()));
  graph_manager_->DeleteResourceNode(n);
  // Everything should be as in the beginning
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes);
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs);
  graph_manager_->AddResourceTopology(&rtn_root);
  // Three resource nodes, plus cluster aggregator EC
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes + 4);
  // Two internal to topology, two to sink, one from cluster agg EC to
  // root resource
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs + 5);
}

TEST_F(FlowGraphManagerTest, DeleteResourceNode) {
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  graph_manager_->AddResourceTopology(&rtn_root);
  string root_id = rtn_root.mutable_resource_desc()->uuid();
  // Add a new core node.
  ResourceTopologyNodeDescriptor* core_node = rtn_root.add_children();
  string core_uid = to_string(GenerateResourceID());
  core_node->mutable_resource_desc()->set_uuid(core_uid);
  core_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  core_node->set_parent_id(root_id);
  graph_manager_->AddOrUpdateResourceNode(core_node);
  uint32_t num_changes = graph_manager_->graph_changes_.size();
  // Delete the core node.
  graph_manager_->DeleteResourceNode(graph_manager_->NodeForResourceID(
       ResourceIDFromString(core_node->resource_desc().uuid())));
  CHECK_EQ(graph_manager_->graph_changes_.size(), num_changes + 1);
}

TEST_F(FlowGraphManagerTest, DeleteReAddResourceTopo) {
  // We have to set the solver not to be cs2 so that we actually adjust
  // NumNodes when we delete a node.
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  uint64_t num_nodes =
    graph_manager_->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager_->graph_change_manager_->flow_graph().NumArcs();
  graph_manager_->AddResourceTopology(&rtn_root);
  // Three resource nodes, plus cluster aggregator EC
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes + 4);
  // Two internal to topology, two to sink, one from cluster agg EC to
  // root resource
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs + 5);
  for (auto it = rtn_root.mutable_children()->begin();
       it != rtn_root.mutable_children()->end();
       ++it) {
    FlowGraphNode* n =
      graph_manager_->NodeForResourceID(
          ResourceIDFromString(it->resource_desc().uuid()));
    graph_manager_->DeleteResourceNode(n);
  }
  // Still have cluster agg and topology root here
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes + 2);
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs + 1);
  FlowGraphNode* n =
    graph_manager_->NodeForResourceID(
        ResourceIDFromString(rtn_root.resource_desc().uuid()));
  graph_manager_->DeleteResourceNode(n);
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes);
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs);
  graph_manager_->AddResourceTopology(&rtn_root);
  // Three resource nodes, plus cluster aggregator EC
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
           num_nodes + 4);
  // Two internal to topology, two to sink, one from cluster agg EC to
  // root resource
  CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
           num_arcs + 5);
}

// Add simple resource topology to graph
TEST_F(FlowGraphManagerTest, SimpleResourceTopo) {
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  graph_manager_->AddResourceTopology(&rtn_root);
}

// Test correct increment/decrement of unscheduled aggregator capacities.
TEST_F(FlowGraphManagerTest, UnschedAggCapacityAdjustment) {
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  graph_manager_->AddResourceTopology(&rtn_root);
  // Now generate a job and add it
  JobID_t jid = GenerateJobID();
  JobDescriptor test_job;
  test_job.set_uuid(to_string(jid));
  TaskDescriptor* rt = test_job.mutable_root_task();
  rt->set_state(TaskDescriptor::RUNNABLE);
  rt->set_uid(GenerateRootTaskID(test_job));
  rt->set_job_id(test_job.uuid());
  CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
  vector<JobDescriptor*> jd_ptr_vect;
  jd_ptr_vect.push_back(&test_job);
  graph_manager_->AddOrUpdateJobNodes(jd_ptr_vect);
  // Grab the unscheduled aggregator for the new job
  FlowGraphNode* unsched_agg_node =
    FindPtrOrNull(graph_manager_->job_unsched_to_node_, jid);
  CHECK_NOTNULL(unsched_agg_node);
  FlowGraphArc* unsched_agg_to_sink_arc = FindPtrOrNull(
      unsched_agg_node->outgoing_arc_map_,
      graph_manager_->sink_node_->id_);
  CHECK_NOTNULL(unsched_agg_to_sink_arc);
  CHECK_EQ(unsched_agg_to_sink_arc->cap_upper_bound_, 1);
  // Now pin the root task to the first resource leaf
  FlowGraphNode* root_task_node =
      FindPtrOrNull(graph_manager_->task_to_node_map_,
                    test_job.root_task().uid());
  CHECK_NOTNULL(root_task_node);
  FlowGraphNode* resource_node =
      FindPtrOrNull(graph_manager_->resource_to_node_map_, ResourceIDFromString(
          rtn_root.mutable_children()->Get(0).resource_desc().uuid()));
  CHECK_NOTNULL(resource_node);
  graph_manager_->PinTaskToNode(root_task_node, resource_node);
  // The unscheduled aggregator's outbound capacity should have been
  // decremented.
  CHECK_EQ(unsched_agg_to_sink_arc->cap_upper_bound_, 0);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
