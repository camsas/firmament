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

#include <gtest/gtest.h>

#include "scheduling/label_utils.h"
#include "misc/utils.h"

namespace firmament {
namespace scheduler {

class LabelUtilsTest : public ::testing::Test {
protected:
  LabelUtilsTest() {
    // You can do initial set-up work for each test here.
  }

  virtual ~LabelUtilsTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  void CreateResourceWithLabels(ResourceTopologyNodeDescriptor* rtnd_ptr, string machine_name, const char* key, const char* value) {
    ResourceID_t res_id = GenerateResourceID(machine_name);
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    rd_ptr->set_uuid(to_string(res_id));
    rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
    //Adding label on index 0.
    Label* rd_label0 = rd_ptr->add_labels();
    rd_label0->set_key(key);
    rd_label0->set_value(value);
  }

  TaskDescriptor* CreateTaskWithLabels(JobDescriptor* jd_ptr, uint64_t job_id_seed, const char* key, const char* value) {
    JobID_t job_id = GenerateJobID(job_id_seed);
    jd_ptr->set_uuid(to_string(job_id));
    jd_ptr->set_name(to_string(job_id));
    TaskDescriptor* td_ptr = jd_ptr->mutable_root_task();
    td_ptr->set_uid(GenerateRootTaskID(*jd_ptr));
    td_ptr->set_job_id(jd_ptr->uuid());
    //Adding label on index 0.
    Label* td_label0 = td_ptr->add_labels();
    td_label0->set_key(key);
    td_label0->set_value(value);
    return td_ptr;
  }
};


TEST_F(LabelUtilsTest, SatisfiesLabelSelector) {
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  //Checking resource label.
  CHECK_STREQ(rtnd.mutable_resource_desc()->mutable_labels(0)->key().c_str(), "1");
  CHECK_STREQ(rtnd.mutable_resource_desc()->mutable_labels(0)->value().c_str(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor *td_ptr = CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  //Checking task label.
  CHECK_STREQ(td_ptr->mutable_labels(0)->key().c_str(), "ID_NUM");
  CHECK_STREQ(td_ptr->mutable_labels(0)->value().c_str(), "ID_STRING");

  //Case1: selector type IN_SET, key match & values match
  //Adding label selector index 0 for task.
  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("1");
  label_selector0->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(0)->key().c_str(), "1");
  CHECK_STREQ(td_ptr->mutable_label_selectors(0)->values(0).c_str(), "One");
  bool ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);

  //Case2: selector type IN_SET, key match & values doesn't match.
  //Adding label selector index 1 for task.
  LabelSelector* label_selector1 = td_ptr->add_label_selectors();
  label_selector1->set_type(LabelSelector::IN_SET);
  label_selector1->set_key("1");
  label_selector1->add_values("Two");
  CHECK_STREQ(td_ptr->mutable_label_selectors(1)->key().c_str(), "1");
  CHECK_STREQ(td_ptr->mutable_label_selectors(1)->values(0).c_str(), "Two");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(1));
  CHECK_EQ(ret, false);

  //Case3: selector type IN_SET, key doesn't match & values match.
  //Adding label selector index 2 for task.
  LabelSelector* label_selector2 = td_ptr->add_label_selectors();
  label_selector2->set_type(LabelSelector::IN_SET);
  label_selector2->set_key("2");
  label_selector2->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(2)->key().c_str(), "2");
  CHECK_STREQ(td_ptr->mutable_label_selectors(2)->values(0).c_str(), "One");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(2));
  CHECK_EQ(ret, false);

  //Case4: selector type NOT_IN_SET, key match & values match.
  //Adding label selector index 3 for task.
  LabelSelector* label_selector3 = td_ptr->add_label_selectors();
  label_selector3->set_type(LabelSelector::NOT_IN_SET);
  label_selector3->set_key("1");
  label_selector3->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(3)->key().c_str(), "1");
  CHECK_STREQ(td_ptr->mutable_label_selectors(3)->values(0).c_str(), "One");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(3));
  CHECK_EQ(ret, false);

  //Case5: selector type NOT_IN_SET, key match & values doesn't match.
  //Adding label selector index 4 for task.
  LabelSelector* label_selector4 = td_ptr->add_label_selectors();
  label_selector4->set_type(LabelSelector::NOT_IN_SET);
  label_selector4->set_key("1");
  label_selector4->add_values("Two");
  CHECK_STREQ(td_ptr->mutable_label_selectors(4)->key().c_str(), "1");
  CHECK_STREQ(td_ptr->mutable_label_selectors(4)->values(0).c_str(), "Two");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(4));
  CHECK_EQ(ret, true);

  //Case6: selector type NOT_IN_SET, key doesn't match & values match.
  //Adding label selector index 5 for task.
  LabelSelector* label_selector5 = td_ptr->add_label_selectors();
  label_selector5->set_type(LabelSelector::NOT_IN_SET);
  label_selector5->set_key("2");
  label_selector5->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(5)->key().c_str(), "2");
  CHECK_STREQ(td_ptr->mutable_label_selectors(5)->values(0).c_str(), "One");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(5));
  CHECK_EQ(ret, true);

  //Case7: selector type EXISTS_KEY, key exists
  //Adding label selector index 6 for task.
  LabelSelector* label_selector6 = td_ptr->add_label_selectors();
  label_selector6->set_type(LabelSelector::EXISTS_KEY);
  label_selector6->set_key("1");
  label_selector6->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(6)->key().c_str(), "1");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(6));
  CHECK_EQ(ret, true);

  //Case8: selector type EXISTS_KEY, key doesn't exists
  //Adding label selector index 7 for task.
  LabelSelector* label_selector7 = td_ptr->add_label_selectors();
  label_selector7->set_type(LabelSelector::EXISTS_KEY);
  label_selector7->set_key("2");
  label_selector7->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(7)->key().c_str(), "2");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(7));
  CHECK_EQ(ret, false);

  //Case9: selector type NOT_EXISTS_KEY, key exists
  //Adding label selector index 8 for task.
  LabelSelector* label_selector8 = td_ptr->add_label_selectors();
  label_selector8->set_type(LabelSelector::NOT_EXISTS_KEY);
  label_selector8->set_key("1");
  label_selector8->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(8)->key().c_str(), "1");
  CHECK_STREQ(td_ptr->mutable_label_selectors(8)->values(0).c_str(), "One");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(8));
  CHECK_EQ(ret, false);

  //Case10: selector type NOT_EXISTS_KEY, key doesn't exists
  //Adding label selector index 9 for task.
  LabelSelector* label_selector9 = td_ptr->add_label_selectors();
  label_selector9->set_type(LabelSelector::NOT_EXISTS_KEY);
  label_selector9->set_key("2");
  label_selector9->add_values("One");
  CHECK_STREQ(td_ptr->mutable_label_selectors(9)->key().c_str(), "2");
  ret = SatisfiesLabelSelector(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors(9));
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectors) {
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine2", "2", "Two");
  //Checking resource label set.
  CHECK_STREQ(rtnd.mutable_resource_desc()->mutable_labels(0)->key().c_str(), "2");
  CHECK_STREQ(rtnd.mutable_resource_desc()->mutable_labels(0)->value().c_str(), "Two");
  JobDescriptor jd_ptr;
  TaskDescriptor *td_ptr = CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  //Checking task label set.
  CHECK_STREQ(td_ptr->mutable_labels(0)->key().c_str(), "ID_NUM");
  CHECK_STREQ(td_ptr->mutable_labels(0)->value().c_str(), "ID_STRING");
  //label selector 0.
  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("2");
  label_selector0->add_values("Two");
  CHECK_STREQ(td_ptr->mutable_label_selectors(0)->key().c_str(), "2");
  CHECK_STREQ(td_ptr->mutable_label_selectors(0)->values(0).c_str(), "Two");
  //label selector 1.
  LabelSelector* label_selector1 = td_ptr->add_label_selectors();
  label_selector1->set_type(LabelSelector::NOT_IN_SET);
  label_selector1->set_key("3");
  label_selector1->add_values("Three");
  CHECK_STREQ(td_ptr->mutable_label_selectors(1)->key().c_str(), "3");
  CHECK_STREQ(td_ptr->mutable_label_selectors(1)->values(0).c_str(), "Three");
  bool ret = SatisfiesLabelSelectors(*(rtnd.mutable_resource_desc()), td_ptr->label_selectors());
  CHECK_EQ(ret, true);
}

} // namespace scheduler
} // namespace firmament


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
