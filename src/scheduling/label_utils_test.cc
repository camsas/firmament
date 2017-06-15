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

#include "misc/utils.h"
#include "scheduling/label_utils.h"

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

  void CreateResourceWithLabels(ResourceTopologyNodeDescriptor* rtnd_ptr,
                                string machine_name, const string& key,
                                const string& value) {
    ResourceID_t res_id = GenerateResourceID(machine_name);
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    rd_ptr->set_uuid(to_string(res_id));
    rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
    // Adding label on index 0.
    Label* rd_label0 = rd_ptr->add_labels();
    rd_label0->set_key(key);
    rd_label0->set_value(value);
  }

  TaskDescriptor* CreateTaskWithLabels(JobDescriptor* jd_ptr,
                                       uint64_t job_id_seed, const string& key,
                                       const string& value) {
    JobID_t job_id = GenerateJobID(job_id_seed);
    jd_ptr->set_uuid(to_string(job_id));
    jd_ptr->set_name(to_string(job_id));
    TaskDescriptor* td_ptr = jd_ptr->mutable_root_task();
    td_ptr->set_uid(GenerateRootTaskID(*jd_ptr));
    td_ptr->set_job_id(jd_ptr->uuid());
    // Adding label on index 0.
    Label* td_label0 = td_ptr->add_labels();
    td_label0->set_key(key);
    td_label0->set_value(value);
    return td_ptr;
  }
};

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorInSetMatch) {
  // Test: Selector type IN_SET, key match & values match
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("1");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "1");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorInSetNoMatch) {
  // Test: selector type IN_SET, key match & values doesn't match.
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("1");
  label_selector0->add_values("Two");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "1");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "Two");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, false);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorInSetNoKeyMatch) {
  // Test: selector type IN_SET, key doesn't match & values match.
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("2");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "2");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, false);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorNotIntSetMatch) {
  // Test: selector type NOT_IN_SET, key match & values match.
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::NOT_IN_SET);
  label_selector0->set_key("1");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "1");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, false);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorNotInSetNotMatch) {
  // Test: selector type NOT_IN_SET, key match & values doesn't match.
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::NOT_IN_SET);
  label_selector0->set_key("1");
  label_selector0->add_values("Two");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "1");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "Two");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorNotIntSetKeyNotMatch) {
  // Test: selector type NOT_IN_SET, key doesn't match & values match.
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::NOT_IN_SET);
  label_selector0->set_key("2");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "2");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorExistsKey) {
  // Test: selector type EXISTS_KEY, key exists
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::EXISTS_KEY);
  label_selector0->set_key("1");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "1");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorExistsKeyNoKey) {
  // Test: selector type EXISTS_KEY, key doesn't exists
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::EXISTS_KEY);
  label_selector0->set_key("2");
  label_selector0->add_values("Two");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "2");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "Two");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, false);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorNotExistsKeyKeyExists) {
  // Test: selector type NOT_EXISTS_KEY, key exists
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::NOT_EXISTS_KEY);
  label_selector0->set_key("1");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "1");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, false);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectorNotExistsKeyNoKey) {
  // Test: selector type NOT_EXISTS_KEY, key doesn't exists
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 44, "ID_NUM", "ID_STRING");
  // Checking task label.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");

  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::NOT_EXISTS_KEY);
  label_selector0->set_key("2");
  label_selector0->add_values("One");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "2");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "One");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectors) {
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine2", "2", "Two");
  // Checking resource label set.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "2");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "Two");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 45, "ID_NUM", "ID_STRING");
  // Checking task label set.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");
  // label selector 0.
  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("2");
  label_selector0->add_values("Two");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "2");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "Two");
  // label selector 1.
  LabelSelector* label_selector1 = td_ptr->add_label_selectors();
  label_selector1->set_type(LabelSelector::NOT_IN_SET);
  label_selector1->set_key("3");
  label_selector1->add_values("Three");
  CHECK_EQ(td_ptr->label_selectors(1).key(), "3");
  CHECK_EQ(td_ptr->label_selectors(1).values(0), "Three");
  bool ret =
      SatisfiesLabelSelectors(rtnd.resource_desc(), td_ptr->label_selectors());
  CHECK_EQ(ret, true);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelector_selectortype_unspecified) {
  // when selector type is not specified i.e. nodeselector case, it should
  // select IN_SET selector type by default.
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine3", "3", "Three");
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "3");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "Three");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 46, "ID_NUM", "ID_STRING");
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");
  // label selector 0.
  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_key("3");
  label_selector0->add_values("Three");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "3");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "Three");
  bool ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(0));
  CHECK_EQ(ret, true);
  // label selector 1.
  LabelSelector* label_selector1 = td_ptr->add_label_selectors();
  label_selector1->set_key("4");
  label_selector1->add_values("Four");
  CHECK_EQ(td_ptr->label_selectors(1).key(), "4");
  CHECK_EQ(td_ptr->label_selectors(1).values(0), "Four");
  ret =
      SatisfiesLabelSelector(rtnd.resource_desc(), td_ptr->label_selectors(1));
  CHECK_EQ(ret, false);
}

TEST_F(LabelUtilsTest, SatisfiesLabelSelectors_multiple_resource_labels) {
  ResourceTopologyNodeDescriptor rtnd;
  CreateResourceWithLabels(&rtnd, "Machine1", "1", "One");
  // Checking resource label0.
  CHECK_EQ(rtnd.resource_desc().labels(0).key(), "1");
  CHECK_EQ(rtnd.resource_desc().labels(0).value(), "One");
  // Adding second label to the same resource.
  Label* rd_label1 = rtnd.mutable_resource_desc()->add_labels();
  rd_label1->set_key("2");
  rd_label1->set_value("Two");
  // Checking resource label1.
  CHECK_EQ(rtnd.resource_desc().labels(1).key(), "2");
  CHECK_EQ(rtnd.resource_desc().labels(1).value(), "Two");
  JobDescriptor jd_ptr;
  TaskDescriptor* td_ptr =
      CreateTaskWithLabels(&jd_ptr, 45, "ID_NUM", "ID_STRING");
  // Checking task label set.
  CHECK_EQ(td_ptr->labels(0).key(), "ID_NUM");
  CHECK_EQ(td_ptr->labels(0).value(), "ID_STRING");
  // label selector 0.
  LabelSelector* label_selector0 = td_ptr->add_label_selectors();
  label_selector0->set_type(LabelSelector::IN_SET);
  label_selector0->set_key("2");
  label_selector0->add_values("Two");
  CHECK_EQ(td_ptr->label_selectors(0).key(), "2");
  CHECK_EQ(td_ptr->label_selectors(0).values(0), "Two");

  bool ret =
      SatisfiesLabelSelectors(rtnd.resource_desc(), td_ptr->label_selectors());
  CHECK_EQ(ret, true);
}

}  // namespace scheduler
}  // namespace firmament

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
