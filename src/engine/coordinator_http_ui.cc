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

#include "engine/coordinator_http_ui.h"

#include <algorithm>
#include <deque>
#include <set>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>
#include <google/protobuf/util/json_util.h>

#include "base/job_desc.pb.h"
#include "engine/coordinator.h"
#include "misc/utils.h"
#include "misc/string_utils.h"
#include "misc/uri_tools.h"
#include "messages/task_kill_message.pb.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/flow_scheduler.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "storage/types.h"

DECLARE_string(task_log_dir);
DECLARE_string(scheduler);
DECLARE_string(debug_output_dir);
DECLARE_int32(flow_scheduling_cost_model);
DECLARE_bool(debug_flow_graph);

DEFINE_string(http_ui_template_dir, "src/webui",
              "Path to the directory where the web UI templates are located.");

using google::protobuf::util::MessageToJsonString;

namespace firmament {
namespace webui {

#define WEBUI_PERF_QUEUE_LEN 200LL

using boost::lexical_cast;

using ctemplate::TemplateDictionary;

using store::DataObjectMap_t;

CoordinatorHTTPUI::CoordinatorHTTPUI(shared_ptr<Coordinator> coordinator)
  : coordinator_(coordinator),
    active_(true) { }

CoordinatorHTTPUI::~CoordinatorHTTPUI() {
  // Kill the server without waiting for connections to terminate
  if (coordinator_http_server_->is_listening()) {
    coordinator_http_server_->stop(false);
    coordinator_http_server_->join();
    LOG(INFO) << "Coordinator HTTP UI server stopped.";
  }
  LOG(INFO) << "Coordinator HTTP UI server destroyed.";
}

void CoordinatorHTTPUI::AddHeaderToTemplate(TemplateDictionary* dict,
                                            ResourceID_t uuid,
                                            ErrorMessage_t* err) {
  // Global: set web UI port
  dict->SetIntValue("WEBUI_PORT", port_);
  // HTML header
  TemplateDictionary* header_sub_dict = dict->AddIncludeDictionary("HEADER");
  header_sub_dict->SetFilename(FLAGS_http_ui_template_dir + "/header.tpl");
  // Page header
  TemplateDictionary* pgheader_sub_dict =
      dict->AddIncludeDictionary("PAGE_HEADER");
  pgheader_sub_dict->SetFilename(
      FLAGS_http_ui_template_dir + "/page_header.tpl");
  pgheader_sub_dict->SetValue("RESOURCE_ID", to_string(uuid));
  pgheader_sub_dict->SetValue("RESOURCE_HOST", coordinator_->hostname());
  // Statistics for page header
  pgheader_sub_dict->SetIntValue("NUM_JOBS_RUNNING",
      static_cast<int64_t>(coordinator_->NumJobsInState(JobDescriptor::RUNNING)));
  pgheader_sub_dict->SetIntValue("NUM_TASKS_RUNNING",
      static_cast<int64_t>(
          coordinator_->NumTasksInState(TaskDescriptor::RUNNING) +
          coordinator_->NumTasksInState(TaskDescriptor::DELEGATED)));
  pgheader_sub_dict->SetIntValue("NUM_RESOURCES",
      static_cast<int64_t>(coordinator_->NumResources()));
  // Error message, if set
  if (err) {
    TemplateDictionary* err_dict =
        pgheader_sub_dict->AddSectionDictionary("ERR");
    err_dict->SetValue("ERR_TITLE", err->first);
    err_dict->SetValue("ERR_TEXT", err->second);
  }
}

void CoordinatorHTTPUI::AddFooterToTemplate(TemplateDictionary* dict) {
  // Page footer
  TemplateDictionary* pgheader_sub_dict =
      dict->AddIncludeDictionary("PAGE_FOOTER");
  pgheader_sub_dict->SetFilename(
      FLAGS_http_ui_template_dir + "/page_footer.tpl");
}

void CoordinatorHTTPUI::HandleCollectlGraphsURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Kick off collectl graph generation
  vector<string> args;
  args.push_back("-scdnm");
  args.push_back("-p");
  args.push_back("/var/log/collectl/20150320*.raw.gz");
  args.push_back("--from");
  args.push_back(http_request->get_query("from"));
  args.push_back("-ozTm");
  args.push_back("-P");
  args.push_back("-f");
  args.push_back("/tmp/collectltmp/");
  int infd[2];
  int outfd[2];
  int errfd[2];
  ExecCommandSync("/usr/bin/collectl", args, infd, outfd, errfd);
  //args.clear();
  //args.push_back("ext/plot_collectl.py");
  //args.push_back("/tmp/collectltmp");
  //args.push_back("/tmp/testgraph.pdf");
  //ExecCommandSync("/usr/bin/python", args, infd, outfd);
  //ServeFile("/tmp/collectltmp/", tcp_conn, http_request, writer);
  writer->write("done!");
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleCollectlRawURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
}

void CoordinatorHTTPUI::HandleJobSubmitURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  // Check if we have a JobDescriptor as part of the POST parameters
  string job_descriptor_param = http_request->get_query("jd");
  if (http_request->get_method() != "POST" || job_descriptor_param.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  // We're okay to continue
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  JobDescriptor job_descriptor;
  google::protobuf::util::JsonStringToMessage(job_descriptor_param,
                                              &job_descriptor);
  // Submit the JD to the coordinator
  VLOG(3) << "JD:" << job_descriptor.DebugString();
  string job_id = coordinator_->SubmitJob(job_descriptor);
  // Return the job ID to the client
  writer->write(job_id);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleRootURI(const http::request_ptr& http_request,
                                      const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Individual to this request
  TemplateDictionary dict("main");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  dict.SetValue("COORD_ID", to_string(coordinator_->uuid()));
  dict.SetValue("COORD_HOST", coordinator_->hostname());
  if (!coordinator_->parent_uri().empty()) {
    TemplateDictionary* parent_dict =
        dict.AddSectionDictionary("COORD_HAS_PARENT");
    parent_dict->SetValue("COORD_PARENT_URI", coordinator_->parent_uri());
    parent_dict->SetValue("COORD_PARENT_HOST",
                          URITools::GetHostnameFromURI(
                              coordinator_->parent_uri()));
  }
  dict.SetIntValue("NUM_JOBS_KNOWN",
                   static_cast<int64_t>(coordinator_->NumJobs()));
  dict.SetIntValue("NUM_JOBS_RUNNING",
                   static_cast<int64_t>(coordinator_->NumJobsInState(
                       JobDescriptor::RUNNING)));
  dict.SetIntValue("NUM_TASKS_KNOWN",
                   static_cast<int64_t>(coordinator_->NumTasks()));
  dict.SetIntValue("NUM_TASKS_RUNNING",
      static_cast<int64_t>(
          coordinator_->NumTasksInState(TaskDescriptor::RUNNING) +
          coordinator_->NumTasksInState(TaskDescriptor::DELEGATED)));
  // The +1 is because the coordinator itself is a resource, too.
  dict.SetIntValue("NUM_RESOURCES_KNOWN",
                   static_cast<int64_t>(coordinator_->NumResources()) + 1);
  dict.SetIntValue("NUM_RESOURCES_LOCAL",
                   static_cast<int64_t>(coordinator_->NumResources()));
  dict.SetIntValue("NUM_REFERENCES_KNOWN",
                   static_cast<int64_t>(
                       coordinator_->get_object_store()->NumTotalReferences()));
  dict.SetIntValue("NUM_REFERENCES_CONCRETE",
                   static_cast<int64_t>(
                       coordinator_->get_object_store()->NumReferencesOfType(
                           ReferenceDescriptor::CONCRETE)));
  // Scheduler information
  if (FLAGS_scheduler == "simple") {
    dict.SetValue("SCHEDULER_NAME", "queue-based");
  } else if (FLAGS_scheduler == "flow") {
    dict.SetValue("SCHEDULER_NAME", "flow network optimization");
    TemplateDictionary* flow_scheduler_detail_dict =
        dict.AddSectionDictionary("FLOW_SCHEDULER_DETAILS");
    const FlowScheduler* sched =
      dynamic_cast<const FlowScheduler*>(coordinator_->scheduler());
    flow_scheduler_detail_dict->SetIntValue(
        "FLOW_SCHEDULER_COST_MODEL",
        FLAGS_flow_scheduling_cost_model);
    if (FLAGS_debug_flow_graph) {
      for (uint64_t i = 0; i < sched->dispatcher().seq_num(); ++i) {
        TemplateDictionary* iteration_dict =
            flow_scheduler_detail_dict->AddSectionDictionary("SCHEDULER_ITER");
        iteration_dict->SetIntValue("SCHEDULER_ITER_ID",
                                    static_cast<int64_t>(i));
      }
    }
  }
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/main.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleFaviconURI(const http::request_ptr& http_request,
                                         const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request, tcp_conn);
}

void CoordinatorHTTPUI::HandleJobsListURI(const http::request_ptr& http_request,
                                          const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get job list from coordinator
  vector<JobDescriptor> jobs = coordinator_->active_jobs();
  int64_t i = 0;
  TemplateDictionary dict("jobs_list");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  for (vector<JobDescriptor>::const_iterator jd_iter =
       jobs.begin();
       jd_iter != jobs.end();
       ++jd_iter) {
    TemplateDictionary* sect_dict = dict.AddSectionDictionary("JOB_DATA");
    sect_dict->SetIntValue("JOB_NUM", i);
    sect_dict->SetValue("JOB_ID", to_string(jd_iter->uuid()));
    sect_dict->SetValue("JOB_FRIENDLY_NAME", jd_iter->name());
    sect_dict->SetFormattedValue("JOB_ROOT_TASK_ID", "%ju",
                                 TaskID_t(jd_iter->root_task().uid()));
    sect_dict->SetValue("JOB_STATE",
                        ENUM_TO_STRING(JobDescriptor::JobState,
                                       jd_iter->state()));
    ++i;
  }
  string output;
  if (!http_request->get_query("json").empty()) {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/json_jobs_list.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  } else {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/jobs_list.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleJobCompletionURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string job_id = http_request->get_query("id");
  if (job_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  JobDescriptor* jd_ptr = coordinator_->GetJob(
      JobIDFromString(job_id));
  TemplateDictionary dict("job_completion");
  if (jd_ptr) {
    dict.SetValue("JOB_ID", jd_ptr->uuid());
    dict.SetValue("JOB_NAME", jd_ptr->name());
    dict.SetValue("JOB_STATUS", ENUM_TO_STRING(JobDescriptor::JobState,
                                               jd_ptr->state()));
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  } else {
    ErrorMessage_t err("Job not found.",
                       "The requested job does not exist or is unknown to "
                       "this coordinator.");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
  }
  AddFooterToTemplate(&dict);
  string output;
  if (!http_request->get_query("json").empty()) {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/json_job_completion.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  } else {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/job_completion.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  }
  writer->write(output);
  FinishOkResponse(writer);
}


void CoordinatorHTTPUI::HandleJobURI(const http::request_ptr& http_request,
                                     const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string job_id = http_request->get_query("id");
  if (job_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  JobDescriptor* jd_ptr = coordinator_->GetJob(
      JobIDFromString(job_id));
  TemplateDictionary dict("job_status");
  if (jd_ptr) {
    if (http_request->get_query("a") == "kill") {
      if (coordinator_->KillRunningJob(JobIDFromString(jd_ptr->uuid()))) {
        RedirectResponse(http_request, tcp_conn, "/job/status/?id=" + job_id);
        return;
      } else {
        ErrorMessage_t err("Failed to kill job.",
                           "The requested job could not be killed; check "
                           "the ERROR log for more information.");
        AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
      }
    }
    dict.SetValue("JOB_ID", jd_ptr->uuid());
    dict.SetValue("JOB_NAME", jd_ptr->name());
    dict.SetValue("JOB_STATUS", ENUM_TO_STRING(JobDescriptor::JobState,
                                               jd_ptr->state()));

    dict.SetFormattedValue("JOB_ROOT_TASK_ID", "%ju",
                           TaskID_t(jd_ptr->root_task().uid()));
    if (jd_ptr->output_ids_size() > 0)
      dict.SetIntValue("JOB_NUM_OUTPUTS", jd_ptr->output_ids_size());
    else
      dict.SetIntValue("JOB_NUM_OUTPUTS", 1);
    for (RepeatedPtrField<string>::const_iterator out_iter =
         jd_ptr->output_ids().begin();
         out_iter != jd_ptr->output_ids().end();
         ++out_iter) {
      TemplateDictionary* out_dict = dict.AddSectionDictionary("JOB_OUTPUTS");
      out_dict->SetValue(
          "JOB_OUTPUT_ID",
          DataObjectIDFromProtobuf(*out_iter).name_printable_string());
    }
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  } else {
    ErrorMessage_t err("Job not found.",
                       "The requested job does not exist or is unknown to "
                       "this coordinator.");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
  }
  AddFooterToTemplate(&dict);
  string output;
  if (!http_request->get_query("json").empty()) {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/json_job_status.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  } else {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/job_status.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleECDetailsURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  string ec_id = http_request->get_query("id");
  if (ec_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  TemplateDictionary dict("ec_details");
  dict.SetFormattedValue("EC_ID", "%llu", strtoull(ec_id.c_str(), 0, 10));
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/ec_details.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleReferencesListURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get reference list from coordinator's object store
  // XXX(malte): This is UNSAFE with regards to concurrent modification!
  shared_ptr<DataObjectMap_t> refs =
      coordinator_->get_object_store()->object_table();
  uint64_t i = 0;
  TemplateDictionary dict("refs_list");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  for (DataObjectMap_t::const_iterator r_iter =
       refs->begin();
       r_iter != refs->end();
       ++r_iter) {
    TemplateDictionary* sect_dict = dict.AddSectionDictionary("OBJ_DATA");
    sect_dict->SetValue("OBJ_ID", r_iter->first.name_printable_string());
    for (unordered_set<ReferenceInterface*>::const_iterator ref_iter =
         r_iter->second.begin();
         ref_iter != r_iter->second.end();
         ++ref_iter) {
      TemplateDictionary* subsect_dict =
          sect_dict->AddSectionDictionary("REF_DATA");
      subsect_dict->SetFormattedValue(
          "REF_PRODUCING_TASK_ID", "%ju",
          TaskID_t((*ref_iter)->desc().producing_task()));
      subsect_dict->SetValue("REF_TYPE",
                          ENUM_TO_STRING(ReferenceDescriptor::ReferenceType,
                                         (*ref_iter)->desc().type()));
      subsect_dict->SetValue("REF_LOCATION", (*ref_iter)->desc().location());
      ++i;
    }
  }
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/refs_list.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourcesListURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  const vector<ResourceStatus*> resources =
      coordinator_->associated_resources();
  int64_t i = 0;
  TemplateDictionary dict("resources_list");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  for (vector<ResourceStatus*>::const_iterator rd_iter =
       resources.begin();
       rd_iter != resources.end();
       ++rd_iter) {
    TemplateDictionary* sect_dict = dict.AddSectionDictionary("RES_DATA");
    sect_dict->SetIntValue("RES_NUM", i);
    sect_dict->SetValue("RES_ID", to_string((*rd_iter)->descriptor().uuid()));
    sect_dict->SetValue("RES_FRIENDLY_NAME",
                        (*rd_iter)->descriptor().friendly_name());
    sect_dict->SetValue("RES_STATE",
                        ENUM_TO_STRING(ResourceDescriptor::ResourceState,
                                       (*rd_iter)->descriptor().state()));
    // N.B.: We make the assumption that only PU type resources are schedulable
    // here!
    if (!(*rd_iter)->descriptor().schedulable())
      sect_dict->AddSectionDictionary("RES_NON_SCHEDULABLE");
    ++i;
  }
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/resources_list.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourceURI(const http::request_ptr& http_request,
                                          const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string res_id = http_request->get_query("id");
  if (res_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  ResourceID_t rid = ResourceIDFromString(res_id);
  ResourceTopologyNodeDescriptor* rtnd_ptr =
    coordinator_->GetResourceTreeNode(rid);
  ResourceStatus* rs_ptr = coordinator_->GetResourceStatus(rid);
  TemplateDictionary dict("resource_status");
  if (rtnd_ptr) {
    dict.SetValue("RES_ID", rtnd_ptr->resource_desc().uuid());
    dict.SetValue("RES_FRIENDLY_NAME",
                  rtnd_ptr->resource_desc().friendly_name());
    coordinator_->scheduler()->PopulateSchedulerResourceUI(rid, &dict);
    dict.SetValue("RES_TYPE", ENUM_TO_STRING(ResourceDescriptor::ResourceType,
                                             rtnd_ptr->resource_desc().type()));
    dict.SetValue("RES_STATUS",
                  ENUM_TO_STRING(ResourceDescriptor::ResourceState,
                                 rtnd_ptr->resource_desc().state()));
    dict.SetValue("RES_PARENT_ID", rtnd_ptr->parent_id());
    dict.SetIntValue("RES_NUM_CHILDREN", rtnd_ptr->children_size());
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator
           c_iter = rtnd_ptr->children().begin();
         c_iter != rtnd_ptr->children().end();
         ++c_iter) {
      TemplateDictionary* child_dict =
          dict.AddSectionDictionary("RES_CHILDREN");
      child_dict->SetValue("RES_CHILD_ID", c_iter->resource_desc().uuid());
    }
    dict.SetValue("RES_LOCATION", rs_ptr->location());
    dict.SetValue("RES_LOCATION_HOST",
                  URITools::GetHostnameFromURI(rs_ptr->location()));
    // JS expects millisecond values
    dict.SetIntValue("RES_LAST_HEARTBEAT", rs_ptr->last_heartbeat() / 1000);
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  } else {
    ErrorMessage_t err("Resource not found.",
                       "The requested resource does not exist.");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
  }
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/resource_status.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourcesTopologyURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  // Get resource topology from coordinator
  const ResourceTopologyNodeDescriptor& root_rtnd =
      coordinator_->local_resource_topology();
  // Return serialized resource topology
  http::response_writer_ptr writer = InitOkResponse(http_request,
                                                tcp_conn);
  string json;
  CHECK(MessageToJsonString(root_rtnd, &json).ok());
  writer->write(json);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleInjectURI(const http::request_ptr& http_request,
                                        const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Individual to this request
  if (http_request->get_method() != "POST") {
    // return an error
    writer->write("POST a message to this URL to inject it.");
  } else {
    writer->write("ok");
  }
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleJobStatusURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  string job_id = http_request->get_query("id");
  if (job_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  TemplateDictionary dict("job_dtg");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  string output;
  if (!job_id.empty()) {
    dict.SetValue("JOB_ID", job_id);
    ExpandTemplate(FLAGS_http_ui_template_dir + "/job_dtg.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  } else {
    output = "Please specify a job ID parameter.";
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleJobDTGURI(const http::request_ptr& http_request,
                                        const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);

  string job_id = http_request->get_query("id");
  if (!job_id.empty()) {
    // Get DTG from coordinator
    const JobDescriptor* jd = coordinator_->DescriptorForJob(job_id);
    if (!jd) {
      // Job not found here
      VLOG(1) << "Requested DTG for non-existent job " << job_id;
      ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request,
                    tcp_conn);
      return;
    }
    // Return serialized DTG
    http::response_writer_ptr writer = InitOkResponse(http_request,
                                                      tcp_conn);
    string json;
    CHECK(MessageToJsonString(*jd, &json).ok());
    writer->write(json);
    FinishOkResponse(writer);
  } else {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
}

void CoordinatorHTTPUI::HandleLogURI(const http::request_ptr& http_request,
                                     const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string log = http_request->get_query("log");
  if (log.empty() && !(log == "INFO" || log == "WARNING" || log == "ERROR")) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  if (FLAGS_log_dir.empty()) {
    writer->write("Need to specify --log_dir on cooordinator command line to "
                  "enable remote log viewing.");
    FinishOkResponse(writer);
  } else {
    string log_filename = FLAGS_log_dir + "/coordinator." + log;
    ServeFile(log_filename, tcp_conn, http_request, writer);
    FinishOkResponse(writer);
  }
}

void CoordinatorHTTPUI::HandleReferenceURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string ref_id = http_request->get_query("id");
  if (ref_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  unordered_set<ReferenceInterface*>* refs =
      coordinator_->get_object_store()->GetReferences(
          DataObjectIDFromString(ref_id));
  TemplateDictionary dict("reference_view");
  if (refs && refs->size() > 0) {
    dict.SetValue("OBJ_ID", ref_id);
    for (unordered_set<ReferenceInterface*>::const_iterator
           ref_iter = refs->begin();
         ref_iter != refs->end();
         ++ref_iter) {
      TemplateDictionary* sect_dict = dict.AddSectionDictionary("REF_DATA");
      sect_dict->SetValue("REF_TYPE",
                          ENUM_TO_STRING(ReferenceDescriptor::ReferenceType,
                                         (*ref_iter)->desc().type()));
      sect_dict->SetValue("REF_SCOPE",
                         ENUM_TO_STRING(ReferenceDescriptor::ReferenceScope,
                                        (*ref_iter)->desc().scope()));
      sect_dict->SetIntValue("REF_NONDET",
                            (*ref_iter)->desc().non_deterministic());
      sect_dict->SetIntValue("REF_SIZE",
                             static_cast<int64_t>((*ref_iter)->desc().size()));
      sect_dict->SetFormattedValue("REF_PRODUCER", "%ju",
                                  (*ref_iter)->desc().producing_task());
    }
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  } else {
    ErrorMessage_t err("Reference or data object not found.",
                       "There exists no local reference for the requested "
                       "data object ID.");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
  }
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/reference_view.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleSchedURI(const http::request_ptr& http_request,
                                       const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string iter_id = http_request->get_query("iter");
  if (iter_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  // XXX(malte): HACK to plot flow graph.
  string cmd;
  spf(&cmd, "bash scripts/plot_flow_graph.sh %s", iter_id.c_str());
  int64_t ret = system(cmd.c_str());
  if (!WIFEXITED(ret)) {
    LOG(WARNING) << "Failed command: " << cmd;
  }
  // End plotting hack.
  string action = http_request->get_query("a");
  string graph_filename = FLAGS_debug_output_dir;
  if (action.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  } else {
    if (action == "png") {
      graph_filename += "/debug_" + iter_id + ".dm.png";
    } else if (action == "gv") {
      graph_filename += "/debug_" + iter_id + ".dm.gv";
    } else if (action == "dimacs") {
      graph_filename += "/debug_" + iter_id + ".dm";
    } else if (action == "view") {
      TemplateDictionary dict("flow_graph_view");
      dict.SetValue("ITER_ID", iter_id);
      AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
      AddFooterToTemplate(&dict);
      string output;
      ExpandTemplate(FLAGS_http_ui_template_dir + "/flow_graph.tpl",
                     ctemplate::DO_NOT_STRIP, &dict, &output);
      writer->write(output);
      FinishOkResponse(writer);
      return;
    } else {
      ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                    tcp_conn);
      return;
    }
  }
  ServeFile(graph_filename, tcp_conn, http_request, writer);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleSchedCostModelURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  const FlowScheduler* sched =
    dynamic_cast<const FlowScheduler*>(coordinator_->scheduler());
  string debug_info = sched->cost_model().DebugInfo();
  TemplateDictionary dict("sched_cost_model");
  dict.SetValue("COST_MODEL_DEBUG_INFO", debug_info);
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate(FLAGS_http_ui_template_dir + "/sched_costmodel.tpl",
                 ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleSchedFlowGraphURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  if (!http_request->get_query("json").empty()) {
    if (FLAGS_scheduler != "flow") {
      ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request,
                    tcp_conn);
      return;
    }
    const FlowScheduler* sched =
      dynamic_cast<const FlowScheduler*>(coordinator_->scheduler());
    string json_flow_graph;
    sched->dispatcher().ExportJSON(&json_flow_graph);
    writer->write(json_flow_graph);
    FinishOkResponse(writer);
  } else {
    TemplateDictionary dict("flow_graph_view");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
    AddFooterToTemplate(&dict);
    string output;
    ExpandTemplate(FLAGS_http_ui_template_dir + "/flow_graph.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
    writer->write(output);
  }
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleStatisticsURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string res_id_str = http_request->get_query("res");
  string task_id_str = http_request->get_query("task");
  string ec_id_str = http_request->get_query("ec");
  if ((res_id_str.empty() && task_id_str.empty() && ec_id_str.empty()) ||
      !(res_id_str.empty() || task_id_str.empty()) ||
      !(res_id_str.empty() || ec_id_str.empty()) ||
      !(task_id_str.empty() || ec_id_str.empty())) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    LOG(WARNING) << "Invalid stats request!";
    return;
  }
  string output = "";
  // Check if we have any statistics for this resource
  if (!res_id_str.empty()) {
    ResourceID_t res_id = ResourceIDFromString(res_id_str);
    const deque<ResourceStats> result =
      coordinator_->scheduler()->knowledge_base()->GetStatsForMachine(res_id);
    if (coordinator_->GetResourceTreeNode(res_id)) {
      int64_t length = static_cast<int64_t>(result.size());
      output += "[";
      for (deque<ResourceStats>::const_iterator it =
             result.begin() + max(0LL, length - WEBUI_PERF_QUEUE_LEN);
          it != result.end();
          ++it) {
        if (output != "[")
          output += ", ";
        string json;
        CHECK(MessageToJsonString(*it, &json).ok());
        output += json;
      }
      output += "]";
    } else {
      ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request,
                    tcp_conn);
      LOG(WARNING) << "Stats request for non-existent resource " << res_id;
      return;
    }
  } else if (!task_id_str.empty()) {
    TaskDescriptor* td = coordinator_->GetTask(TaskIDFromString(task_id_str));
    if (!td) {
      ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request,
                    tcp_conn);
      LOG(WARNING) << "Stats request for non-existent task " << task_id_str;
      return;
    }
    output += "{ \"samples\": [";
    const deque<TaskStats>* samples_result =
      coordinator_->scheduler()->knowledge_base()->GetStatsForTask(
            TaskIDFromString(task_id_str));
    if (samples_result) {
      bool first = true;
      int64_t length = static_cast<int64_t>(samples_result->size());
      for (deque<TaskStats>::const_iterator it =
             samples_result->begin() + max(0LL, length - WEBUI_PERF_QUEUE_LEN);
          it != samples_result->end();
          ++it) {
        if (!first)
          output += ", ";
        string json;
        CHECK(MessageToJsonString(*it, &json).ok());
        output += json;
        first = false;
      }
    }
    output += "]";
    output += ", \"reports\": [";
    const deque<TaskFinalReport>* report_result =
      coordinator_->scheduler()->knowledge_base()->GetFinalReportForTask(
          td->uid());
    if (report_result) {
      bool first = true;
      for (deque<TaskFinalReport>::const_iterator it =
          report_result->begin();
          it != report_result->end();
          ++it) {
        if (!first)
          output += ", ";
        string json;
        CHECK(MessageToJsonString(*it, &json).ok());
        output += json;
        first = false;
      }
    }
    output += "] }";
  } else if (!ec_id_str.empty()) {
    output += "{ \"reports\": [";
    const deque<TaskFinalReport>* report_result =
      coordinator_->scheduler()->knowledge_base()->GetFinalReportsForTEC(
          strtoull(ec_id_str.c_str(), 0, 10));
    if (report_result) {
      bool first = true;
      for (deque<TaskFinalReport>::const_iterator it =
          report_result->begin();
          it != report_result->end();
          ++it) {
        if (!first)
          output += ", ";
        string json;
        CHECK(MessageToJsonString(*it, &json).ok());
        output += json;
        first = false;
      }
    }
    output += "] }";
  } else {
    LOG(FATAL) << "Neither task_id, nor ec_id, nor res_id set, "
               << "but they should be!";
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleTasksListURI(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get task list from coordinator
  vector<TaskDescriptor*> tasks = coordinator_->active_tasks();
  TemplateDictionary dict("tasks_list");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  AddFooterToTemplate(&dict);
  for (vector<TaskDescriptor*>::const_iterator td_iter =
       tasks.begin();
       td_iter != tasks.end();
       ++td_iter) {
    TemplateDictionary* sect_dict = dict.AddSectionDictionary("TASK_DATA");
    sect_dict->SetFormattedValue("TASK_ID", "%ju", (*td_iter)->uid());
    sect_dict->SetValue("TASK_JOB_ID", (*td_iter)->job_id());
    sect_dict->SetValue("TASK_FRIENDLY_NAME", (*td_iter)->name());
    sect_dict->SetValue("TASK_STATE",
                        ENUM_TO_STRING(TaskDescriptor::TaskState,
                                       (*td_iter)->state()));
  }
  string output;
  if (!http_request->get_query("json").empty()) {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/json_tasks_list.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  } else {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/tasks_list.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleTaskURI(const http::request_ptr& http_request,
                                      const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string task_id = http_request->get_query("id");
  if (task_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  TemplateDictionary dict("task_status");
  string action = http_request->get_query("a");
  if (!action.empty()) {
    if (action == "kill") {
      if (coordinator_->KillRunningTask(TaskIDFromString(task_id),
                                        TaskKillMessage::USER_ABORT)) {
        RedirectResponse(http_request, tcp_conn, "/task/?id=" + task_id);
        return;
      } else {
        ErrorMessage_t err("Failed to kill task.",
                           "The requested task could not be killed; check "
                           "the ERROR log for more information.");
        AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
      }
    }
  }
  TaskDescriptor* td_ptr = coordinator_->GetTask(
      TaskIDFromString(task_id));
  if (td_ptr) {
    dict.SetFormattedValue("TASK_ID", "%ju", TaskID_t(td_ptr->uid()));
    if (!td_ptr->name().empty())
      dict.SetValue("TASK_NAME", td_ptr->name());
    dict.SetValue("TASK_BINARY", td_ptr->binary());
    dict.SetValue("TASK_JOB_ID", td_ptr->job_id());
    JobDescriptor* jd_ptr = coordinator_->GetJob(
        JobIDFromString(td_ptr->job_id()));
    if (jd_ptr)
      dict.SetValue("TASK_JOB_NAME", jd_ptr->name());
    string arg_string = "";
    for (RepeatedPtrField<string>::const_iterator arg_iter =
         td_ptr->args().begin();
         arg_iter != td_ptr->args().end();
         ++arg_iter) {
      arg_string += " " + *arg_iter;
    }
    dict.SetValue("TASK_ARGS", arg_string);
    dict.SetValue("TASK_STATUS", ENUM_TO_STRING(TaskDescriptor::TaskState,
                                                td_ptr->state()));
    // Scheduled to resource
    if (!td_ptr->scheduled_to_resource().empty()) {
      dict.SetValue("TASK_SCHEDULED_TO", td_ptr->scheduled_to_resource());
    }
    // Location
    if (!td_ptr->scheduled_to_resource().empty()) {
      ResourceDescriptor* rd_ptr =
        coordinator_->GetMachineRDForResource(ResourceIDFromString(
            td_ptr->scheduled_to_resource()));
      dict.SetValue("TASK_LOCATION", rd_ptr->friendly_name());
      dict.SetValue("TASK_LOCATION_HOST", rd_ptr->friendly_name());
    } else {
      dict.SetValue("TASK_LOCATION", "unknown");
      dict.SetValue("TASK_LOCATION_HOST", "localhost");
    }
    if (!td_ptr->delegated_to().empty()) {
      dict.SetValue("TASK_LOCATION", td_ptr->delegated_to());
      dict.SetValue("TASK_LOCATION_HOST",
                    URITools::GetHostnameFromURI(td_ptr->delegated_to()));
    }
    if (!td_ptr->delegated_from().empty()) {
      TemplateDictionary* del_dict =
          dict.AddSectionDictionary("TASK_DELEGATION");
      del_dict->SetValue("TASK_DELEGATED_FROM_HOST",
                         URITools::GetHostnameFromURI(
                             td_ptr->delegated_from()));
    }
    // Timestamps
    dict.SetIntValue("TASK_SUBMIT_TIME", td_ptr->submit_time() / 1000);
    dict.SetIntValue("TASK_START_TIME", td_ptr->start_time() / 1000);
    dict.SetValue("TASK_START_TIME_HR",
        CoarseTimestampToHumanReadble(td_ptr->start_time() / 1000000));
    dict.SetIntValue("TASK_FINISH_TIME", td_ptr->finish_time() / 1000);
    dict.SetValue("TASK_FINISH_TIME_HR",
        CoarseTimestampToHumanReadble(td_ptr->finish_time() / 1000000));
    // Heartbeat time
    // JS expects millisecond values
    dict.SetIntValue("TASK_LAST_HEARTBEAT",
                     td_ptr->last_heartbeat_time() / 1000);
    coordinator_->scheduler()->PopulateSchedulerTaskUI(td_ptr->uid(), &dict);
    // Dependencies
    if (td_ptr->dependencies_size() > 0)
      dict.SetIntValue("TASK_NUM_DEPS", td_ptr->dependencies_size());
    else
      dict.SetIntValue("TASK_NUM_DEPS", 1);
    for (RepeatedPtrField<ReferenceDescriptor>::const_iterator dep_iter =
         td_ptr->dependencies().begin();
         dep_iter != td_ptr->dependencies().end();
         ++dep_iter) {
      TemplateDictionary* dep_dict = dict.AddSectionDictionary("TASK_DEPS");
      dep_dict->SetValue("TASK_DEP_ID", DataObjectIDFromProtobuf(
          dep_iter->id()).name_printable_string());
    }
    // Spawned
    if (td_ptr->spawned_size() > 0)
      dict.SetIntValue("TASK_NUM_SPAWNED", td_ptr->spawned_size());
    else
      dict.SetIntValue("TASK_NUM_SPAWNED", 1);
    for (RepeatedPtrField<TaskDescriptor>::const_iterator spawn_iter =
         td_ptr->spawned().begin();
         spawn_iter != td_ptr->spawned().end();
         ++spawn_iter) {
      TemplateDictionary* spawn_dict =
          dict.AddSectionDictionary("TASK_SPAWNED");
      spawn_dict->SetFormattedValue("TASK_SPAWNED_ID", "%ju",
                                  TaskID_t(spawn_iter->uid()));
    }
    // Outputs
    if (td_ptr->outputs_size() > 0)
      dict.SetIntValue("TASK_NUM_OUTPUTS", td_ptr->outputs_size());
    else
      dict.SetIntValue("TASK_NUM_OUTPUTS", 1);
    for (RepeatedPtrField<ReferenceDescriptor>::const_iterator out_iter =
         td_ptr->outputs().begin();
         out_iter != td_ptr->outputs().end();
         ++out_iter) {
      TemplateDictionary* out_dict = dict.AddSectionDictionary("TASK_OUTPUTS");
      out_dict->SetValue("TASK_OUTPUT_ID", DataObjectIDFromProtobuf(
          out_iter->id()).name_printable_string());
    }
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  } else {
    ErrorMessage_t err("Task not found.",
                       "The requested task does not exist or is unknown to "
                       "this coordinator.");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
  }
  AddFooterToTemplate(&dict);
  string output;
  if (!http_request->get_query("json").empty()) {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/json_task_status.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  } else {
    ExpandTemplate(FLAGS_http_ui_template_dir + "/task_status.tpl",
                   ctemplate::DO_NOT_STRIP, &dict, &output);
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleTaskLogURI(const http::request_ptr& http_request,
                                         const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string task_id_str = http_request->get_query("id");
  if (task_id_str.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  TaskID_t task_id = TaskIDFromString(task_id_str);
  TaskDescriptor* td = coordinator_->GetTask(task_id);
  if (!td->delegated_to().empty()) {
    string target = "http://" +
                    URITools::GetHostnameFromURI(td->delegated_to()) +
                    ":" + to_string(port_) + "/tasklog/?id=" +
                    to_string(task_id) + "&a=" + http_request->get_query("a");
    RedirectResponse(http_request, tcp_conn, target);
    return;
  }
  string action = http_request->get_query("a");
  string tasklog_filename = FLAGS_task_log_dir + "/" + td->job_id() + "-"
                            + to_string(task_id);
  if (action.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  } else {
    if (action == "1") {
      tasklog_filename += "-stdout";
    } else if (action == "2") {
      tasklog_filename += "-stderr";
    } else {
      ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                    tcp_conn);
      return;
    }
  }
  ServeFile(tasklog_filename, tcp_conn, http_request, writer);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleShutdownURI(const http::request_ptr& http_request,
                                          const tcp::connection_ptr& tcp_conn) {
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  string reason = "HTTP request from " + tcp_conn->get_remote_ip().to_string();
  // Make the HTTP server inactive, so that the coordinator does not try to shut
  // it down.
  active_ = false;
  // Now initiate coordinator shutdown
  coordinator_->Shutdown(reason);
  writer->write("Shutdown for coordinator initiated.");
  FinishOkResponse(writer);
  // Now shut down the HTTP server itself
  Shutdown(true);
}

http::response_writer_ptr CoordinatorHTTPUI::InitOkResponse(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  http::response_writer_ptr writer = http::response_writer::create(
      tcp_conn, *http_request, boost::bind(&tcp::connection::finish,
                                           tcp_conn));
  http::response& r = writer->get_response();
  r.set_status_code(http::types::RESPONSE_CODE_OK);
  r.set_status_message(http::types::RESPONSE_MESSAGE_OK);
  // Hack to allow file:// access
  r.add_header("Access-Control-Allow-Origin", "*");
  return writer;
}

void CoordinatorHTTPUI::ErrorResponse(
    const unsigned int error_code,
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn) {
  http::response_writer_ptr writer = http::response_writer::create(
      tcp_conn, *http_request, boost::bind(&tcp::connection::finish,
                                           tcp_conn));
  http::response& r = writer->get_response();
  r.set_status_code(error_code);
  //r.set_status_message("test");
  writer->send();
}

void CoordinatorHTTPUI::RedirectResponse(
    const http::request_ptr& http_request,
    const tcp::connection_ptr& tcp_conn,
    const string& location) {
  http::response_writer_ptr writer = http::response_writer::create(
      tcp_conn, *http_request, boost::bind(&tcp::connection::finish,
                                           tcp_conn));
  http::response& r = writer->get_response();
  r.set_status_code(http::types::RESPONSE_CODE_FOUND);
  r.add_header("Location", location);
  writer->send();
}

void CoordinatorHTTPUI::FinishOkResponse(
    const http::response_writer_ptr& writer) {
  writer->send();
}

void CoordinatorHTTPUI::LogRequest(const http::request_ptr& http_request) {
  LOG(INFO) << "[HTTPREQ] Serving " << http_request->get_resource();
}

void __attribute__((no_sanitize_address)) CoordinatorHTTPUI::Init(
    uint16_t port) {
  try {
    // Fail if we are not assured that no existing server object is stored.
    if (coordinator_http_server_) {
      LOG(FATAL) << "Trying to initialized an HTTP server that has already "
                 << "been initialized!";
    }
    // Otherwise, make such an object and store it.
    coordinator_http_server_.reset(new http::server(port));
    // Bind handlers for different kinds of entry points
    // Root URI
    coordinator_http_server_->add_resource("/", boost::bind(
        &CoordinatorHTTPUI::HandleRootURI, this, _1, _2));
    // Root URI
    coordinator_http_server_->add_resource("/favicon.ico", boost::bind(
        &CoordinatorHTTPUI::HandleFaviconURI, this, _1, _2));
    // Collectl graph hook
    coordinator_http_server_->add_resource("/collectl/graphs/", boost::bind(
        &CoordinatorHTTPUI::HandleCollectlGraphsURI, this, _1, _2));
    // Collectl raw data hook
    coordinator_http_server_->add_resource("/collectl/raw/", boost::bind(
        &CoordinatorHTTPUI::HandleCollectlRawURI, this, _1, _2));
    // Equivalence class details
    coordinator_http_server_->add_resource("/ec/", boost::bind(
        &CoordinatorHTTPUI::HandleECDetailsURI, this, _1, _2));
    // Job list
    coordinator_http_server_->add_resource("/jobs/", boost::bind(
        &CoordinatorHTTPUI::HandleJobsListURI, this, _1, _2));
    // Job submission
    coordinator_http_server_->add_resource("/job/submit/", boost::bind(
        &CoordinatorHTTPUI::HandleJobSubmitURI, this, _1, _2));
    // Job completion hook
    coordinator_http_server_->add_resource("/job/completion/", boost::bind(
        &CoordinatorHTTPUI::HandleJobCompletionURI, this, _1, _2));
    // Job status
    coordinator_http_server_->add_resource("/job/status/", boost::bind(
        &CoordinatorHTTPUI::HandleJobURI, this, _1, _2));
    // Job task graph visualization
    coordinator_http_server_->add_resource("/job/dtg-view/", boost::bind(
        &CoordinatorHTTPUI::HandleJobStatusURI, this, _1, _2));
    // Job task graph
    coordinator_http_server_->add_resource("/job/dtg/", boost::bind(
        &CoordinatorHTTPUI::HandleJobDTGURI, this, _1, _2));
    // Log for coordinator
    coordinator_http_server_->add_resource("/log/", boost::bind(
        &CoordinatorHTTPUI::HandleLogURI, this, _1, _2));
    // Resource list
    coordinator_http_server_->add_resource("/resources/", boost::bind(
        &CoordinatorHTTPUI::HandleResourcesListURI, this, _1, _2));
    // Resource topology
    coordinator_http_server_->add_resource("/resources/topology/", boost::bind(
        &CoordinatorHTTPUI::HandleResourcesTopologyURI, this, _1, _2));
    // Resource page
    coordinator_http_server_->add_resource("/resource/", boost::bind(
        &CoordinatorHTTPUI::HandleResourceURI, this, _1, _2));
    // Message injection
    coordinator_http_server_->add_resource("/inject/", boost::bind(
        &CoordinatorHTTPUI::HandleInjectURI, this, _1, _2));
    // Reference status
    coordinator_http_server_->add_resource("/ref/", boost::bind(
        &CoordinatorHTTPUI::HandleReferenceURI, this, _1, _2));
    // Reference list
    coordinator_http_server_->add_resource("/refs/", boost::bind(
        &CoordinatorHTTPUI::HandleReferencesListURI, this, _1, _2));
    // Scheduler data
    coordinator_http_server_->add_resource("/sched/", boost::bind(
        &CoordinatorHTTPUI::HandleSchedURI, this, _1, _2));
    // Scheduler live flow graph JSON
    coordinator_http_server_->add_resource("/sched/flowgraph/", boost::bind(
        &CoordinatorHTTPUI::HandleSchedFlowGraphURI, this, _1, _2));
    // Scheduler cost model JSON
    coordinator_http_server_->add_resource("/sched/costmodel/", boost::bind(
        &CoordinatorHTTPUI::HandleSchedCostModelURI, this, _1, _2));
    // Statistics data serving pages
    coordinator_http_server_->add_resource("/stats/", boost::bind(
        &CoordinatorHTTPUI::HandleStatisticsURI, this, _1, _2));
    // Task list
    coordinator_http_server_->add_resource("/tasks/", boost::bind(
        &CoordinatorHTTPUI::HandleTasksListURI, this, _1, _2));
    // Task status
    coordinator_http_server_->add_resource("/task/", boost::bind(
        &CoordinatorHTTPUI::HandleTaskURI, this, _1, _2));
    // Task log
    coordinator_http_server_->add_resource("/tasklog/", boost::bind(
        &CoordinatorHTTPUI::HandleTaskLogURI, this, _1, _2));
    // Shutdown request
    coordinator_http_server_->add_resource("/shutdown/", boost::bind(
        &CoordinatorHTTPUI::HandleShutdownURI, this, _1, _2));
    // Start the HTTP server
    port_ = port;
    coordinator_http_server_->start();  // spawns a thread!
    LOG(INFO) << "Coordinator HTTP interface up!";
  } catch(const std::exception& e) {
    LOG(ERROR) << "Failed running the coordinator's HTTP UI due to "
               << e.what();
  }
}

void CoordinatorHTTPUI::ServeFile(const string& filename,
                                  const tcp::connection_ptr& tcp_conn,
                                  const http::request_ptr& http_request,
                                  http::response_writer_ptr writer) {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0) {
    PLOG(ERROR) << "Failed to open file for reading.";
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  string output(1024, '\0');
  while (true) {
    int64_t n = read(fd, &output[0], 1024);
    if (n < 0) {
      PLOG(ERROR) << "Send data: read from file failed";
      ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                    tcp_conn);
      return;
    } else if (n == 0) {
      break;
    }
    writer->write(&output[0], static_cast<size_t>(n));
  }
  close(fd);
}

void CoordinatorHTTPUI::Shutdown(bool block) {
  LOG(INFO) << "Coordinator HTTP UI server shutting down on request.";
  coordinator_http_server_->stop(block);
  VLOG(1) << "HTTP UI shut down.";
}

}  // namespace webui
}  // namespace firmament
