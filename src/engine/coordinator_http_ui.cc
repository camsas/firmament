// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "engine/coordinator_http_ui.h"

#include <deque>
#include <set>
#include <string>
#include <vector>

#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>
#include <google/protobuf/text_format.h>
#include <pb2json.h>

#include "base/job_desc.pb.h"
#include "engine/coordinator.h"
#include "engine/knowledge_base.h"
#include "misc/utils.h"
#include "misc/equivclasses.h"
#include "messages/task_kill_message.pb.h"
#include "storage/types.h"

namespace firmament {
namespace webui {

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
  // HTML header
  TemplateDictionary* header_sub_dict = dict->AddIncludeDictionary("HEADER");
  header_sub_dict->SetFilename("src/webui/header.tpl");
  // Page header
  TemplateDictionary* pgheader_sub_dict =
      dict->AddIncludeDictionary("PAGE_HEADER");
  pgheader_sub_dict->SetFilename("src/webui/page_header.tpl");
  pgheader_sub_dict->SetValue("RESOURCE_ID", to_string(uuid));
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
  pgheader_sub_dict->SetFilename("src/webui/page_footer.tpl");
}

void CoordinatorHTTPUI::HandleJobSubmitURI(http::request_ptr& http_request,  // NOLINT
                                           tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  // Check if we have a JobDescriptor as part of the POST parameters
  string job_descriptor_param = http_request->get_query("test");
  if (http_request->get_method() != "POST" || job_descriptor_param.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  // We're okay to continue
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Submit the JD to the coordinator
  JobDescriptor job_descriptor;
  google::protobuf::TextFormat::ParseFromString(job_descriptor_param,
                                                &job_descriptor);
  VLOG(3) << "JD:" << job_descriptor.DebugString();
  string job_id = coordinator_->SubmitJob(job_descriptor);
  // Return the job ID to the client
  writer->write(job_id);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleRootURI(http::request_ptr& http_request,  // NOLINT
                                      tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Individual to this request
  TemplateDictionary dict("main");
  AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  dict.SetValue("COORD_ID", to_string(coordinator_->uuid()));
  dict.SetIntValue("NUM_JOBS_KNOWN", coordinator_->NumJobs());
  dict.SetIntValue("NUM_JOBS_RUNNING", coordinator_->NumJobsInState(
      JobDescriptor::RUNNING));
  dict.SetIntValue("NUM_TASKS_KNOWN", coordinator_->NumTasks());
  dict.SetIntValue("NUM_TASKS_RUNNING", coordinator_->NumTasksInState(
      TaskDescriptor::RUNNING));
  // The +1 is because the coordinator itself is a resource, too.
  dict.SetIntValue("NUM_RESOURCES_KNOWN", coordinator_->NumResources() + 1);
  dict.SetIntValue("NUM_RESOURCES_LOCAL", coordinator_->NumResources());
  dict.SetIntValue("NUM_REFERENCES_KNOWN",
                   coordinator_->get_object_store()->NumTotalReferences());
  dict.SetIntValue("NUM_REFERENCES_CONCRETE",
                   coordinator_->get_object_store()->NumReferencesOfType(
                       ReferenceDescriptor::CONCRETE));
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate("src/webui/main.tpl", ctemplate::DO_NOT_STRIP, &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleFaviconURI(http::request_ptr& http_request,  // NOLINT
                                         tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request, tcp_conn);
}

void CoordinatorHTTPUI::HandleJobsListURI(http::request_ptr& http_request,  // NOLINT
                                          tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get job list from coordinator
  vector<JobDescriptor> jobs = coordinator_->active_jobs();
  uint64_t i = 0;
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
  ExpandTemplate("src/webui/jobs_list.tpl", ctemplate::DO_NOT_STRIP, &dict,
                 &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleJobURI(http::request_ptr& http_request,  // NOLINT
                                     tcp::connection_ptr& tcp_conn) {  // NOLINT
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
  ExpandTemplate("src/webui/job_status.tpl", ctemplate::DO_NOT_STRIP,
                 &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleReferencesListURI(http::request_ptr& http_request,  // NOLINT
                                                tcp::connection_ptr& tcp_conn) {  // NOLINT
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
    for (set<ReferenceInterface*>::const_iterator ref_iter =
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
  ExpandTemplate("src/webui/refs_list.tpl", ctemplate::DO_NOT_STRIP, &dict,
                 &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourcesListURI(http::request_ptr& http_request,  // NOLINT
                                               tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  const vector<ResourceStatus*> resources =
      coordinator_->associated_resources();
  uint64_t i = 0;
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
  ExpandTemplate("src/webui/resources_list.tpl", ctemplate::DO_NOT_STRIP, &dict,
                 &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourceURI(http::request_ptr& http_request,  // NOLINT
                                          tcp::connection_ptr& tcp_conn) {  // NOLINT
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
  ResourceDescriptor* rd_ptr = coordinator_->GetResource(rid);
  ResourceStatus* rs_ptr = coordinator_->GetResourceStatus(rid);
  TemplateDictionary dict("resource_status");
  if (rd_ptr) {
    dict.SetValue("RES_ID", rd_ptr->uuid());
    dict.SetValue("RES_FRIENDLY_NAME", rd_ptr->friendly_name());
    ResourceTopologyNodeDescriptor* rtnd = coordinator_->GetResourceTreeNode(rid);
    dict.SetValue("RES_REC", to_string(GenerateResourceTopologyEquivClass(*rtnd)));
    dict.SetValue("RES_TYPE", ENUM_TO_STRING(ResourceDescriptor::ResourceType,
                                             rd_ptr->type()));
    dict.SetValue("RES_STATUS",
                  ENUM_TO_STRING(ResourceDescriptor::ResourceState,
                                 rd_ptr->state()));
    dict.SetValue("RES_PARENT_ID", rd_ptr->parent());
    dict.SetIntValue("RES_NUM_CHILDREN", rd_ptr->children_size());
    for (RepeatedPtrField<string>::const_iterator c_iter =
         rd_ptr->children().begin();
         c_iter != rd_ptr->children().end();
         ++c_iter) {
      TemplateDictionary* child_dict =
          dict.AddSectionDictionary("RES_CHILDREN");
      child_dict->SetValue("RES_CHILD_ID", *c_iter);
    }
    dict.SetValue("RES_LOCATION", rs_ptr->location());
    dict.SetIntValue("RES_LAST_HEARTBEAT", rs_ptr->last_heartbeat());
    AddHeaderToTemplate(&dict, coordinator_->uuid(), NULL);
  } else {
    VLOG(1) << "rd_ptr is: " << rd_ptr;
    ErrorMessage_t err("Resource not found.",
                       "The requested resource does not exist.");
    AddHeaderToTemplate(&dict, coordinator_->uuid(), &err);
  }
  AddFooterToTemplate(&dict);
  string output;
  ExpandTemplate("src/webui/resource_status.tpl", ctemplate::DO_NOT_STRIP,
                 &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourcesTopologyURI(
    http::request_ptr& http_request,  // NOLINT
    tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  // Get resource topology from coordinator
  const ResourceTopologyNodeDescriptor& root_rtnd =
      coordinator_->local_resource_topology();
  // Return serialized resource topology
  http::response_writer_ptr writer = InitOkResponse(http_request,
                                                tcp_conn);
  char *json = pb2json(root_rtnd);
  writer->write(json);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleInjectURI(http::request_ptr& http_request,  // NOLINT
                                        tcp::connection_ptr& tcp_conn) {  // NOLINT
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

void CoordinatorHTTPUI::HandleJobStatusURI(http::request_ptr& http_request,  // NOLINT
                                           tcp::connection_ptr& tcp_conn) {  // NOLINT
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
    ExpandTemplate("src/webui/job_dtg.tpl", ctemplate::DO_NOT_STRIP, &dict,
                   &output);
  } else {
    output = "Please specify a job ID parameter.";
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleJobDTGURI(http::request_ptr& http_request,  // NOLINT
                                        tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  string job_id = http_request->get_query("id");
  if (!job_id.empty()) {
    // Get DTG from coordinator
    const JobDescriptor* jd = coordinator_->DescriptorForJob(job_id);
    if (!jd) {
      // Job not found here
      VLOG(1) << "Requested DTG for non-existent job " << job_id;
      ErrorResponse(http::types::RESPONSE_CODE_NOT_FOUND, http_request, tcp_conn);
      return;
    }
    // Return serialized DTG
    http::response_writer_ptr writer = InitOkResponse(http_request,
                                                  tcp_conn);
    char *json = pb2json(*jd);
    writer->write(json);
    FinishOkResponse(writer);
  } else {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
}

void CoordinatorHTTPUI::HandleReferenceURI(http::request_ptr& http_request,  // NOLINT
                                           tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string ref_id = http_request->get_query("id");
  if (ref_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  set<ReferenceInterface*>* refs =
      coordinator_->get_object_store()->GetReferences(
          DataObjectIDFromString(ref_id));
  TemplateDictionary dict("reference_view");
  if (refs && refs->size() > 0) {
    dict.SetValue("OBJ_ID", ref_id);
    for (set<ReferenceInterface*>::const_iterator ref_iter = refs->begin();
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
      sect_dict->SetIntValue("REF_SIZE", (*ref_iter)->desc().size());
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
  ExpandTemplate("src/webui/reference_view.tpl", ctemplate::DO_NOT_STRIP,
                 &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleStatisticsURI(http::request_ptr& http_request,  // NOLINT
                                            tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string res_id = http_request->get_query("res");
  string task_id = http_request->get_query("task");
  if (!(res_id.empty() || task_id.empty()) || (res_id.empty() && task_id.empty())) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    LOG(WARNING) << "Invalid stats request!";
    return;
  }
  string output = "";
  // Check if we have any statistics for this resource
  if (!res_id.empty()) {
    const deque<MachinePerfStatisticsSample>* result =
        coordinator_->knowledge_base().GetStatsForMachine(
            ResourceIDFromString(res_id));
    if (result) {
      for (deque<MachinePerfStatisticsSample>::const_iterator it =
          result->begin();
          it != result->end();
          ++it) {
        if (output.empty())
          output = "[";
        else
          output += ", ";
        output += pb2json(*it);
      }
    }
  } else if (!task_id.empty()) {
    TaskDescriptor* td = coordinator_->GetTask(TaskIDFromString(task_id));
    CHECK_NOTNULL(td);
    output += "{ \"samples\": [";
    const deque<TaskPerfStatisticsSample>* samples_result =
        coordinator_->knowledge_base().GetStatsForTask(
            TaskIDFromString(task_id));
    if (samples_result) {
      bool first = true;
      for (deque<TaskPerfStatisticsSample>::const_iterator it =
          samples_result->begin();
          it != samples_result->end();
          ++it) {
        if (!first)
          output += ", ";
        output += pb2json(*it);
        first = false;
      }
    }
    output += "]";
    output += ", \"reports\": [";
    const deque<TaskFinalReport>* report_result =
        coordinator_->knowledge_base().GetFinalStatsForTask(
            GenerateTaskEquivClass(*td));
    if (report_result) {
      bool first = true;
      for (deque<TaskFinalReport>::const_iterator it =
          report_result->begin();
          it != report_result->end();
          ++it) {
        if (!first)
          output += ", ";
        output += pb2json(*it);
        first = false;
      }
    }
    output += "] }";
  } else {
    LOG(FATAL) << "Neither task_id nor res_id set, but they should be!";
  }
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleTaskURI(http::request_ptr& http_request,  // NOLINT
                                      tcp::connection_ptr& tcp_conn) {  // NOLINT
  LogRequest(http_request);
  http::response_writer_ptr writer = InitOkResponse(http_request, tcp_conn);
  // Get resource information from coordinator
  string task_id = http_request->get_query("id");
  if (task_id.empty()) {
    ErrorResponse(http::types::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  string action = http_request->get_query("a");
  if (!action.empty()) {
    if (action == "kill") {
      coordinator_->KillRunningTask(TaskIDFromString(task_id),
                                    TaskKillMessage::USER_ABORT);
    }
  }
  TaskDescriptor* td_ptr = coordinator_->GetTask(
      TaskIDFromString(task_id));
  TemplateDictionary dict("task_status");
  if (td_ptr) {
    dict.SetFormattedValue("TASK_ID", "%ju", TaskID_t(td_ptr->uid()));
    if (td_ptr->has_name())
      dict.SetValue("TASK_NAME", td_ptr->name());
    dict.SetValue("TASK_BINARY", td_ptr->binary());
    dict.SetValue("TASK_STATUS", ENUM_TO_STRING(TaskDescriptor::TaskState,
                                                td_ptr->state()));
    // Equivalence classes
    dict.SetValue("TASK_TEC", to_string(GenerateTaskEquivClass(*td_ptr)));
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
  ExpandTemplate("src/webui/task_status.tpl", ctemplate::DO_NOT_STRIP,
                 &dict, &output);
  writer->write(output);
  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleShutdownURI(http::request_ptr& http_request,  // NOLINT
                                          tcp::connection_ptr& tcp_conn) {  // NOLINT
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
    http::request_ptr http_request,
    tcp::connection_ptr tcp_conn) {
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
    http::request_ptr http_request,
    tcp::connection_ptr tcp_conn) {
  http::response_writer_ptr writer = http::response_writer::create(
      tcp_conn, *http_request, boost::bind(&tcp::connection::finish,
                                           tcp_conn));
  http::response& r = writer->get_response();
  r.set_status_code(error_code);
  //r.set_status_message("test");
  writer->send();
}

void CoordinatorHTTPUI::FinishOkResponse(http::response_writer_ptr writer) {
  writer->send();
}

void CoordinatorHTTPUI::LogRequest(const http::request_ptr& http_request) {
  LOG(INFO) << "[HTTPREQ] Serving " << http_request->get_resource();
}

void CoordinatorHTTPUI::Init(uint16_t port) {
  try {
    // Fail if we are not assured that no existing server object is stored.
    if (coordinator_http_server_ != NULL) {
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
    // Job submission
    coordinator_http_server_->add_resource("/jobs/", boost::bind(
        &CoordinatorHTTPUI::HandleJobsListURI, this, _1, _2));
    // Job submission
    coordinator_http_server_->add_resource("/job/submit/", boost::bind(
        &CoordinatorHTTPUI::HandleJobSubmitURI, this, _1, _2));
    // Job status
    coordinator_http_server_->add_resource("/job/status/", boost::bind(
        &CoordinatorHTTPUI::HandleJobURI, this, _1, _2));
    // Job task graph visualization
    coordinator_http_server_->add_resource("/job/dtg-view/", boost::bind(
        &CoordinatorHTTPUI::HandleJobStatusURI, this, _1, _2));
    // Job task graph
    coordinator_http_server_->add_resource("/job/dtg/", boost::bind(
        &CoordinatorHTTPUI::HandleJobDTGURI, this, _1, _2));
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
    // Statistics data serving pages
    coordinator_http_server_->add_resource("/stats/", boost::bind(
        &CoordinatorHTTPUI::HandleStatisticsURI, this, _1, _2));
    // Task status
    coordinator_http_server_->add_resource("/task/", boost::bind(
        &CoordinatorHTTPUI::HandleTaskURI, this, _1, _2));
    // Shutdown request
    coordinator_http_server_->add_resource("/shutdown/", boost::bind(
        &CoordinatorHTTPUI::HandleShutdownURI, this, _1, _2));
    // Start the HTTP server
    coordinator_http_server_->start();  // spawns a thread!
    LOG(INFO) << "Coordinator HTTP interface up!";
  } catch(const std::exception& e) {
    LOG(ERROR) << "Failed running the coordinator's HTTP UI due to "
               << e.what();
  }
}

void CoordinatorHTTPUI::Shutdown(bool block) {
  LOG(INFO) << "Coordinator HTTP UI server shutting down on request.";
  coordinator_http_server_->stop(block);
  VLOG(1) << "HTTP UI shut down.";
}

}  // namespace webui
}  // namespace firmament
