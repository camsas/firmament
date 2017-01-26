from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import json
import time
import shlex
from task import *

class Job:
  def __init__(self, name):
    self.job_name = name
    self.instance = 0
    self.desc = job_desc_pb2.JobDescriptor()

    self.desc.uuid = "" # UUID will be set automatically on submission
    if name != "":
      self.desc.name = name
    else:
      self.desc.name = "anonymous_job_at_%d" % (int(time.time()))

  def add_root_task(self, binary, args=[], inject_task_lib=True,
                    resource_request=None):
    self.root_task = Task(self.desc.root_task, self.desc, 0, binary, args,
                          self.task_type)
    if resource_request:
      self.root_task.add_resource_request(resource_request)

  def submit(self, hostname, port, verbose=False):
    self.desc.name = "%s/%d" % (self.job_name, self.instance)
    params = 'jd=%s' % text_format.MessageToString(self.desc)
    if verbose:
      print "SUBMITTING job \"%s\" with parameters:" % (self.desc.name)
      print params
      print ""

    try:
      headers = {"Content-type": "application/x-www-form-urlencoded"}
      conn = httplib.HTTPConnection("%s:%s" % (hostname, port))
      conn.request("POST", "/job/submit/", params, headers)
      response = conn.getresponse()
    except Exception as e:
      print "ERROR connecting to coordinator: %s" % (e)
      return (False, "")

    data = response.read()
    match = re.search(r"([0-9a-f\-]+)", data, re.MULTILINE | re.S | re.I | re.U)
    if verbose:
      print "----------------------------------------------"
    if match and response.status == 200:
      job_id = match.group(1)
      if verbose:
        print "JOB SUBMITTED successfully!\nJOB ID is %s\nStatus page: " \
            "http://%s:%d/job/status/?id=%s" % (job_id, hostname, port, job_id)
      else:
        pass
      self.desc.uuid = job_id
      return (True, job_id)
    else:
      if verbose:
        print "ERROR submitting job -- response was: %s (Code %d)" \
                % (response.reason, response.status)
      return (False, "")
    if verbose:
      print "----------------------------------------------"
    conn.close()

  def prepare(self, binary, task_args, num_tasks, name="", inject_task_lib=True,
              task_type=task_desc_pb2.TaskDescriptor.TURTLE,
              resource_request=None):
    self.task_type = task_type
    self.add_root_task(binary, task_args[0], inject_task_lib, resource_request)
    # add more tasks
    for i in range(1, num_tasks):
      self.root_task.add_subtask(binary, task_args[i], i, task_type, resource_request)

  def completed(self, hostname, port):
    job_id = self.desc.uuid

    try:
      conn = httplib.HTTPConnection("%s:%s" % (hostname, port))
      conn.request("GET", "/job/completion/?id=%s&json=1" % (job_id))
      response = conn.getresponse()
    except Exception as e:
      print "ERROR connecting to coordinator: %s" % (e)
      return False

    data = response.read()
    try:
      as_json = json.loads(data)
    except Exception as e:
      print "ERROR parsing response to JSON: %s" % (e)
      return False

    if "COMPLETED" in as_json["job_status"]:
      return True
    else:
      return False
