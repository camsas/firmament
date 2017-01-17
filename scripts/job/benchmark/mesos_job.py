from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex
import threading
import subprocess
from task import *

class MesosJob:
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

  def mesos_run_helper(self, hostname, port):
    try:
      ret = subprocess.call("mesos-execute --master=%s:%d --name=%s " \
                            "--command=\"%s %s\" " \
                            "--resources=\"cpus:%f;mem:%d;disk:%d\"" \
                            % (hostname, port, self.desc.name, \
                               self.root_task.desc.binary, \
                               " ".join(self.root_task.desc.args), \
                               self.root_task.desc.resource_request.cpu_cores,
                               self.root_task.desc.resource_request.ram_cap,
                               self.root_task.desc.resource_request.ram_cap), \
                            shell=True)
    except Exception as e:
      print "ERROR submitted job to Mesos: %s" % (e)
      return (False, "")
    return (True, "")

  def submit(self, hostname, port, verbose=False):
    self.desc.name = "%s-%d" % (self.job_name, self.instance)
    params = 'jd=%s' % text_format.MessageToString(self.desc)
    if verbose:
      print "SUBMITTING job \"%s\" with parameters:" % (self.desc.name)
      print params
      print ""

    self.mesos_execute_thread = threading.Thread(target=self.mesos_run_helper, args=(hostname, port))
    self.mesos_execute_thread.start()
    if self.mesos_execute_thread.is_alive():
      return (True, "")

  def prepare(self, binary, args, name="", inject_task_lib=True,
              task_type=task_desc_pb2.TaskDescriptor.TURTLE,
              resource_request=None):
    self.task_type = task_type
    self.add_root_task(binary, args, inject_task_lib, resource_request)

  def completed(self, hostname, port):
    job_id = self.desc.uuid

    if self.mesos_execute_thread.is_alive():
      return False
    else:
      self.mesos_execute_thread.join()
      return True
