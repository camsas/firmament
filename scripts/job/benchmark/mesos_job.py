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

  def add_root_task(self, binary, args=[], inject_task_lib=True):
    self.root_task = Task(self.desc.root_task, self.desc, 0, binary, args)

  def mesos_run_helper(self, master_hostname, master_port):
    try:
      # XXX(malte): hack -- this (for now) assumes that mesos-execute is
      # available in $(PWD)
      ret = subprocess.call("./mesos-execute --master=%s:%d --name=%s " \
                            "--command=\"%s %s\" --instances=%d" \
                            % (master_hostname, master_port, self.desc.name,
                               self.root_task.desc.binary,
                               " ".join(self.root_task.desc.args), \
                               len(self.root_task.subtasks) + 1), shell=True)
    except Exception as e:
      print "ERROR: failed to submit job to Mesos: %s" % (e)
      return (False, "")
    return (True, "")

  def submit(self, master_hostname, master_port, verbose=False):
    self.desc.name = "%s-%d" % (self.job_name, self.instance)
    params = 'test=%s' % text_format.MessageToString(self.desc)
    if verbose:
      print "SUBMITTING job \"%s\" with parameters:" % (self.desc.name)
      print params
      print ""

    # create a thread to run the CLI 'framework' in
    self.mesos_execute_thread = threading.Thread(
            target=self.mesos_run_helper,
            args=(master_hostname, master_port))
    self.mesos_execute_thread.start()
    if self.mesos_execute_thread.is_alive():
      return (True, self.desc.name)
    else
      print "ERROR: mesos-execute failed to run"
      return (False, "")

  def prepare(self, binary, args, num_tasks, name="", inject_task_lib=True):
    self.add_root_task(binary, args, inject_task_lib)
    for i in range(1, num_tasks):
      self.root_task.add_subtask(binary, args, i)

  def completed(self, hostname, port):
    job_id = self.desc.uuid

    if self.mesos_execute_thread.is_alive():
      return False
    else:
      self.mesos_execute_thread.join()
      return True
