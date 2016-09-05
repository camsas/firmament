from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from base import resource_vector_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex
import Queue
from datetime import datetime
from mesos_job import *
from job import *
from task import *

class SyncWorkload:
  def __init__(self, host, port, target):
    self.events = Queue.PriorityQueue()
    self.scheduler_host = host
    self.scheduler_port = port
    self.target = target

  def add(self, run_at_time, name, binary, tasks_args, task_count,
          task_type=task_desc_pb2.TaskDescriptor.TURTLE,
          resource_request=None):
    if self.target == "firmament":
      new_job = Job(name)
      new_job.prepare(binary, tasks_args, task_count, task_type=task_type,
                      resource_request=resource_request)
      self.events.put((run_at_time, new_job))
    elif self.target == "mesos":
      if task_count != 1:
        print 'ERROR: Mesos jobs can not have more than 1 task'
      new_job = MesosJob(name)
      new_job.prepare(binary, tasks_args, task_type=task_type,
                      resource_request=resource_request)
      self.events.put((run_at_time, new_job))
    else:
      print "ERROR: Unexpected target %s" % (self.target)


  def start(self):
    self.start_time = datetime.now()
    while self.events.empty() is False:
      (run_at_time, job) = self.events.get()
      time_diff = datetime.now() - self.start_time
      time_now_ms = (time_diff.days * 24 * 60 * 60 + time_diff.seconds) * 1000 + time_diff.microseconds / 1000
      if run_at_time <= time_now_ms:
        print '... Running at: ', time_now_ms
        (success, job_id) = job.submit(self.scheduler_host, self.scheduler_port)
        if success:
          print "... running (%s)" % (job_id)
        else:
          print "... ERROR"
      else:
        self.events.put((run_at_time, job))
