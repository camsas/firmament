from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex

class Task:
  def __init__(self, task_desc, job, task_index, binary, args=[], \
               task_type=task_desc_pb2.TaskDescriptor.TURTLE):
    self.desc = task_desc
    self.subtasks = []
    self.job = job

    self.desc.uid = 0  # automatically filled in by coordinator
    self.desc.name = "%s/%d" % (job.name, task_index)
    self.desc.state = task_desc_pb2.TaskDescriptor.CREATED
    self.desc.binary = binary
    if len(args) > 0:
      self.desc.args.extend(shlex.split(args))
    # XXX(malte): don't hardcode the following
    self.desc.inject_task_lib = True
    self.desc.task_type = task_type

  def add_subtask(self, binary, args, task_index, task_type=None):
    if task_type is None:
      task_type = self.desc.task_type
    new_desc = self.desc.spawned.add()
    new_task = Task(new_desc, self.job, task_index, binary, args, task_type)
    self.subtasks.append(new_task)

  def add_resource_request(self, cores, ram, net_bw, disk_bw):
    self.desc.resource_request.cpu_cores = cores
    self.desc.resource_request.ram_gb = ram
    self.desc.resource_request.net_bw = net_bw
    self.desc.resource_request.disk_bw = disk_bw


