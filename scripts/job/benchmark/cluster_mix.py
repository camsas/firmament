from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex
import argparse
from job import *
from task import *
from workload import *

def parse_arguments():
  parser.add_argument("--host", dest="scheduler_hostname",
                      default="localhost",
                      help="Scheduling coordinator hostname")
  parser.add_argument("--port", dest="scheduler_port", type=int,
                      default=8080, help="Scheduler coordinator port")
  parser.add_argument("-d", "--duration", dest="duration", type=int,
                      default=120, help="Experiment runtime in sec.")
  parser.add_argument("-t", "--target", dest="target",
                      default="firmament",
                      help="Target system (firmament, mesos)")

  arguments = parser.parse_args()
  return arguments

parser = argparse.ArgumentParser(description="Run cluster mix.")

arguments = parse_arguments()

scheduler_hostname = arguments.scheduler_hostname
scheduler_port = arguments.scheduler_port
duration = arguments.duration
target = arguments.target

print time.ctime()
print "Running experiment for %d seconds." % (duration)
cur_time = time.time()
start_time = cur_time

wl = Workload(scheduler_hostname, scheduler_port, target)
wl.add("test-sleep", "/bin/sleep", "10", 8)

wl.start()

while cur_time < start_time + duration:
  wl.restart_completed()
  cur_time = time.time()
  time.sleep(10)

print "Done!"
print time.ctime()
