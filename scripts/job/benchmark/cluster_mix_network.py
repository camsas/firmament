import sys
sys.path.append('/home/srguser/firmament/scripts/job')
from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from base import resource_vector_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex
import argparse
import random
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

random.seed(42)

bin_path = "/home/srguser/firmament-experiments/workloads/synthetic"

wl = Workload(scheduler_hostname, scheduler_port, target)

# HDFS JOB
rv = resource_vector_pb2.ResourceVector()
rv.cpu_cores = 0.9
rv.ram_cap = 128
rv.net_bw = 600
rv.disk_bw = 0

# 3 NGINX with 4 AB each
# 1 PS with 15 workers

# 3GB of input (1)
for i in range(0, 2):
  wl.add("hdfs_get_%d" % (i), bin_path + "/hdfs/hdfs_get",
         "caelum10g-301.cl.cam.ac.uk 8020 /input/test_data/task_runtime_events.csv", 1, 2, rv)

# About 3.7GB of input (8)
# for i in range(0, 8):
#   wl.add("hdfs_get_%d" % (i), bin_path + "/hdfs/hdfs_get",
#          "caelum10g-301.cl.cam.ac.uk 8020 /input/sssp_tw_edges_splits8/sssp_tw_edges%d.in" % (i),
#          1, 2, rv)

# About 3.9GB of input (16)
for i in range(0, 16):
  wl.add("hdfs_get_%d" % (i), bin_path + "/hdfs/hdfs_get",
         "caelum10g-301.cl.cam.ac.uk 8020 /input/pagerank_uk-2007-05_edges_splits16/pagerank_uk-2007-05_edges%d.in" % (i),
         1, 2, rv)

# About 1.4GB of input (14)
for i in range(0, 14):
  wl.add("hdfs_get_%d" % (i), bin_path + "/hdfs/hdfs_get",
         "caelum10g-301.cl.cam.ac.uk 8020 /input/lineitem_splits14/lineitem%d.in" % (i),
         1, 2, rv)

wl.start()

while cur_time < start_time + duration:
  wl.restart_completed()
  cur_time = time.time()
  time.sleep(2)

print "Done!"
print time.ctime()
