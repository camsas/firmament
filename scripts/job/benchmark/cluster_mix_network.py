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
from sync_workload import *

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

wl = SyncWorkload(scheduler_hostname, scheduler_port, target)

# HDFS JOB
rv = resource_vector_pb2.ResourceVector()
rv.cpu_cores = 0.9
rv.ram_cap = 128
rv.net_bw = 600
rv.disk_bw = 0

# 3 NGINX with 4 AB each
# 3 PS with 4 workers

# 3GB of input (2)
tasks_args = ["caelum10g-301.cl.cam.ac.uk 8020 /input/test_data/task_runtime_events.csv",
              "caelum10g-301.cl.cam.ac.uk 8020 /input/test_data/task_runtime_events.csv"]

for i in range(0, 96000, 8000):
  wl.add(i, "hdfs_get_task_runtime_events%d" % (i), bin_path + "/hdfs/hdfs_get", tasks_args, 2, 2, rv)

# About 3.7GB of input (8)
tasks_args = []
for i in range(0, 8):
  tasks_args.append("caelum10g-301.cl.cam.ac.uk 8020 /input/sssp_tw_edges_splits8/sssp_tw_edges%d.in" % (i))

for i in range(2000, 96000, 8000):
  wl.add(i, "hdfs_get_sspp_tw%d" % (i), bin_path + "/hdfs/hdfs_get", tasks_args, 8, 2, rv)

# About 3.9GB of input (16). Each task takes about 6-8 seconds.
tasks_args = []
for i in range(0, 16):
  tasks_args.append("caelum10g-301.cl.cam.ac.uk 8020 /input/pagerank_uk-2007-05_edges_splits16/pagerank_uk-2007-05_edges%d.in" % (i))

for i in range(6000, 96000, 8000):
  wl.add(i, "hdfs_get_pagerank_uk%d" % (i), bin_path + "/hdfs/hdfs_get", tasks_args, 16, 2, rv)

# About 1.4GB of input (14). Each task takes about 6-8 seconds.
tasks_args = []
for i in range(0, 14):
  tasks_args.append("caelum10g-301.cl.cam.ac.uk 8020 /input/lineitem_splits14/lineitem%d.in" % (i))

for i in range(8000, 96000, 8000):
  wl.add(i, "hdfs_get_lineitem%d" % (i), bin_path + "/hdfs/hdfs_get", tasks_args, 14, 2, rv)

wl.start()

print "Done!"
print time.ctime()
