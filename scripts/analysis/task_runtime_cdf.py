#!/usr/bin/python

import sys, re
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
  print "usage: task_runtime_cdf.py <log file>"
  sys.exit(1)

inputfile = sys.argv[1]

tasks = {}

# read and process log file
for line in open(inputfile).readlines():
  rec = re.match("[A-Z][0-9]+ ([0-9:\.]+) [0-9]+ .+\] (.+)", line)
  if not rec:
    print "ERROR: failed to match line %s" % (line)
  else:
    timestamp_str = rec.group(1)
    message_str = rec.group(2)
    timestamp = datetime.strptime(timestamp_str, "%H:%M:%S.%f")

    m = re.match("About to fork child process for task execution of ([0-9]+)!",
                 message_str)
    if m:
      task_id = long(m.group(1))
      tasks[task_id] = { 'start': timestamp, 'end': None }
    
    m = re.match("Task ([0-9]+) now in state COMPLETED.", message_str)
    if m:
      task_id = long(m.group(1))
      tasks[task_id]['end'] = timestamp

runtimes = []
for tid, d in tasks.items():
  print "%d: %f" % (tid, (d['end'] - d['start']).total_seconds())
  runtimes.append((d['end'] - d['start']).total_seconds())

plt.figure()

plt.hist(runtimes, bins=200)

plt.ylabel("Count")
plt.xlabel("Task runtime [sec]")

plt.savefig("task_runtime_hist.pdf", format="pdf", bbox_inches='tight')

plt.clf()

plt.hist(runtimes, bins=200, histtype='step', cumulative=True, normed=True)

plt.xlabel("Task runtime [sec]")

plt.savefig("task_runtime_cdf.pdf", format="pdf", bbox_inches='tight')
