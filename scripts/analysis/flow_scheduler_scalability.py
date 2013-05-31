#!/usr/bin/python

import sys, re
from datetime import timedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
  print "usage: flow_scheduler_scalability.py <log file 0> " \
      "<log file 1> ..."
  sys.exit(1)

durations_x = []
durations_y = []

for i in range(1, len(sys.argv)):
  inputfile = sys.argv[i]

  # read and process log file
  for line in open(inputfile).readlines():
    m = re.match("/tmp/test([0-9]+).dm", line)
    if m:
      size = long(m.group(1))
      durations_x.append(size)
    
    m = re.match("real\s+([0-9]+)m([0-9]+)\.([0-9]+)s", line)
    if m:
      mins = int(m.group(1))
      secs = int(m.group(2))
      msecs = int(m.group(3))
      td = timedelta(0, secs, 0, msecs, mins, 0, 0)
      durations_y.append(td.total_seconds())

# sorting
combined = zip(durations_x, durations_y)
sorted_combined = sorted(combined)
durations_x = [x[0] for x in sorted_combined]
durations_y = [x[1] for x in sorted_combined]

print durations_x
print durations_y

plt.figure()

plt.plot(durations_x, durations_y)

plt.legend(loc=4)
plt.ylabel("Runtime [sec]")
plt.xlabel("Number of jobs [100 tasks with 20 prefs each]")

plt.savefig("flow_scheduler_scalability.pdf", format="pdf", bbox_inches='tight')
