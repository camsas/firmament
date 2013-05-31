#!/usr/bin/python

import sys, re
from datetime import datetime

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
  print "usage: scheduling_duration_cdf.py <log file 0> <label 0> " \
      "<log file 1> <label 1> ..."
  sys.exit(1)

durations = {}

for i in range(1, len(sys.argv), 2):
  inputfile = sys.argv[i]
  label = sys.argv[i+1]

  att_start = None

  # read and process log file
  for line in open(inputfile).readlines():
    rec = re.match("[A-Z][0-9]+ ([0-9:\.]+)\s+[0-9]+ .+\] (.+)", line)
    if not rec:
      #print "ERROR: failed to match line %s" % (line)
      pass
    else:
      timestamp_str = rec.group(1)
      message_str = rec.group(2)
      timestamp = datetime.strptime(timestamp_str, "%H:%M:%S.%f")

      m = re.match("START SCHEDULING (.+)",
                   message_str)
      if m:
        if att_start == None:
          job_id = m.group(1)
          att_start = timestamp
        else:
          print "ERROR: overlapping scheduling events?"
      
      m = re.match("STOP SCHEDULING (.+).", message_str)
      if m:
        if att_start != None:
          job_id = m.group(1)
          duration = timestamp - att_start
          if not label in durations:
            durations[label] = []
          durations[label].append(duration.total_seconds())
          att_start = None
        else:
          print "ERROR: overlapping scheduling events?"

plt.figure()

for l, d in durations.items():
  plt.hist(d, bins=200, label=l)

plt.legend(loc=4)
plt.ylabel("Count")
plt.xlabel("Scheduler runtime [sec]")

plt.savefig("scheduling_duration_hist.pdf", format="pdf", bbox_inches='tight')

plt.clf()

for l, d in durations.items():
  plt.hist(d, bins=200, histtype='step', cumulative=True, normed=True, label=l,
           lw=2.0)

plt.legend(loc=4)
plt.ylim(0, 1)
plt.xlabel("Scheduler runtime [sec]")

plt.savefig("scheduling_duration_cdf.pdf", format="pdf", bbox_inches='tight')
