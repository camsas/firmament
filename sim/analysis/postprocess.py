#!/usr/bin/python

import sys, os, re
from matplotlib import rc, use
#use('Agg')
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm

RESOURCE_UTILIZATION_SAMPLE = 0
JOB_ARRIVAL_EVENT = 1
JOB_COMPLETION_EVENT = 2

def writeout(filename_base):
  for fmt in ['pdf', 'png']:
    plt.savefig("%s.%s" % (filename_base, fmt), format=fmt)

# -----------------------------------

rc('font',**{'family':'serif','sans-serif':['Helvetica'],'serif':['Times'],
             'size':12})
rc('text', usetex=True)
rc('legend', fontsize=8)
#rc('figure', figsize=(6,4))
#rc('figure.subplot', left=0.15, top=0.85, bottom=0.075, right=0.95)
rc('axes', linewidth=0.5)
rc('lines', linewidth=0.5)

basename = sys.argv[2]

arr_ts = []
arr_events = []
lea_ts = []
lea_events = []
utilization_series = {}
last_arrival = 0
# scan each input file and record the values for cells with MR jobs
with open(sys.argv[1]) as eventlog:
  for x in eventlog.readlines():

    fields = x.split()
    event_type = int(fields[0].strip())
    timestamp = float(fields[1].strip())

    if event_type == JOB_ARRIVAL_EVENT:
      job_uid = int(fields[2].strip())
      size = int(fields[3].strip())
      interarrival_time = timestamp - last_arrival
      last_arrival = timestamp
      arr_events.append([timestamp, interarrival_time, size])
    elif event_type == JOB_COMPLETION_EVENT:
      job_uid = int(fields[2].strip())
      runtime = float(fields[3].strip())
      lea_events.append([timestamp, runtime])
    elif event_type == RESOURCE_UTILIZATION_SAMPLE:
      ensemble_uid = int(fields[2].strip())
      occupied_workers = int(fields[3].strip())
      occupied_percent = float(fields[4].strip())
      pending_queue_len = int(fields[5].strip())
      if ensemble_uid in utilization_series:
        utilization_series[ensemble_uid].append([timestamp, occupied_workers,
                                                 occupied_percent,
                                                 pending_queue_len])
      else:
        utilization_series[ensemble_uid] = [[timestamp, occupied_workers,
                                             occupied_percent,
                                             pending_queue_len]]

    last_timestamp = timestamp

print "last timestamp was: %f" % last_timestamp

fig = plt.figure()

util_ts = {}
res_use_abs = {}
res_use_rel = {}
pending_queue_len = {}
for uid, series in utilization_series.items():
  if not uid in util_ts:
    util_ts[uid] = []
    res_use_abs[uid] = []
    res_use_rel[uid] = []
    pending_queue_len[uid] = []
  for event in series:
    util_ts[uid].append(event[0])
    res_use_abs[uid].append(event[1])
    res_use_rel[uid].append(event[2])
    pending_queue_len[uid].append(event[3])

if (len(res_use_abs) > 0):
  ax3 = fig.add_subplot(311)
  for uid, series in util_ts.items():
    ax3.plot(util_ts[uid], res_use_abs[uid], label="abs resource usage (" +
             str(uid) + ")")
  ax3.set_ylabel(u'')
  ax3.set_xticks([])
  plt.legend()

if (len(res_use_rel) > 0):
  ax3 = fig.add_subplot(312)
  for uid, series in util_ts.items():
    ax3.plot(util_ts[uid], res_use_rel[uid], label="resource utilization (" +
             str(uid) + ")")
  ax3.set_ylabel(u'')
  plt.legend()

if (len(pending_queue_len) > 0):
  ax4 = fig.add_subplot(313)
  for uid, series in util_ts.items():
    ax4.plot(util_ts[uid], pending_queue_len[uid], label="pending queue length"
             + "(" + str(uid) + ")")
  ax4.set_xlabel(u'Time')
  ax4.set_ylabel(u'')
  #ax3.set_xticks([])
  plt.legend(loc=2)
  plt.xlabel(u'Time')

writeout(basename + '/simulation_overview')

# -------------------------------------------------

leave_event_series = []
job_runtimes = []
for event in lea_events:
  leave_event_series.append(event[0])
  job_runtimes.append(event[1])

fig2 = plt.figure()

if (len(lea_events) > 0):
  ax = fig2.add_subplot(111)
  ax.hist(leave_event_series, normed=False, bins=200)
  plt.xlabel(u'Time')

writeout(basename + '/leave_histogram')

# -------------------------------------------------

fig3 = plt.figure()

if (len(job_runtimes) > 0):
  ax = fig3.add_subplot(111)
#  ax.hist(job_runtimes, normed=True, log=False, cumulative=True,
#          bins=10**np.linspace(0, 4, 1000), histtype='step')
  ax.hist(job_runtimes, normed=True, log=False, cumulative=True,
          bins=200, histtype='step')
  plt.xlabel(u'Job runtime')
  writeout(basename + '/job_runtimes_cdf')

  ax.set_xscale('log')
  writeout(basename + '/job_runtimes_cdf_log')


# -------------------------------------------------

fig4 = plt.figure()

c_sizes = []
interarrival_times = []
for event in arr_events:
  c_sizes.append(event[2])
  interarrival_times.append(event[1])

if (len(c_sizes) > 0):
  ax = fig4.add_subplot(111)
  ax.hist(c_sizes, normed=True, log=False, cumulative=True,
          bins=10**np.linspace(0, 4, 1000), histtype='step')
  plt.xlabel(u'Collection size in number of VMs')
  writeout(basename + '/collection_size_cdf')

  ax.set_xscale('log')
  writeout(basename + '/collection_size_cdf_log')

# ------

fig5 = plt.figure()

plt.hist(interarrival_times, normed=False, bins=200)
plt.xlabel('Interarrival time in seconds')
plt.ylabel('count')

writeout(basename + '/interarrival_times')

plt.clf()

#plt.hist(interarrival_times, normed=False, bins=10**np.linspace(0, 3, 1000))
plt.hist(interarrival_times, normed=False, bins=200, log=True)
plt.xlabel('Interarrival time in seconds')
#plt.yscale('log')
plt.ylabel('count')

writeout(basename + '/interarrival_times_logy')

# -------------------------------------------------
