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
JOB_HANDOFF_TO_PEERS_EVENT = 3

def writeout(filename_base):
  for fmt in ['pdf', 'png']:
    plt.savefig("%s.%s" % (filename_base, fmt), format=fmt)

# -----------------------------------

rc('font',**{'family':'serif','sans-serif':['Helvetica'],'serif':['Times'],
             'size':12})
rc('text', usetex=True)
rc('legend', fontsize=8)
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
handoff_series = {}
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
      pending_tasks = int(fields[6].strip())
      if ensemble_uid in utilization_series:
        utilization_series[ensemble_uid].append([timestamp, occupied_workers,
                                                 occupied_percent,
                                                 pending_queue_len,
                                                 pending_tasks])
      else:
        utilization_series[ensemble_uid] = [[timestamp, occupied_workers,
                                             occupied_percent,
                                             pending_queue_len,
                                             pending_tasks]]
    elif event_type == JOB_HANDOFF_TO_PEERS_EVENT:
      job_uid = int(fields[2].strip())
      ensemble_uid = int(fields[3].strip())
      num_tasks = int(fields[4].strip())
      if ensemble_uid in handoff_series:
        handoff_series[ensemble_uid].append([timestamp, num_tasks])
      else:
        handoff_series[ensemble_uid] = [[timestamp, num_tasks]]

    last_timestamp = timestamp

print "last timestamp was: %f" % last_timestamp

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
          bins=1000, histtype='step')
  plt.xlabel(u'Job runtime')
  plt.ylim(0,1.0)
  plt.xlim(0,800)
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

fig = plt.figure()
rc('figure', figsize=(12,8))

util_ts = {}
res_use_abs = {}
res_use_rel = {}
pending_queue_len = {}
pending_tasks = {}
for uid, series in utilization_series.items():
  if not uid in util_ts:
    util_ts[uid] = []
    res_use_abs[uid] = []
    res_use_rel[uid] = []
    pending_queue_len[uid] = []
    pending_tasks[uid] = []
  for event in series:
    util_ts[uid].append(event[0])
    res_use_abs[uid].append(event[1])
    res_use_rel[uid].append(event[2])
    pending_queue_len[uid].append(event[3])
    pending_tasks[uid].append(event[4])

handoff_ts = {}
handoff_counts = {}
handoff_hist = []
for uid, series in handoff_series.items():
  if not uid in handoff_ts:
    handoff_ts[uid] = []
    handoff_counts[uid] = []
  for event in series:
    handoff_ts[uid].append(event[0])
    handoff_counts[uid].append(event[1])
    for i in range(event[1]):
      handoff_hist.append(event[0])

if (len(res_use_abs) > 0):
  ax3 = fig.add_subplot(511)
  for uid, series in util_ts.items():
    ax3.plot(util_ts[uid], res_use_abs[uid], label="abs resource usage (" +
             str(uid) + ")")
  ax3.set_ylabel(u'')
  ax3.set_xticks([])
  plt.xlim(0, last_timestamp)
  plt.legend()

if (len(res_use_rel) > 0):
  ax3 = fig.add_subplot(512)
  for uid, series in util_ts.items():
    ax3.plot(util_ts[uid], res_use_rel[uid], label="resource utilization (" +
             str(uid) + ")")
  ax3.set_ylabel(u'')
  plt.legend()
  plt.xlim(0, last_timestamp)

if (len(pending_queue_len) > 0):
  ax4 = fig.add_subplot(513)
  for uid, series in util_ts.items():
    ax4.plot(util_ts[uid], pending_queue_len[uid], label="pending queue length"
             + "(" + str(uid) + ")")
  ax4.set_xlabel(u'')
  ax4.set_ylabel(u'')
  #ax3.set_xticks([])
  plt.legend()
  plt.xlabel(u'Time')
  plt.xlim(0, last_timestamp)
  plt.ylim(0, 50)

if (len(pending_tasks) > 0):
  ax5 = fig.add_subplot(514)
  for uid, series in util_ts.items():
    ax5.plot(util_ts[uid], pending_tasks[uid], label="pending tasks (" +
             str(uid) + ")")
  ax5.set_xlabel(u'')
  ax5.set_ylabel(u'')
  plt.legend()
  plt.xlabel(u'')
  plt.xlim(0, last_timestamp)
  plt.ylim(0, 500)

if (len(handoff_series) > 0):
  ax4 = fig.add_subplot(515)
#  for uid, series in handoff_ts.items():
#    ax4.bar(handoff_ts[uid], handoff_counts[uid],
#            label="number of tasks handed off (" + str(uid) + ")")
  ax4.hist(handoff_hist, bins=int(last_timestamp), histtype='step')
#  ax4.set_xlabel(u'Time')
#  ax4.set_ylabel(u'')
  #ax3.set_xticks([])
#  plt.legend()
  plt.xlabel(u'Time')
  plt.xlim(0,last_timestamp)
#  plt.ylim(0,10)

writeout(basename + '/simulation_overview')
