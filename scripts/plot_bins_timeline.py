import matplotlib
matplotlib.use("agg")
import os, sys
import matplotlib.pyplot as plt

filename = sys.argv[1]

times = []
data = []
for l in open(filename).readlines():
  fields = [x.strip() for x in l.split(" ")]
  times.append(int(fields[0][1:-1]))
  data.append(int(fields[2]))

plt.figure(figsize=(20,4))
plt.plot(times, data)
plt.savefig("bins.pdf", format="pdf")
