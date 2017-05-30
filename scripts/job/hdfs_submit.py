from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex

if len(sys.argv) < 4:
  print "usage: job_submit.py <coordinator hostname> <web UI port> " \
      "<job name> <input file> <args>"
  sys.exit(1)

hostname = sys.argv[1]
port = int(sys.argv[2])

job_desc = job_desc_pb2.JobDescriptor()

job_desc.uuid = "" # UUID will be set automatically on submission
job_desc.name = sys.argv[3]
job_desc.root_task.uid = 0
job_desc.root_task.name = "root_task"
job_desc.root_task.state = task_desc_pb2.TaskDescriptor.CREATED
job_desc.root_task.binary = "/usr/bin/hadoop"
job_desc.root_task.priority = 5
job_desc.root_task.resource_request.cpu_cores = 0.1
job_desc.root_task.resource_request.ram_cap = 131072
job_desc.root_task.resource_request.net_tx_bw = 0
job_desc.root_task.resource_request.net_rx_bw = 0
job_desc.root_task.resource_request.disk_bw = 0
if len(sys.argv) > 5:
  job_desc.root_task.args.extend(shlex.split(sys.argv[5]))
#job_desc.root_task.args.append("--v=2")
job_desc.root_task.inject_task_lib = True
input_desc = job_desc.root_task.dependencies.add()
input_desc.id = binascii.unhexlify('feedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeef')
input_desc.location = sys.argv[4]
output_id = binascii.unhexlify('db33daba280d8e68eea6e490723b02cedb33daba280d8e68eea6e490723b02ce')
job_desc.output_ids.append(output_id)
input_desc.scope = reference_desc_pb2.ReferenceDescriptor.PUBLIC
input_desc.type = reference_desc_pb2.ReferenceDescriptor.CONCRETE
input_desc.non_deterministic = False
final_output_desc = job_desc.root_task.outputs.add()
final_output_desc.id = output_id
final_output_desc.scope = reference_desc_pb2.ReferenceDescriptor.PUBLIC
final_output_desc.type = reference_desc_pb2.ReferenceDescriptor.FUTURE
final_output_desc.non_deterministic = True
final_output_desc.location = "blob:/tmp/out1"


#params = urllib.urlencode({'test': text_format.MessageToString(job_desc)})
params = 'jd=%s' % text_format.MessageToString(job_desc)
print "SUBMITTING job with parameters:"
print params
print ""

try:
  headers = {"Content-type": "application/x-www-form-urlencoded"}
  conn = httplib.HTTPConnection("%s:%s" % (hostname, port))
  conn.request("POST", "/job/submit/", params, headers)
  response = conn.getresponse()
except Exception as e:
  print "ERROR connecting to coordinator: %s" % (e)
  sys.exit(1)

data = response.read()
match = re.search(r"([0-9a-f\-]+)", data, re.MULTILINE | re.S | re.I | re.U)
print "----------------------------------------------"
if match and response.status == 200:
  job_id = match.group(1)
  print "JOB SUBMITTED successfully!\nJOB ID is %s\nStatus page: " \
      "http://%s:%d/job/status/?id=%s" % (job_id, hostname, port, job_id)
else:
  print "ERROR submitting job -- response was: %s (Code %d)" % (response.reason,
                                                                response.status)
print "----------------------------------------------"
conn.close()
