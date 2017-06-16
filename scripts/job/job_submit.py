from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import json_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex

if len(sys.argv) < 4:
  print "usage: job_submit.py <coordinator hostname> <web UI port> " \
      "<task binary> [<args>] [<job name>] [<input object>]"
  sys.exit(1)

hostname = sys.argv[1]
port = int(sys.argv[2])

job_desc = job_desc_pb2.JobDescriptor()

job_desc.uuid = "" # UUID will be set automatically on submission
if len(sys.argv) > 5:
  job_desc.name = sys.argv[5]
else:
  job_desc.name = "anonymous_job_at_%d" % (int(time.time()))
job_desc.root_task.uid = 0
job_desc.root_task.name = "root_task"
job_desc.root_task.state = task_desc_pb2.TaskDescriptor.CREATED
job_desc.root_task.binary = sys.argv[3]
job_desc.root_task.priority = 5
job_desc.root_task.resource_request.cpu_cores = 0.1
job_desc.root_task.resource_request.ram_cap = 131072
job_desc.root_task.resource_request.net_tx_bw = 0
job_desc.root_task.resource_request.net_rx_bw = 0
job_desc.root_task.resource_request.disk_bw = 0
if len(sys.argv) > 4:
  job_desc.root_task.args.extend(shlex.split(sys.argv[4]))
#job_desc.root_task.args.append("--v=2")
job_desc.root_task.inject_task_lib = True
if len(sys.argv) == 7:
  input_id = binascii.unhexlify(sys.argv[6])
else:
  input_id = binascii.unhexlify('feedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeef')
output_id = binascii.unhexlify('db33daba280d8e68eea6e490723b02cedb33daba280d8e68eea6e490723b02ce')
output2_id = binascii.unhexlify('feedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeeffeedcafedeadbeef')
job_desc.output_ids.append(output_id)
job_desc.output_ids.append(output2_id)
input_desc = job_desc.root_task.dependencies.add()
input_desc.id = input_id
input_desc.scope = reference_desc_pb2.ReferenceDescriptor.PUBLIC
input_desc.type = reference_desc_pb2.ReferenceDescriptor.CONCRETE
input_desc.non_deterministic = False
input_desc.location = "blob:/tmp/fib_in"
final_output_desc = job_desc.root_task.outputs.add()
final_output_desc.id = output_id
final_output_desc.scope = reference_desc_pb2.ReferenceDescriptor.PUBLIC
final_output_desc.type = reference_desc_pb2.ReferenceDescriptor.FUTURE
final_output_desc.non_deterministic = True
final_output_desc.location = "blob:/tmp/out1"
final_output2_desc = job_desc.root_task.outputs.add()
final_output2_desc.id = output2_id
final_output2_desc.scope = reference_desc_pb2.ReferenceDescriptor.PUBLIC
final_output2_desc.type = reference_desc_pb2.ReferenceDescriptor.FUTURE
final_output2_desc.non_deterministic = True
final_output2_desc.location = "blob:/tmp/out2"

jd_as_json = json_format.MessageToJson(job_desc)
params = urllib.urlencode({'jd': jd_as_json })
print "SUBMITTING job with parameters:"
print jd_as_json
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
