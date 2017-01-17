from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random
import binascii
import time
import shlex

def add_worker_task(task, binary, args, job_name, worker_id, num_workers, extra_args):
  task.uid = 0
  task.name = "%s/%d" % (job_name, worker_id)
  task.state = task_desc_pb2.TaskDescriptor.CREATED
  task.binary = "/usr/bin/python"
  task.args.extend(args)
  task.args.append(str(worker_id))
  task.args.append(str(num_workers))
  task.args.append(binary)
  task.args.extend(extra_args)
  task.inject_task_lib = True

if len(sys.argv) < 4:
  print "usage: naiad_submit.py <coordinator hostname> <web UI port> " \
      "<task binary> [<args>] [<job name>] [<num workers>]"
  sys.exit(1)

hostname = sys.argv[1]
port = int(sys.argv[2])
naiad_exe = sys.argv[3]

if len(sys.argv) > 4:
  extra_args = shlex.split(sys.argv[4])
else:
  extra_args = []

if len(sys.argv) > 5:
  job_name = sys.argv[5]
else:
  job_name = "naiad_job_at_%d" % (int(time.time()))

if len(sys.argv) > 6:
  num_workers = int(sys.argv[6])
else:
  num_workers = 1

basic_args = []
basic_args.append("/home/srguser/firmament-experiments/helpers/napper/napper_naiad.py")
basic_args.append("caelum-301:2181")
basic_args.append(job_name)

job_desc = job_desc_pb2.JobDescriptor()
job_desc.uuid = "" # UUID will be set automatically on submission
job_desc.name = job_name
# set up root task
job_desc.root_task.uid = 0
job_desc.root_task.name = "%s/0" % (job_name)
job_desc.root_task.state = task_desc_pb2.TaskDescriptor.CREATED
job_desc.root_task.binary = "/usr/bin/python"
job_desc.root_task.args.extend(basic_args)
job_desc.root_task.args.append("0") # root task is worker ID 0
job_desc.root_task.args.append(str(num_workers))
job_desc.root_task.args.append(naiad_exe)
job_desc.root_task.args.extend(extra_args)
job_desc.root_task.inject_task_lib = True

# add workers
for i in range(1, num_workers):
  task = job_desc.root_task.spawned.add()
  add_worker_task(task, naiad_exe, basic_args, job_name, i, num_workers, extra_args)

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
