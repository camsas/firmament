from base import job_desc_pb2
from base import task_desc_pb2
from base import reference_desc_pb2
from google.protobuf import text_format
import httplib, urllib, re, sys, random

if len(sys.argv) < 4:
  print "usage: job_submit.py <coordinator hostname> <web UI port> " \
      "<task binary>"
  sys.exit(1)

hostname = sys.argv[1]
port = int(sys.argv[2])

job_desc = job_desc_pb2.JobDescriptor()

job_desc.uuid = "" # UUID will be set automatically on submission
job_desc.name = "testjob"
job_desc.root_task.uid = 0
job_desc.root_task.name = "root_task"
job_desc.root_task.state = task_desc_pb2.TaskDescriptor.CREATED
job_desc.root_task.binary = sys.argv[3]
output_id = random.randint(0, 10000000)
job_desc.output_ids.append(output_id)
#job_desc.root_task.binary = "/bin/echo"
#job_desc.root_task.args.append("Hello World!")
final_output_desc = job_desc.root_task.outputs.add()
final_output_desc.id = output_id
final_output_desc.scope = reference_desc_pb2.ReferenceDescriptor.PUBLIC
final_output_desc.type = reference_desc_pb2.ReferenceDescriptor.FUTURE
final_output_desc.non_deterministic = False

#params = urllib.urlencode({'test': text_format.MessageToString(job_desc)})
params = 'test=%s' % text_format.MessageToString(job_desc)
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
