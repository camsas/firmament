#!/usr/bin/python

import sys, re

if len(sys.argv) < 2:
  print "usage: dimacs_to_dot.py <input> <flowinput>"
  sys.exit(1)

inputfile = sys.argv[1]
flowinputfile = sys.argv[2]

def dot_out(nodes, edges):
  # dot header
  print "digraph G {"
  print "\tgraph [rankdir=\"LR\"]"
  # nodes
  print "\t{ node [shape=box]"
  print "\t",
  for n in nodes:
    label = n['desc'] if (n['desc'] in ['SINK', 'CLUSTER_AGG']) else \
        (n['desc'][:3] + "..." + n['desc'][-3:])
    print "%d [ label = \"%d: %s\" ];" % (n['nid'], n['nid'], label)
  print "\t}"
  # edges WITH flow
  print "\t{ edge [color=\"#ff0000\"]"
  print "\t",
  for e in filter(lambda x:x['flow']>0, flatten_edges(edges)):
    print "%d -> %d [ label = \"%d/%d/%d c: %d\" ];" \
        % (e['src'], e['dst'], e['flow'], e['cap_lb'], e['cap_ub'], e['cost'])
  print "\t}"
  # edges WITHOUT flow
  print "\t{ edge [color=\"#cccccc\"]"
  print "\t",
  for e in filter(lambda x:x['flow']==0, flatten_edges(edges)):
    print "%d -> %d [ label = \"%d/%d/%d c: %d\" ];" \
        % (e['src'], e['dst'], e['flow'], e['cap_lb'], e['cap_ub'], e['cost'])
  print "\t}"
  # dot footer
  print "}"


def flatten_edges(edges):
  elist = []
  for src, v in edges.items():
    for dst, d in v.items():
      entry = { 'src': src, 'dst': dst, 'cap_lb': d['cap_lb'],
                'cap_ub': d['cap_ub'], 'cost': d['cost'], 'flow': d['flow'] }
      elist.append(entry)
  return elist



nodes = []
edges = {}
node_task_id = "0"
node_res_id = "00000000-0000-0000-0000-000000000000"

# read and process graph specification
for line in open(inputfile).readlines():
  fields = [x.strip() for x in line.split(" ")]
  if fields[0] == 'c':
    # comment, check if this contains node descriptions
    if len(fields) > 2 and fields[1] == "nd":
      node_task_id = fields[2]
      node_res_id = fields[3]
    continue
  if fields[0] == 'p':
    # problem descr
    continue
  if fields[0] == 'n':
    # node
    node_desc = ""
    if node_task_id != "0" and \
        node_res_id == "00000000-0000-0000-0000-000000000000":
      node_desc = node_task_id
    elif node_task_id == "0" and \
        node_res_id != "00000000-0000-0000-0000-000000000000":
      node_desc = node_res_id
    else:
      # ignore
      pass
    node = { 'nid': int(fields[1]),
             'supp': int(fields[2]),
             'desc': node_desc }
    nodes.append(node)
    node_task_id = "0"
    node_res_id = "00000000-0000-0000-0000-000000000000"
  if fields[0] == 'a':
    # arc
    src = int(fields[1])
    dst = int(fields[2])
    if not src in edges:
      edges[src] = {}
    edges[src][dst] = { 'cap_lb': int(fields[3]),
                        'cap_ub': int(fields[4]),
                        'cost': int(fields[5]),
                        'flow': 0.0 }

# read and process flow solution, if existent
try:
  for line in open(flowinputfile).readlines():
    fields = re.findall(r"[\w']+", line)
    if fields[0] == 'c':
      # skip comments
      continue
    if fields[0] == 's':
      # skip total ("sum")
      continue
    if fields[0] == 'f':
      src = int(fields[1])
      dst = int(fields[2])
      flow = int(fields[3])
      edges[src][dst]['flow'] = flow
except Exception as e:
  print >> sys.stderr, e
  print >> sys.stderr, \
      "WARNING: failed to read flow file, resulting graph will have all " \
      "flows set to zero!";

dot_out(nodes, edges)
