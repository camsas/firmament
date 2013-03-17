#!/usr/bin/python

import sys, re

if len(sys.argv) < 2:
  print "usage: dimacs_to_dot.py <input>"
  sys.exit(1)

inputfile = sys.argv[1]

def dot_out(nodes, edges):
  # dot header
  print "digraph G {"
  print "\tgraph [center rankdir=LR]"
  # nodes
  print "\t{ node [shape=box]"
  print "\t",
  for n in nodes:
    print "%d [ label = \"%d: %s\" ];" % (n['nid'], n['nid'], n['desc'])
  print "\t}"
  # edges
  print "\t{ edge [color=\"#ff0000\"]"
  print "\t",
  for e in edges:
    print "%d -> %d [ label = \"%d/%d/%d\" ];" % (e['src'], e['dst'],
                                                  e['cap_lb'], e['cap_ub'],
                                                  e['cost'])
  print "\t}"
  # dot footer
  print "}"

nodes = []
edges = []
node_task_id = "0"
node_res_id = "00000000-0000-0000-0000-000000000000"
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
    edge = { 'src': int(fields[1]),
             'dst': int(fields[2]),
             'cap_lb': int(fields[3]),
             'cap_ub': int(fields[4]),
             'cost': int(fields[5]) }
    edges.append(edge)

dot_out(nodes, edges)
