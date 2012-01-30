#!/usr/bin/env python
import sys, os
import subprocess

build_dir = sys.argv[1]

try:
  for line in open(build_dir + "/tests/all_tests.txt", "r").readlines():
    print "******************************************************************" \
        "*************"
    print "* RUNNING: %s" % line.strip()
    print "******************************************************************" \
        "*************"
    subprocess.call([line, "--logtostderr"], shell=True)
except Exception as e:
  print "Failed to run all tests. Does all_tests.txt in $BUILD_DIR/tests/" \
        "exist?"
  print e
