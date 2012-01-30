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
    # run the next unit test
    subprocess.check_call([line.strip(), "--logtostderr"],
                          stdout=sys.stdout, stderr=sys.stderr)

  # we actually made it to the end
  print "====================================================================" \
      "===========\n" \
      "ALL UNIT TESTS PASSING :-)"
except subprocess.CalledProcessError as e:
  print "====================================================================" \
      "===========\n" \
      "UNIT TEST(s) FAILED :-( See above for details. \n" \
      "====================================================================" \
      "===========\n"
except Exception as e:
  print "Failed to run all tests. Check the following: \n" \
        "1) Does all_tests.txt in $BUILD_DIR/tests/ exist?" \
        "2) Do all test binaries listed in all_tests.txt exist and are they " \
        "executable?"
  print e
