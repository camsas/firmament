#!/usr/bin/env python
import sys, os
import subprocess

class bcolors:
    PURPLE = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW= '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'

ignore_warnings = ["build/header_guard", "build/include_order",
                   "whitespace/comments"]

source_dir = sys.argv[1]
filter_string = "-" + ",-".join(ignore_warnings)
source_files = []
num_good_files = 0
num_bad_files = 0

try:
  for root, subfolders, files in os.walk(source_dir):
    for filename in files:
      if not (filename.endswith(".pb.cc") or filename.endswith(".pb.h")) and \
          filename.endswith(".h") or filename.endswith(".cc"):
        f = os.path.join(root, filename)
        # XXX(malte): Hack to ignore simulator for linting purposes (for now)
        if "/sim/" in f:
          continue
        print "Adding source file %s to list..." % (f)
        source_files.append(f)

  for source_file in source_files:
    print "******************************************************************" \
        "*************"
    print "* LINTING: %s" % source_file
    print "******************************************************************" \
        "*************"
    # lint the next file
    retcode = subprocess.call(["python", "scripts/cpplint.py",
                               "--filter=%s" % (filter_string), source_file],
                              stdout=sys.stdout, stderr=sys.stderr)

    if retcode == 0:
      print bcolors.GREEN + "GOOD :-)\n" + bcolors.ENDC
      num_good_files = num_good_files + 1
    else:
      print bcolors.RED + "FAILED :-( See above for details.\n" + bcolors.ENDC
      num_bad_files = num_bad_files + 1

except Exception as e:
  print "Failed to run linter. Check the following: \n" \
        "1) Does the cpplist.py script in $ROOT_DIR/scripts/ exist?\n" \
        "2) Is it executable?\n"
  print e

print "Total files: %d, of which\n-- " % (num_good_files + num_bad_files) + \
    bcolors.GREEN + "%d" % (num_good_files) + bcolors.ENDC + " good (no " \
    "warnings)\n-- " + bcolors.RED + "%d" % (num_bad_files) + bcolors.ENDC + \
    " bad (warnings)"
