Firmament is a cluster manager and scheduling platform developed CamSaS
(http://camsas.org) at the University of Cambridge Computer Laboratory.

It is currently in early alpha stage, with much of the high-level functionality
still missing, and interfaces frequently changing.

[![Build Status](https://travis-ci.org/ms705/firmament.svg)](https://travis-ci.org/ms705/firmament)

## System requirements

Firmament is currently known to work on Ubuntu LTS releases 12.04 (precise) and
14.04 (trusty). With caveats (see below), it works on 13.04 (raring) and 13.10
(saucy); it does NOT work on other versions prior to 12.10 (quantal) as they
cannot build libpion, which is now included as a self-built dependency in order
to ease transition to libpion v5 and for compatibility with Arch Linux.

Other configurations are untested - YMMV. Recent Debian versions typically work
with a bit of fiddling of the build configuration files in the `include`
directory.

Reasons for known breakage:
 * Ubuntu 13.04 - segfault failures when using Boost 1.53 packages; use 1.49
                  (default).
 * Ubuntu 13.10 - clang/LLVM include paths need to be fixed.
                  /usr/{lib,include}/clang/3.2 should be symlinked to
                  /usr/lib/llvm-3.2/lib/clang/3.2.

## Getting started

After cloning the repository,

```
$ make all
```

fetches dependencies are necessary, and may ask you to install required
packages.

```
$ make test
```

runs unit tests.

Other targets can be listed by running

```
$ make help
```

Binaries are in the build/ subdirectory of the project root, and all accept the
`--helpshort` argument to show their command line options.

Start up by running a coordinator:

```
$ build/engine/coordinator --listen_uri tcp://<host>:<port> --task_lib_path=$(PWD)/build/engine/
```

Once the coordinator is up and running, you can access its HTTP interface at
http://<host>:8080/ (the port can be customized using `--http_ui_port`
argument).

To submit a toy job, first make the examples target and then use the script in
`scripts/job/job_submit.py`. Note that jobs are submitted to the web UI port,
and NOT the internal listen port!

```
$ make examples
$ cd scripts/job/
$ make
$ python job_submit.py <host> <webUI port (8080)> <binary>
```

Example for the last line:

```
$ python job_submit.py localhost 8080 /bin/sleep 60
```

(Note that you may need to run `make` in the script directory since the script
depends on some protocol buffer data structures that need to be compiled. If
you have run `make all`, all script dependencies should automatically have been
built, though.)

If this all works, you should see the new job on the web UI.

## Using the flow scheduler

By default, Firmament starts up with a simple queue-based scheduler. If you want
to instead use our new scheduler based on flow network optimization, pass
the `--scheduler flow` flag to the coordinator on startup:

```
$ build/engine/coordinator --scheduler flow --flow_scheduling_cost_model 6 --listen_uri tcp://<host>:<port> --task_lib_path=$(PWD)/build/engine/
```

The `--flow_scheduling_cost_model`` option choses the cost model on which the
scheduler's flow network is based: here, we specify a simple load-balacing model
that aims to put the same number of tasks on each machine. Several other cost
models are available and in development.

## Contributing

We always welcome contributions to Firmament. We use GerritHub for
code review; you can find the Firmament review board there:

https://review.gerrithub.io/#/q/project:ms705/firmament

The easiest way to submit changes for review is to check our
Firmament from GerritHub, or to add GerritHub as a remote.
Alternatively, you can submit a pull request on GitHub and it will
appear for review on GerritHub.
