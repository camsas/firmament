Firmament is a cluster manager and scheduling platform developed CamSaS
(http://camsas.org) at the University of Cambridge Computer Laboratory.

[![Build Status](https://travis-ci.org/ms705/firmament.svg)](https://travis-ci.org/ms705/firmament)

Firmament is currently in alpha stage: it runs your jobs and tasks fairly
reliably, but system components keep changing regularly, and we are actively
developing system features.

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

```console
$ make all
```

fetches dependencies are necessary, and may ask you to install required
packages.

```console
$ make test
```

runs unit tests.

Other targets can be listed by running

```console
$ make help
```

Binaries are in the build/ subdirectory of the project root, and all accept the
`--helpshort` argument to show their command line options.

Start up by running a coordinator:

```console
$ build/engine/coordinator --listen_uri tcp:<host>:<port> --task_lib_dir=$(pwd)/build/engine/
```

Once the coordinator is up and running, you can access its HTTP interface at
http://<host>:8080/ (the port can be customized using `--http_ui_port`
argument).

To submit a toy job, first make the examples target and then use the script in
`scripts/job/job_submit.py`. Note that jobs are submitted to the web UI port,
and NOT the internal listen port!

```console
$ make examples
$ cd scripts/job/
$ make
$ python job_submit.py <host> <webUI port (8080)> <binary>
```

Example for the last line:

```console
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

```console
$ build/engine/coordinator --scheduler flow --flow_scheduling_cost_model 6 --listen_uri tcp:<host>:<port> --task_lib_dir=$(pwd)/build/engine/
```

The `--flow_scheduling_cost_model` option choses the cost model on which the
scheduler's flow network is based: here, we specify a simple load-balacing model
that aims to put the same number of tasks on each machine. Several other cost
models are available and in development.

### Cost models

There are currently seven cost models in the Firmament code base:

| Cost model  | Description                                               | Status   |
| ----------- | --------------------------------------------------------- | -------- |
| TRIVIAL (0) | Fixed costs, tasks always schedule if resources are idle. | Complete |
| RANDOM (1)  | Random costs, for fuzz tests. Not useful in practice!     | Complete |
| SJF (2)     | Shortest job first policy based on avg. past runtimes.    | Complete |
| QUINCY (3)  | Original Quincy cost model, with data locality.           | Broken!  |
| WHARE (4)   | Implementation of Whare-Map's M and MCs policies.         | Complete |
| COCO (5)    | Coordinated co-location model (in development).           | In dev   |
| OCTOPUS (6) | Simple load balancing based on task counts.               | Complete |
| ----------- | --------------------------------------------------------- | -------- |
| VOID (7)    | Bogus cost model used for KB in simple scheduler.         | Complete |
| SIM QUINCY (8) | Quincy cost model with simulated DFS; for simulator only. | Complete |

## Running on multiple machines

To use Firmament across multiple machines, you need to run a `coordinator`
instance on each machine. These coordinators can then be arranged in a tree
hierarchy, in which each coordinator can schedule tasks locally _and_ on its
subordinate childrens' resources.

To run a coordinator as a child of a parent coordinator, pass the `--parent_uri`
flag on launch and set it to the parent coordinator's network location:
```console
$ build/engine/coordinator --listen_uri tcp:<local host>:<local port> --parent_uri tcp:<parent host>:<parent port> --task_lib_dir=$(pwd)/build/engine/
```
The parent coordinator must already be running. Once both coordinators are up,
you will be able to see the child resources on the parent coordinator's web UI.

## Contributing

We always welcome contributions to Firmament. One contribution you can
easily make as a newcomer is to do **code reviews** -- this also helps you
familiarise yourself with the Firmament code base, _en passant_.

We use GerritHub for our code reviews. You can find the Firmament review board
there:

https://review.gerrithub.io/#/q/project:ms705/firmament

In order to do code reviews, you will need an account on GerritHub (you can link
your GitHub account). Once you've created an account, please email us at
firmament-dev@camsas.org to let us know that you're interested in doing reviews,
or comment on an open review.

If you would like to contribute a **pull request**, that's also most welcome!
The easiest way to submit changes for review is to check out Firmament from
GerritHub, or to add GerritHub as a remote. Alternatively, you can submit a pull
request on GitHub and we will import it for review on GerritHub.

### Code style

We follow the [Google C++ style guide](https://google-styleguide.googlecode.com/svn/trunk/cppguide.html)
in the Firmament code base. A subset of the style guide's rules can be verified
using the `make lint` target, which runs the C++ linting script on your
checkout.

## Contact

If you would like to contact us, please send an email to firmament@camsas.org,
or create an issue on GitHub.
