#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Make the docker image and tag it as "firmament"
docker run --expose=8080 --workdir=/firmament firmament build/engine/coordinator --flagfile=/firmament/default.conf
