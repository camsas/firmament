#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Make the docker image and tag it as "firmament"
docker build -t firmament ${DIR}/docker --network=host
