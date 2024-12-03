#!/bin/bash

NUM_CONSUMERS=${1:-1}
HREF=$(cat datahref.txt)
HREF=${2:-$HREF}

export NUM_CONSUMERS
export HREF

envsubst < ./HZZanalysis/docker-compose.template.yml > ./HZZanalysis/docker-compose.yml

docker-compose -f "./HZZanalysis/docker-compose.yml" up --build