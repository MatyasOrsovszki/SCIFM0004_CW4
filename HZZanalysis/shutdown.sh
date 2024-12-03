#!/bin/bash

# Your processing code here
echo "Processing done, shutting down..."

set -x
# Run docker-compose down to stop and remove containers
docker stop $(docker ps -a -q)

# Exit the script to shut down the container
exit 0
