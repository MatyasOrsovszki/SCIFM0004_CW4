#!/bin/bash

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker Desktop to continue."
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo "Docker daemon is not running. Attempting to start Docker Desktop..."

    # Attempt to start Docker Desktop (for Windows)
    powershell.exe -Command "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'"
    echo "Please wait while Docker Desktop starts up."
    sleep 15  # Wait for Docker to start; adjust as needed

    # Check again if Docker started successfully
    if ! docker info &> /dev/null; then
        echo "Failed to start Docker. Please start Docker Desktop manually and try again."
        exit 1
    fi
fi

# Proceed with Docker-related commands
echo "Docker is running. Proceeding with the script..."

NUM_CONSUMERS=12
HREF=$(cat datahref.txt)
DEBUG=false

# Using keyword arguments for internal variables
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --consumers) NUM_CONSUMERS="$2"; shift ;;
        --href) HREF="$2"; shift ;;
        --debug) DEBUG="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

export NUM_CONSUMERS
export HREF
export DEBUG

envsubst < ./HZZanalysis/docker-compose.template.yml > ./HZZanalysis/docker-compose.yml

docker-compose -f "./HZZanalysis/docker-compose.yml" up --build
