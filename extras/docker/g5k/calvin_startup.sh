#!/bin/bash

# Start the first process
netdata
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start netdata: $status"
  exit $status
fi

# Start the second process
blackbox_exporter --config.file="/root/go/src/github.com/prometheus/blackbox_exporter/blackbox.yml"&
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start blackbox_exporter: $status"
  exit $status
fi

ip_addr=`ifconfig eth0 | grep 'inet addr' | cut -d: -f2 | awk '{print $1}'`
csruntime -n $ip_addr --loglevel=INFO &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start csruntime: $status"
  exit $status
fi
