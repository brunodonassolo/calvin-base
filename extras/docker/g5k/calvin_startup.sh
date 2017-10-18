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


# Naive check runs checks once a minute to see if either of the processes exited.
# This illustrates part of the heavy lifting you need to do if you want to run
# more than one service in a container. The container will exit with an error
# if it detects that either of the processes has exited.
# Otherwise it will loop forever, waking up every 60 seconds
  
while /bin/true; do
  ps aux |grep netdata |grep -q -v grep
  PROCESS_1_STATUS=$?
  ps aux |grep blackbox_exporter |grep -q -v grep
  PROCESS_2_STATUS=$?
  ps aux |grep csruntime |grep -q -v grep
  PROCESS_3_STATUS=$?
  # If the greps above find anything, they will exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 -o $PROCESS_3_STATUS -ne 0 ]; then
    echo "One of the processes has already exited."
    exit -1
  fi
  sleep 60
done
