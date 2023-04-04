#! /bin/bash

while true; do
  luigi RunTasks --module poltergust --hostname="$(hostname -f)" --path="${PIPELINE_URL}" --scheduler-url="${SCHEDULER_URL}"
  sleep 1
done
