#! /bin/bash

if [ "$HOSTNAME" == "" ]; then
    HOSTNAME="$(hostname -f)"
fi

while true; do
  luigi RunTasks --module poltergust --hostname="${HOSTNAME}" --path="${PIPELINE_URL}" --scheduler-url="${SCHEDULER_URL}"
  sleep 1
done
