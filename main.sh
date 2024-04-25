#! /bin/bash

if [ "$HOSTNAME" == "" ]; then
    HOSTNAME="$(hostname -f)"
fi

luigi KillTask --module poltergust.tasks --scheduler-url="${SCHEDULER_URL}" &>/dev/null &

while true; do
  luigi RunTasks --module poltergust.tasks --hostname="${HOSTNAME}" --path="${PIPELINE_URL}" --scheduler-url="${SCHEDULER_URL}"
  sleep 1
done
