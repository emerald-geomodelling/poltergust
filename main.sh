#! /bin/bash

if [ "$HOSTNAME" == "" ]; then
    HOSTNAME="$(hostname -f)"
fi

#PIPELINE_URL="gs://experimental-pipeline-inversion-ali-v1/pipeline"
#SCHEDULER_URL="http://localhost:8082"

luigi KillTask --module poltergust --scheduler-url="${SCHEDULER_URL}" & 
##>/dev/null &

while true; do
  luigi RunTasks --module poltergust --hostname="${HOSTNAME}" --path="${PIPELINE_URL}" --scheduler-url="${SCHEDULER_URL}"
  sleep 1
done
