#! /bin/bash

NAME="$1"
echo $NAME
[ -n "$NAME" ] || NAME=my-luigi-cluster
NRWORKERS="$2"
[ -n "$NRWORKERS" ] || NRWORKERS=3

gsutil mb "gs://${NAME}"
gsutil cp dataproc-initactions.sh "gs://${NAME}/initactions.sh"

gcloud dataproc clusters create "$NAME" \
  --master-machine-type n1-standard-4 \
  --worker-machine-type n1-standard-4 \
  --initialization-actions "gs://${NAME}/initactions.sh" \
  --num-workers=$NRWORKERS \
  --enable-component-gateway \
  --master-boot-disk-size=40G \
  --worker-boot-disk-size=40G \
  --metadata dask-runtime=standalone \
  --metadata dask-worker-on-master=false \
  --metadata pipeline-url=gs://${NAME}/pipeline \
  --image-version=2.0-ubuntu18

gcloud compute ssh \
  --zone "europe-north1-a" \
  "${NAME}-m" \
  -- \
  -L 8082:localhost:8082
