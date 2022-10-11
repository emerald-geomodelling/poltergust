#!/bin/bash

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail

readonly PIPELINE_URL="$(/usr/share/google/get_metadata_value attributes/pipeline-url)"
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly MASTER="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

cd /

function install_deps() {
  apt update
  apt install -y pkgconf python3-dev python3-pip python3-virtualenv libxml2-dev build-essential screen mg libffi-dev
  /usr/bin/python3 -m virtualenv --python=python3 /pythonenv
  # See https://stackoverflow.com/questions/42997258/virtualenv-activate-script-wont-run-in-bash-script-with-set-euo
  set +u
  source /pythonenv/bin/activate
  set -u
  pip install --upgrade pip
 
  (
    git clone https://github.com/emerald-geomodelling/poltergust.git
    cd poltergust
    pip install .
  )
}

function install_systemd_service() {
  echo "Installing systemd service..."

  if [[ "${ROLE}" == "Master" ]]; then
    cat <<EOF > /luigi_launcher
#!/bin/bash

source /pythonenv/bin/activate

luigid --address 0.0.0.0
EOF
  else
    cat <<EOF > /luigi_launcher
#!/bin/bash

source /pythonenv/bin/activate

while true; do
  luigi RunTasks --module poltergust --hostname="$(hostname -f)" --path="${PIPELINE_URL}" --scheduler-url="http://${MASTER}:8082/"
  sleep 1
done
EOF
  fi
  
  chmod 750 /luigi_launcher

  cat <<EOF > /usr/lib/systemd/system/luigi.service
[Unit]
Description=Luigi Cluster Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec /luigi_launcher'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r /usr/lib/systemd/system/luigi.service

  systemctl daemon-reload
  systemctl enable luigi
}

function main() {
  install_deps
  install_systemd_service
  systemctl start luigi
  echo "Installation & startup done"
}

main
