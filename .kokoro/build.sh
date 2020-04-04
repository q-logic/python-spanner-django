#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

cd github/python-spanner-django

# Disable buffering, so that the logs stream through.
export PYTHONUNBUFFERED=1

# Debug: show build environment
env | grep KOKORO

# Setup service account credentials.
export GOOGLE_APPLICATION_CREDENTIALS=${KOKORO_GFILE_DIR}/service-account.json

# Setup project id.
export PROJECT_ID=$(cat "${KOKORO_GFILE_DIR}/project-id.json")

# Install tox
python3.6 -m pip install --upgrade --quiet tox flake8 isort
python3.6 -m tox --version

python3.6 -m tox
python3.6 -m isort --recursive --check-only --diff
python3.6 -m flake8

# Export essential environment variables for Django tests.
export RUNNING_SPANNER_BACKEND_TESTS=1
export DJANGO_WORKER_COUNT=$(ls .kokoro/presubmit/worker* | wc -l)

pip3 install .
# Create a unique DJANGO_TESTS_DIR per worker to avoid
# any clashes with configured tests by other workers.
export DJANGO_TESTS_DIR="django_tests_$DJANGO_WORKER_INDEX"
mkdir -p $DJANGO_TESTS_DIR && git clone --depth 1 --single-branch --branch spanner-2.2.x https://github.com/timgraham/django.git $DJANGO_TESTS_DIR/django

# Install dependencies for Django tests.
sudo apt-get update
apt-get install -y libffi-dev libjpeg-dev zlib1g-dev libmemcached-dev
cd $DJANGO_TESTS_DIR/django && pip3 install -e . && pip3 install -r tests/requirements/py3.txt; cd ../../

# Install and start the Spanner emulator
VERSION=0.7.3
wget https://storage.googleapis.com/cloud-spanner-emulator/releases/${VERSION}/cloud-spanner-emulator_linux_amd64-${VERSION}.tar.gz
tar zxvf cloud-spanner-emulator_linux_amd64-${VERSION}.tar.gz
chmod +x emulator_main
export USE_SPANNER_EMULATOR=1

./bin/parallelize_tests_linux
