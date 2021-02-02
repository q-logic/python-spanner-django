# Copyright 2020 Google LLC.

# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import math
import os
import random
import time

from google.cloud.spanner_v1 import Client


def create_test_instance():
    project = os.getenv(
        "GOOGLE_CLOUD_PROJECT",
        os.getenv("PROJECT_ID", "emulator-test-project"),
    )

    client = Client(project=project)

    config = f"{client.project_name}/instanceConfigs/regional-us-central1"

    name = "spanner-django-test-{}".format(str(int(time.time())))

    instance = client.instance("django-backend-tests")
    instance.delete()

    instance = client.instance("spanner-django-test-1612252824")
    instance.delete()

    instance = client.instance("libc-django-test")
    instance.delete()

    instance = client.instance(name, config)
    created_op = instance.create()
    created_op.result(30)  # block until completion
    return name, instance


worker_index = int(os.getenv("DJANGO_WORKER_INDEX", 0))
worker_count = int(os.getenv("DJANGO_WORKER_COUNT", 1))

if worker_index > worker_count:
    print(
        "worker_index (wi) > worker_count (wc)".format(
            wi=worker_index, wc=worker_count,
        )
    )
    exit()

with open("django_test_apps.txt", "r") as file:
    all_apps = file.read().split("\n")

apps_per_worker = math.ceil(len(all_apps) / worker_count)

start_index = min(worker_index * apps_per_worker, len(all_apps))
end_index = min(start_index + apps_per_worker, len(all_apps))

test_apps = all_apps[start_index:end_index]
print("test apps: ", test_apps)

if not test_apps:
    exit()

delay = random.randint(10, 60)
print("creating instance with delay: {} seconds".format(delay))
time.sleep(delay)

instance_name, instance = create_test_instance()

print(
    """DJANGO_TEST_APPS="{apps}" SPANNER_TEST_INSTANCE={instance} django_test_suite.sh""".format(
        apps=",".join(test_apps), instance=instance_name
    )
)

os.system(
    """DJANGO_TEST_APPS="{apps}" SPANNER_TEST_INSTANCE={instance} django_test_suite.sh""".format(
        apps=",".join(test_apps), instance=instance_name
    )
)

instance.delete()