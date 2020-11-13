# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

import uuid

from google.api_core.exceptions import DeadlineExceeded
from google.cloud import spanner
import pytest

from . import autocommit


def unique_instance_id():
    """Creates a unique id for the database."""
    return "test-instance-{}".format(uuid.uuid4().hex[:10])


def unique_database_id():
    """Creates a unique id for the database."""
    return "test-db-{}".format(uuid.uuid4().hex[:10])


INSTANCE_ID = unique_instance_id()
DATABASE_ID = unique_database_id()


@pytest.fixture(scope="module")
def spanner_instance():
    spanner_client = spanner.Client()
    config_name = "{}/instanceConfigs/regional-us-central1".format(
        spanner_client.project_name
    )
    instance = spanner_client.instance(INSTANCE_ID, config_name)
    op = instance.create()
    op.result(120)  # block until completion
    yield instance
    instance.delete()


@pytest.fixture(scope="module")
def database(spanner_instance):
    """Creates a temporary database that is removed after testing."""
    db = spanner_instance.database(DATABASE_ID)
    db.create()
    yield db
    db.drop()


def test_enable_autocommit_mode(capsys, database):
    autocommit.enable_autocommit_mode(INSTANCE_ID, DATABASE_ID)
    out, _ = capsys.readouterr()
    assert "Autocommit mode is enabled." in out
    assert "SingerId: 13, AlbumId: Russell, AlbumTitle: Morales" in out