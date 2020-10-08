# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""DB-API driver Connection implementation for Google Cloud Spanner.

   See
   https://www.python.org/dev/peps/pep-0249/#connection-objects
"""

from collections import namedtuple
from weakref import WeakSet

from .cursor import Cursor
from .exceptions import InterfaceError, Warning
from .enums import AutocommitDMLModes, TransactionModes


ColumnDetails = namedtuple("column_details", ["null_ok", "spanner_type"])


class Connection(object):
    """Representation of a connection to a Cloud Spanner database.

    You most likely don't need to instantiate `Connection` objects
    directly, use the `connect` module function instead.

    :type instance: :class:`~google.cloud.spanner_v1.instance.Instance`
    :param instance: Cloud Spanner instance to connect to.

    :type database: :class:`~google.cloud.spanner_v1.database.Database`
    :param database: Cloud Spanner database to connect to.
    """

    def __init__(self, instance, database):
        self.instance = instance
        self.database = database
        self.autocommit = True
        self.read_only = False
        self.transaction_mode = (
            TransactionModes.READ_ONLY
            if self.read_only
            else TransactionModes.READ_WRITE
        )
        self.autocommit_dml_mode = AutocommitDMLModes.TRANSACTIONAL
        self.timeout_secs = 0
        self.read_timestamp = None
        self.commit_timestamp = None
        self._is_closed = False
        self._inside_transaction = not self.autocommit
        self._transaction_started = False
        self._cursors = WeakSet()
        self.read_only_staleness = {}

    @property
    def is_closed(self):
        return self._is_closed

    @property
    def inside_transaction(self):
        return self._inside_transaction

    @property
    def transaction_started(self):
        return self._transaction_started

    def cursor(self):
        """Returns cursor for current connection"""
        if self._is_closed:
            raise InterfaceError("connection is already closed")

        return Cursor(self)

    def close(self):
        """Close this connection.

        The connection will be unusable from this point forward.
        """
        self._is_closed = True

    def commit(self):
        """Commit all the pending transactions."""
        raise Warning(
            "Cloud Spanner DB API always works in `autocommit` mode."
            "See https://github.com/googleapis/python-spanner-django#transaction-management-isnt-supported"
        )

    def rollback(self):
        """Rollback all the pending transactions."""
        raise Warning(
            "Cloud Spanner DB API always works in `autocommit` mode."
            "See https://github.com/googleapis/python-spanner-django#transaction-management-isnt-supported"
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
