# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""Connection() class unit tests."""

import unittest

# import google.cloud.spanner_dbapi.exceptions as dbapi_exceptions

from google.cloud.spanner_dbapi import Connection, InterfaceError, Warning
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance


class TestConnection(unittest.TestCase):
    instance_name = "instance-name"
    database_name = "database-name"

    def _make_connection(self):
        # we don't need real Client object to test the constructor
        instance = Instance(self.instance_name, client=None)
        database = instance.database(self.database_name)
        return Connection(instance, database)

    def test_ctor(self):
        connection = self._make_connection()

        self.assertIsInstance(connection.instance, Instance)
        self.assertEqual(connection.instance.instance_id, self.instance_name)

        self.assertIsInstance(connection.database, Database)
        self.assertEqual(connection.database.database_id, self.database_name)

        self.assertFalse(connection._is_closed)

    def test_close(self):
        connection = self._make_connection()

        self.assertFalse(connection._is_closed)
        connection.close()
        self.assertTrue(connection._is_closed)

        with self.assertRaises(InterfaceError):
            connection.cursor()

    def test_transaction_management_warnings(self):
        connection = self._make_connection()

        with self.assertRaises(Warning):
            connection.commit()

        with self.assertRaises(Warning):
            connection.rollback()

    def test_connection_close_check_if_open(self):
        connection = self._make_connection()

        connection.cursor()
        self.assertFalse(connection._is_closed)

    def test_is_closed(self):
        connection = self._make_connection()

        self.assertEqual(connection._is_closed, connection.is_closed)
        connection.close()
        self.assertEqual(connection._is_closed, connection.is_closed)

    def test_inside_transaction(self):
        connection = self._make_connection()

        self.assertEqual(
            connection._inside_transaction, connection.inside_transaction,
        )

    def test_transaction_started(self):
        connection = self._make_connection()

        self.assertEqual(
            connection.transaction_started, connection._transaction_started,
        )

    def test_cursor(self):
        from google.cloud.spanner_dbapi.cursor import Cursor

        connection = self._make_connection()
        cursor = connection.cursor()

        self.assertIsInstance(cursor, Cursor)
        self.assertEqual(connection, cursor._connection)

    def test_commit(self):
        connection = self._make_connection()

        with self.assertRaises(Warning):
            connection.commit()

    def test_rollback(self):
        connection = self._make_connection()

        with self.assertRaises(Warning):
            connection.rollback()

    def test_context_success(self):
        connection = self._make_connection()

        with connection as conn:
            conn.cursor()
        self.assertTrue(connection._is_closed)

    def test_context_error(self):
        connection = self._make_connection()

        with self.assertRaises(Exception):
            with connection:
                raise Exception
        self.assertTrue(connection._is_closed)
