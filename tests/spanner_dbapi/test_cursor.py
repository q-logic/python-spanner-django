# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""Cursor() class unit tests."""

import unittest
from unittest import mock


class TestCursor(unittest.TestCase):
    def _make_cursor(self):
        from google.cloud.spanner_dbapi import connect

        with mock.patch(
            "google.cloud.spanner_v1.instance.Instance.exists",
            return_value=True,
        ):
            with mock.patch(
                "google.cloud.spanner_v1.database.Database.exists",
                return_value=True,
            ):
                connection = connect("test-instance", "test-database")

        return connection.cursor()

    def test_close(self):
        from google.cloud.spanner_dbapi import connect, InterfaceError

        with mock.patch(
            "google.cloud.spanner_v1.instance.Instance.exists",
            return_value=True,
        ):
            with mock.patch(
                "google.cloud.spanner_v1.database.Database.exists",
                return_value=True,
            ):
                connection = connect("test-instance", "test-database")

        cursor = connection.cursor()
        self.assertFalse(cursor.is_closed)

        cursor.close()

        self.assertTrue(cursor.is_closed)
        with self.assertRaises(InterfaceError):
            cursor.execute("SELECT * FROM database")

    def test_connection(self):
        cursor = self._make_cursor()

        self.assertEqual(cursor.connection, cursor._connection)

        cursor._connection = "changed-connection"
        self.assertEqual(cursor.connection, cursor._connection)

    def test_description_if_not_stream(self):
        cursor = self._make_cursor()
        cursor._stream = None

        self.assertIsNone(cursor.description)

    def test_rowcount(self):
        cursor = self._make_cursor()

        self.assertEqual(cursor.rowcount, cursor._row_count)

        cursor._row_count = 52
        self.assertEqual(cursor.rowcount, cursor._row_count)

    def test_lastrowid(self):
        cursor = self._make_cursor()

        self.assertIsNone(cursor.lastrowid)

    def test_callproc(self):
        from google.cloud.spanner_dbapi import InterfaceError

        cursor = self._make_cursor()

        self.assertIsNone(cursor.callproc("procname"))

        cursor.close()
        with self.assertRaises(InterfaceError):
            cursor.callproc("procname")

    def test_nextset(self):
        from google.cloud.spanner_dbapi import InterfaceError

        cursor = self._make_cursor()
        self.assertIsNone(cursor.nextset())
        cursor.close()
        with self.assertRaises(InterfaceError):
            cursor.nextset()

    def test_setinputsizes(self):
        from google.cloud.spanner_dbapi import InterfaceError

        cursor = self._make_cursor()
        self.assertIsNone(cursor.setinputsizes("sizes"))
        cursor.close()
        with self.assertRaises(InterfaceError):
            cursor.setinputsizes("sizes")

    def test_setoutputsize(self):
        from google.cloud.spanner_dbapi import InterfaceError

        cursor = self._make_cursor()
        self.assertIsNone(cursor.setoutputsize("size"))
        cursor.close()
        with self.assertRaises(InterfaceError):
            cursor.setoutputsize("size")

    def test_execute_without_connection(self):
        from google.cloud.spanner_dbapi import ProgrammingError

        cursor = self._make_cursor()
        cursor._connection = None

        with self.assertRaises(ProgrammingError):
            cursor.execute('SELECT * FROM table1 WHERE "col1" = @a1')

    def test_executemany_without_connection(self):
        from google.cloud.spanner_dbapi import ProgrammingError

        cursor = self._make_cursor()
        cursor._connection = None

        with self.assertRaises(ProgrammingError):
            cursor.executemany(
                """SELECT * FROM table1 WHERE "col1" = @a1""", ()
            )

    def test_executemany_on_closed_cursor(self):
        from google.cloud.spanner_dbapi import InterfaceError

        cursor = self._make_cursor()
        cursor.close()

        with self.assertRaises(InterfaceError):
            cursor.executemany(
                """SELECT * FROM table1 WHERE "col1" = @a1""", ()
            )

    def test_executemany(self):
        operation = """SELECT * FROM table1 WHERE "col1" = @a1"""
        params_seq = ((1,), (2,))

        cursor = self._make_cursor()
        with mock.patch(
            "google.cloud.spanner_dbapi.cursor.Cursor.execute"
        ) as execute_mock:
            cursor.executemany(operation, params_seq)

        execute_mock.assert_has_calls(
            (mock.call(operation, (1,)), mock.call(operation, (2,)))
        )

    def test_context_success(self):
        cursor = self._make_cursor()

        with cursor as c:
            c.nextset()
        self.assertTrue(cursor._is_closed)

    def test_context_error(self):
        cursor = self._make_cursor()

        with self.assertRaises(Exception):
            with cursor:
                raise Exception
        self.assertTrue(cursor._is_closed)


class TestColumns(unittest.TestCase):
    def test_ctor(self):
        from google.cloud.spanner_dbapi.cursor import ColumnInfo

        name = "col-name"
        type_code = 8
        display_size = 5
        internal_size = 10
        precision = 3
        scale = None
        null_ok = False

        cols = ColumnInfo(
            name,
            type_code,
            display_size,
            internal_size,
            precision,
            scale,
            null_ok,
        )

        self.assertEqual(cols.name, name)
        self.assertEqual(cols.type_code, type_code)
        self.assertEqual(cols.display_size, display_size)
        self.assertEqual(cols.internal_size, internal_size)
        self.assertEqual(cols.precision, precision)
        self.assertEqual(cols.scale, scale)
        self.assertEqual(cols.null_ok, null_ok)
        self.assertEqual(
            cols.fields,
            (
                name,
                type_code,
                display_size,
                internal_size,
                precision,
                scale,
                null_ok,
            ),
        )

    def test___get_item__(self):
        from google.cloud.spanner_dbapi.cursor import ColumnInfo

        fields = ("col-name", 8, 5, 10, 3, None, False)
        cols = ColumnInfo(*fields)

        for i in range(0, 7):
            self.assertEqual(cols[i], fields[i])

    def test___str__(self):
        from google.cloud.spanner_dbapi.cursor import ColumnInfo

        cols = ColumnInfo("col-name", 8, None, 10, 3, None, False)

        self.assertEqual(
            str(cols),
            "ColumnInfo(name='col-name', type_code=8, internal_size=10, precision='3')",
        )
