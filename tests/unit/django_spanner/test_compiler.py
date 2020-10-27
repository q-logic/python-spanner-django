# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""Django-Spanner SQLCompiler class unit tests."""

import unittest
from unittest import mock


class TestSQLCompiler(unittest.TestCase):
    def test_get_combinator_sql(self):
        from django.core.exceptions import EmptyResultSet
        from django.db.utils import DatabaseError
        from django_spanner.compiler import SQLCompiler

        def _empty_result_set():
            from django.core.exceptions import EmptyResultSet

            raise EmptyResultSet

        query = mock.MagicMock()
        query.values_select = False
        connection = mock.MagicMock()
        using = 'using'

        compiler = SQLCompiler(query, connection, using)
        self.assertIsInstance(compiler, SQLCompiler)

        mock_cquery = mock.MagicMock()
        mock_cquery.get_compiler = mock_compiler = mock.MagicMock()
        part_sql = 'part_sql'
        part_args = 'part_args'
        mock_compiler.return_value.as_sql.return_value = (part_sql, part_args)
        mock_cquery.is_empty = lambda: False

        compiler.query = mock_query = mock.MagicMock()
        mock_query.combined_queries = [mock_cquery]

        combinator = 'union'
        all_ = True

        res, params = compiler.get_combinator_sql(combinator, all_)
        self.assertEqual(res, ['({})'.format(part_sql)])
        self.assertEqual(params, [c for c in part_args])

        compiler.connection.features.supports_slicing_ordering_in_compound = False
        q = compiler.query.combined_queries[0]
        q.low_mark = q.high_mark = True
        with self.assertRaises(DatabaseError):
            compiler.get_combinator_sql(combinator, all_)
        q.low_mark = q.high_mark = False
        mock_compiler.return_value.get_order_by.return_value = True
        with self.assertRaises(DatabaseError):
            compiler.get_combinator_sql(combinator, all_)

        compiler.connection.features.supports_slicing_ordering_in_compound = True
        mock_compiler.return_value.query.values_select = False
        compiler.query.values_select = [0]
        mock_compiler.return_value.query.set_values = mock_set_values = mock.MagicMock()
        compiler.get_combinator_sql(combinator, all_)
        mock_set_values.assert_called_once_with((0,))

        compiler.query.combined_queries = []
        with self.assertRaises(EmptyResultSet):
            compiler.get_combinator_sql(combinator, all_)

        compiler.connection.features.supports_slicing_ordering_in_compound = False
        compiler.query.combined_queries = [mock_cquery, mock_cquery]
        mock_cquery.return_value.get_compiler.return_value = [0, 0]
        mock_compiler.return_value.get_order_by.return_value = False
        res, params = compiler.get_combinator_sql(combinator, all_)
        self.assertEqual(res[0].split()[0], '({})'.format(part_sql))
        self.assertEqual(params[:9], [c for c in part_args])

        compiler.connection.features.supports_parentheses_in_compound = False
        res, params = compiler.get_combinator_sql(combinator, all_)
        self.assertEqual(res[0].split()[0], 'SELECT')
        self.assertEqual(params[:9], [c for c in part_args])

        mock_compiler.return_value.query.combinator = False
        res, params = compiler.get_combinator_sql(combinator, all_)
        self.assertEqual(res[0].split()[0], part_sql)
        self.assertEqual(params[:9], [c for c in part_args])

        mock_compiler.return_value.as_sql = _empty_result_set
        with self.assertRaises(EmptyResultSet):
            compiler.get_combinator_sql(combinator, all_)
        with self.assertRaises(EmptyResultSet):
            compiler.get_combinator_sql(None, all_)
