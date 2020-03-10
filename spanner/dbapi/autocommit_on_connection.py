# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from .autocommit_on_cursor import Cursor
from .exceptions import Error
from .utils import get_table_column_schema as get_table_column_schema_impl


class Connection(object):
    def __init__(self, db_handle, *args, **kwargs):
        self.__dbhandle = db_handle
        self.__closed = False
        self.__ddl_statements = []

    def __raise_if_already_closed(self):
        """
        Raises an exception if attempting to use an already closed connection.
        """
        if self.__closed:
            raise Error('attempting to use an already closed connection')

    def close(self):
        self.rollback()
        self.__dbhandle = None
        self.__closed = True

    def __enter__(self):
        return self

    def __exit__(self, etype, value, traceback):
        self.commit()
        self.close()

    def commit(self):
        self.__raise_if_already_closed()

        self.run_prior_DDL_statements()

    def rollback(self):
        self.__raise_if_already_closed()

        # TODO: to be added.

    def cursor(self):
        self.__raise_if_already_closed()

        return Cursor(self)

    def __handle_update_ddl(self, ddl_statements):
        """
        Runs the list of Data Definition Language (DDL) statements on the underlying
        database. Note that each DDL statement MUST NOT contain a semicolon.
        Args:
            ddl_statements: a list of DDL statements, each without a semicolon.
        Returns:
            google.api_core.operation.Operation.result()
        """
        self.__raise_if_already_closed()

        # Synchronously wait on the operation's completion.
        return self.__dbhandle.update_ddl(ddl_statements).result()

    def read_snapshot(self):
        self.__raise_if_already_closed()

        return self.__dbhandle.snapshot()

    def in_transaction(self, fn, *args, **kwargs):
        self.__raise_if_already_closed()

        return self.__dbhandle.run_in_transaction(fn, *args, **kwargs)

    def append_ddl_statement(self, ddl_statement):
        self.__raise_if_already_closed()

        self.__ddl_statements.append(ddl_statement)

    def run_prior_DDL_statements(self):
        self.__raise_if_already_closed()

        if not self.__ddl_statements:
            return

        ddl_statements = self.__ddl_statements
        self.__ddl_statements = []
        return self.__handle_update_ddl(ddl_statements)

    def list_tables(self):
        # We CANNOT list tables with
        #   SELECT
        #     t.table_name
        #   FROM
        #     information_schema.tables AS t
        #   WHERE
        #     t.table_catalog = '' and t.table_schema = ''
        # with a transaction otherwise we get back:
        #   400 Unsupported concurrency mode in query using INFORMATION_SCHEMA.
        # hence this specialized method.
        self.run_prior_DDL_statements()

        with self.__dbhandle.snapshot() as snapshot:
            res = snapshot.execute_sql("""
             SELECT
              t.table_name
            FROM
              information_schema.tables AS t
            WHERE
              t.table_catalog = '' and t.table_schema = ''
            """)
            return list(res)

    def get_table_column_schema(self, table_name):
        return get_table_column_schema_impl(self.__dbhandle, table_name)