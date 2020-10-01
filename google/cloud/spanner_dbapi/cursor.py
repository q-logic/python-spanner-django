# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""Database cursor API."""
from functools import wraps

from google.api_core.exceptions import (
    AlreadyExists,
    FailedPrecondition,
    InternalServerError,
    InvalidArgument,
)
from google.cloud.spanner_v1 import param_types

from .exceptions import (
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from .parse_utils import (
    STMT_DDL,
    STMT_INSERT,
    STMT_NON_UPDATING,
    classify_stmt,
    ensure_where_clause,
    get_param_types,
    parse_insert,
    sql_pyformat_args_to_spanner,
)
from .utils import PeekIterator

_UNSET_COUNT = -1

# This table maps spanner_types to Spanner's data type sizes as per
#   https://cloud.google.com/spanner/docs/data-types#allowable-types
# It is used to map `display_size` to a known type for Cursor.description
# after a row fetch.
# Since ResultMetadata
#   https://cloud.google.com/spanner/docs/reference/rest/v1/ResultSetMetadata
# does not send back the actual size, we have to lookup the respective size.
# Some fields' sizes are dependent upon the dynamic data hence aren't sent back
# by Cloud Spanner.
code_to_display_size = {
    param_types.BOOL.code: 1,
    param_types.DATE.code: 4,
    param_types.FLOAT64.code: 8,
    param_types.INT64.code: 8,
    param_types.TIMESTAMP.code: 12,
}


def _is_connection_closed(func):
    """Raise an exception if this cursor is closed.

    Helper to check this cursor's state before running a
    SQL/DDL/DML query. If the parent connection is
    already closed it also raises an error.

    :raises: :class:`InterfaceError` if this cursor or connection is closed."""

    @wraps(func)
    def wrapped(self):
        if self._is_closed:
            raise InterfaceError("cursor is already closed")
        elif self._connection.is_closed:
            raise InterfaceError("connection is already closed")

    return wrapped


class Cursor(object):
    """
    Database cursor to manage the context of a fetch operation.

    :type connection: :class:`spanner_dbapi.connection.Connection`
    :param connection: Parent connection object for this Cursor.
    """

    def __init__(self, connection):
        self._connection = connection
        self._is_closed = False
        self._stream = None
        self._itr = None
        self._row_count = _UNSET_COUNT
        self.arraysize = 1
        self._ddl_statements = []

    @property
    def is_closed(self):
        """The cursor close indicator.
        :rtype: :class:`bool`
        :returns: True if this cursor or it's parent connection is closed,
                  False otherwise."""

        return self._is_closed or self._connection.is_closed

    @property
    def connection(self):
        return self._connection

    @property
    def rowcount(self):
        return self._row_count

    @property
    def description(self):
        """Returns tuple with description of every column of the resulting rows
        with several fields

        :return: tuple
        """
        if not (self._stream and self._stream.metadata):
            return None

        row_type = self._stream.metadata.row_type
        columns = []
        for field in row_type.fields:
            columns.append(
                Column(
                    name=field.name,
                    type_code=field.type.code,
                    # Size of the SQL type of the column.
                    display_size=code_to_display_size.get(field.type.code),
                    # Client perceived size of the column.
                    internal_size=field.ByteSize(),
                )
            )
        return tuple(columns)

    def close(self):
        """Closes this cursor."""

        self._connection = None
        self._is_closed = True

    @_is_connection_closed
    def execute(self, sql, args=None):
        """Abstracts and implements execute SQL statements on Cloud Spanner.
        Args:
            sql: A SQL statement
            *args: variadic argument list
            **kwargs: key worded arguments
        Returns:
            None
        """

        if not self._connection:
            raise ProgrammingError("Cursor is not connected to the database")

        self._stream = None

        # Classify whether this is a read-only SQL statement.
        try:
            classification = classify_stmt(sql)
            if classification == STMT_DDL:
                self._ddl_statements.append()
                self._run_ddl_statements(sql)
            elif classification == STMT_NON_UPDATING:
                self._handle_dql(sql, args or None)
            elif classification == STMT_INSERT:
                self._handle_insert(sql, args or None)
            else:
                self._handle_update(sql, args or None)
        except (AlreadyExists, FailedPrecondition) as e:
            raise IntegrityError(e.details if hasattr(e, "details") else e)
        except InvalidArgument as e:
            raise ProgrammingError(e.details if hasattr(e, "details") else e)
        except InternalServerError as e:
            raise OperationalError(e.details if hasattr(e, "details") else e)

    def executemany(self, operation, seq_of_params):
        if not self._connection:
            raise ProgrammingError("Cursor is not connected to the database")

        for params in seq_of_params:
            self.execute(operation, params)

    @_is_connection_closed
    def fetchone(self):
        try:
            return next(self)
        except StopIteration:
            return None

    @_is_connection_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences.
        An empty sequence is returned when no more rows are available.

        Args:
            size: optional integer to determine the maximum number of results to fetch.


        Raises:
            Error if the previous call to .execute*() did not produce any result set
            or if no call was issued yet.
        """

        if size is None:
            size = self.arraysize

        items = []
        for i in range(size):
            try:
                items.append(tuple(self.__next__()))
            except StopIteration:
                break

        return items

    @_is_connection_closed
    def fetchall(self):
        return list(self.__iter__())

    def __next__(self):
        if self._itr is None:
            raise ProgrammingError("no results to return")
        return next(self._itr)

    def __iter__(self):
        if self._itr is None:
            raise ProgrammingError("no results to return")
        return self._itr

    @_is_connection_closed
    def _handle_update(self, sql, params):
        self._connection.database.run_in_transaction(self.__do_execute_update,
                                                     sql, params)

    def __do_execute_update(self, transaction, sql, params, param_types=None):
        sql = ensure_where_clause(sql)
        sql, params = sql_pyformat_args_to_spanner(sql, params)

        res = transaction.execute_update(
            sql, params=params, param_types=get_param_types(params)
        )
        self._itr = None
        if type(res) == int:
            self._row_count = res

        return res

    def _handle_insert(self, sql, params):
        parts = parse_insert(sql, params)

        # The split between the two styles exists because:
        # in the common case of multiple values being passed
        # with simple pyformat arguments,
        #   SQL: INSERT INTO T (f1, f2) VALUES (%s, %s, %s)
        #   Params:   [(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,)]
        # we can take advantage of a single RPC with:
        #       transaction.insert(table, columns, values)
        # instead of invoking:
        #   with transaction:
        #       for sql, params in sql_params_list:
        #           transaction.execute_sql(sql, params, param_types)
        # which invokes more RPCs and is more costly.

        if parts.get("homogenous"):
            # The common case of multiple values being passed in
            # non-complex pyformat args and need to be uploaded in one RPC.
            return self._connection.database.run_in_transaction(
                self._do_execute_insert_homogenous, parts
            )
        else:
            # All the other cases that are esoteric and need
            #   transaction.execute_sql
            sql_params_list = parts.get("sql_params_list")
            return self._connection.database.run_in_transaction(
                self._do_execute_insert_heterogenous, sql_params_list
            )

    def _do_execute_insert_heterogenous(self, transaction, sql_params_list):
        for sql, params in sql_params_list:
            sql, params = sql_pyformat_args_to_spanner(sql, params)
            param_types = get_param_types(params)
            res = transaction.execute_sql(
                sql, params=params, param_types=param_types
            )
            # TODO: File a bug with Cloud Spanner and the Python client maintainers
            # about a lost commit when res isn't read from.
            _ = list(res)

    def _do_execute_insert_homogenous(self, transaction, parts):
        # Perform an insert in one shot.
        table, columns, values = parts.get("table"), parts.get(
            "columns"), parts.get("values")

        return transaction.insert(table, columns, values)

    @_is_connection_closed
    def _handle_dql(self, sql, params):
        with self._connection.database.snapshot() as snapshot:
            # Reference
            #  https://googleapis.dev/python/spanner/latest/session-api.html#google.cloud.spanner_v1.session.Session.execute_sql
            sql, params = sql_pyformat_args_to_spanner(sql, params)
            res = snapshot.execute_sql(
                sql, params=params, param_types=get_param_types(params)
            )
            if type(res) == int:
                self._row_count = res
                self._itr = None
            else:
                # Immediately using:
                #   iter(response)
                # here, because this Spanner API doesn't provide
                # easy mechanisms to detect when only a single item
                # is returned or many, yet mixing results that
                # are for .fetchone() with those that would result in
                # many items returns a RuntimeError if .fetchone() is
                # invoked and vice versa.
                self._stream = res
                # Read the first element so that StreamedResult can
                # return the metadata after a DQL statement. See issue #155.
                self._itr = PeekIterator(self._stream)
                # Unfortunately, Spanner doesn't seem to send back
                # information about the number of rows available.
                self._row_count = _UNSET_COUNT

    def __enter__(self):
        return self

    def __exit__(self, etype, value, traceback):
        self.close()

    def setinputsizes(self):
        raise ProgrammingError("Unimplemented")

    def setoutputsize(self, column=None):
        raise ProgrammingError("Unimplemented")

    @_is_connection_closed
    def _run_ddl_statements(self, sql):
        return self._connection.database.update_ddl(sql).result()


class Column:
    def __init__(
            self,
            name,
            type_code,
            display_size=None,
            internal_size=None,
            precision=None,
            scale=None,
            null_ok=False,
    ):
        self.name = name
        self.type_code = type_code
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.null_ok = null_ok

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        string = ", ".join(
            [
                field
                for field in [
                    "name='{}'".format(self.name),
                    "type_code={}".format(self.type_code),
                    None
                    if not self.display_size
                    else "display_size={}".format(self.display_size),
                    None
                    if not self.internal_size
                    else "internal_size={}".format(self.internal_size),
                    None
                    if not self.precision
                    else "precision='{}'".format(self.precision),
                    None if not self.scale else "scale='{}'".format(self.scale),
                    None
                    if not self.null_ok
                    else "null_ok='{}'".format(self.null_ok),
                ]
                if field
            ]
        )

        return "Column({})".format(string)
