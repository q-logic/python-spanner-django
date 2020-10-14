# Copyright 2020 Google LLC
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""This application demonstrates how to do basic operations using Cloud
Spanner DBAPI.

For more information, see the README.rst under /python-spanner-django.
"""

import argparse

from google.cloud.spanner_dbapi import connect


def create_connection(instance_id, database_id):
    """Create a connection to Cloud Spanner database."""
    # [START spanner_dbapi_create_connection]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)

    print("Created connection to database {}".format(database_id))
    # [END spanner_dbapi_create_connection]


def close_connection(instance_id, database_id):
    """Close current connection."""
    # [START spanner_dbapi_close_connection]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)

    connection.close()

    print("Connection is closed")
    # [END spanner_dbapi_close_connection]


def create_cursor(instance_id, database_id):
    """Create a DB-API Cursor for current connection."""
    # [START spanner_dbapi_create_cursor]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)

    cursor = connection.cursor()

    print("Created cursor for current connection")
    # [END spanner_dbapi_create_cursor]


def close_cursor(instance_id, database_id):
    """Closes current Cursor."""
    # [START spanner_dbapi_close_cursor]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    cursor.close()

    print("Cursor is closed")
    # [END spanner_dbapi_close_cursor]


def execute_operation(instance_id, database_id):
    """Executes a Spanner database operation."""
    # [START spanner_dbapi_execute_operation]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    sql = """
        INSERT Singers (SingerId, FirstName, LastName)
        VALUES (1, 'Marc', 'Richards'), 
               (2, 'Catalina', 'Smith'),
               (3, 'Alice', 'Trentor'),
               (4, 'Lea', 'Martin'),
               (5, 'David', 'Lomond'),
        """
    cursor.execute(sql)
    connection.close()

    print('Operation is executed')
    # [END spanner_dbapi_execute_operation]


def executemany_operation(instance_id, database_id):
    """Executes a Spanner database operation with every parameters set."""
    # [START spanner_dbapi_executemany_operation]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    sql = """
        SELECT SingerId, FirstName, LastName 
        FROM Singers 
        WHERE LastName = @lastName
        """
    cursor.executemany(sql, [{"lastName": "Smith"}, {"lastName": "Martin"}])
    connection.close()

    print('Operation is executed')
    # [END spanner_dbapi_executemany_operation]


def fetch_next_row(instance_id, database_id):
    """Fetch the next row of a query result set."""
    # [START spanner_dbapi_fetch_next_row]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    sql = "SELECT SingerId, FirstName, LastName FROM Singers"
    cursor.execute(sql)

    row = cursor.fetchone()
    connection.close()

    print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))
    # [END spanner_dbapi_fetch_next_row]


def fetch_many_rows(instance_id, database_id):
    """Fetch the next set of 5 rows of a query result."""
    # [START spanner_dbapi_fetch_many_rows]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    sql = "SELECT SingerId, FirstName, LastName FROM Singers"
    cursor.execute(sql)

    rows = cursor.fetchmany(size=5)
    connection.close()

    for row in rows:
        print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))
    # [END spanner_dbapi_fetch_many_rows]


def fetch_all_rows(instance_id, database_id):
    """Fetch all (remaining) rows of a query result."""
    # [START spanner_dbapi_fetch_all_rows]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)
    cursor = connection.cursor()

    sql = "SELECT SingerId, FirstName, LastName FROM Singers"
    cursor.execute(sql)

    rows = cursor.fetchall()

    for row in rows:
        print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))
    # [END spanner_dbapi_fetch_all_rows]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("instance_id", help="Your Cloud Spanner instance ID.")
    parser.add_argument(
        "--database-id", help="Your Cloud Spanner database ID.", default="example_db"
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("create_connection", help=create_connection.__doc__)
    subparsers.add_parser("close_connection", help=close_connection.__doc__)
    subparsers.add_parser("create_cursor", help=create_cursor.__doc__)
    subparsers.add_parser("close_cursor", help=close_cursor.__doc__)
    subparsers.add_parser("execute_operation", help=execute_operation.__doc__)
    subparsers.add_parser(
        "executemany_operation", help=executemany_operation.__doc__
    )
    subparsers.add_parser("fetch_next_row", help=fetch_next_row.__doc__)
    subparsers.add_parser("fetch_many_rows", help=fetch_many_rows.__doc__)
    subparsers.add_parser("fetch_all_rows", help=fetch_all_rows.__doc__)

    args = parser.parse_args()

    if args.command == "create_connection":
        create_connection(args.instance_id, args.database_id)
    elif args.command == "close_connection":
        close_connection(args.instance_id, args.database_id)
    elif args.command == "create_cursor":
        create_cursor(args.instance_id, args.database_id)
    elif args.command == "close_cursor":
        close_cursor(args.instance_id, args.database_id)
    elif args.command == "execute_operation":
        execute_operation(args.instance_id, args.database_id)
    elif args.command == "executemany_operation":
        executemany_operation(args.instance_id, args.database_id)
    elif args.command == "fetch_next_row":
        fetch_next_row(args.instance_id, args.database_id)
    elif args.command == "fetch_many_rows":
        fetch_many_rows(args.instance_id, args.database_id)
    elif args.command == "fetch_all_rows":
        fetch_all_rows(args.instance_id, args.database_id)
