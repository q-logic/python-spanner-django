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


# [START spanner_dbapi_create_table]
def create_table(instance_id, database_id):
    """Creates a table for sample data."""
    with connect(instance_id, database_id) as conn:
        with conn.cursor() as curs:
            sql = """CREATE TABLE Singers (
                SingerId     INT64 NOT NULL,
                FirstName    STRING(1024),
                LastName     STRING(1024),
                SingerInfo   BYTES(MAX)
                ) PRIMARY KEY (SingerId)
                """
            curs.execute(sql)

        print("Created table in database {}".format(database_id))


# [END spanner_dbapi_create_table]


# [START spanner_dbapi_create_connection]
def create_connection(instance_id, database_id):
    """Create a connection to Cloud Spanner database."""
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)

    print("Created connection to database {}".format(database_id))


# [END spanner_dbapi_create_connection]


# [START spanner_dbapi_close_connection]
def close_connection(instance_id, database_id):
    """Close current connection."""
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)

    connection.close()

    print("Connection is closed: {}".format(connection.is_closed))


# [END spanner_dbapi_close_connection]


# [START spanner_dbapi_create_cursor]
def create_cursor(instance_id, database_id):
    """Create a DB-API Cursor for current connection."""
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    connection = connect(instance_id, database_id)

    cursor = connection.cursor()

    print("Created cursor")


# [END spanner_dbapi_create_cursor]


# [START spanner_dbapi_close_cursor]
def close_cursor(instance_id, database_id):
    """Closes current Cursor."""
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    with connect(instance_id, database_id) as conn:
        cursor = conn.cursor()

        cursor.close()

    print("Connection is closed: {}".format(cursor.is_closed))


# [END spanner_dbapi_close_cursor]


# [START spanner_dbapi_insert_data]
def insert_data(instance_id, database_id):
    """Inserts sample data into the given database.

    The database and table must already exist and a table can be created using
    `create_table`.
    """
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    with connect(instance_id, database_id) as conn:
        with conn.cursor() as curs:
            sql = """
                INSERT Singers (SingerId, FirstName, LastName)
                VALUES (1, 'Marc', 'Martin'), 
                       (2, 'Catalina', 'Smith'),
                       (3, 'Alice', 'Trentor'),
                       (4, 'Lea', 'Martin'),
                       (5, 'David', 'Lomond')
                """
            curs.execute(sql)

            print("Inserted data.")


# [END spanner_dbapi_insert_data]


# [START spanner_dbapi_query_with_parameters]
def query_with_parameters(instance_id, database_id):
    """Query a table using parameters.

    The database and table must already exist and a table can be created using
    `create_table`.
    """
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    with connect(instance_id, database_id) as conn:
        with conn.cursor() as curs:
            sql = """
                INSERT Singers (SingerId, FirstName, LastName)
                VALUES (1, 'Marc', 'Martin'), 
                       (2, 'Catalina', 'Smith'),
                       (3, 'Alice', 'Trentor'),
                       (4, 'Lea', 'Martin'),
                       (5, 'David', 'Lomond')
                """
            curs.execute(sql)

            print("Inserted data.")


            sql = """
                SELECT SingerId, FirstName, LastName 
                FROM Singers 
                WHERE LastName = @lastName
                """
            curs.executemany(
                sql, [{"lastName": "Smith"}, {"lastName": "Martin"}]
            )

            for row in curs:
                print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))


# [END spanner_dbapi_query_with_parameters]


# [START spanner_dbapi_fetch_next_row]
def fetch_next_row(instance_id, database_id):
    """Fetch the next row of a query result set.

    The database and table must already exist and a table can be created using
    `create_table`.
    """
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    with connect(instance_id, database_id) as conn:
        with conn.cursor() as curs:
            sql = """
                INSERT Singers (SingerId, FirstName, LastName)
                VALUES (1, 'Marc', 'Martin'), 
                       (2, 'Catalina', 'Smith'),
                       (3, 'Alice', 'Trentor'),
                       (4, 'Lea', 'Martin'),
                       (5, 'David', 'Lomond')
                """
            curs.execute(sql)

            print("Inserted data.")


            sql = "SELECT SingerId, FirstName, LastName FROM Singers"
            curs.execute(sql)
            row = curs.fetchone()

            print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))


# [END spanner_dbapi_fetch_next_row]


# [START spanner_dbapi_fetch_many_rows]
def fetch_many_rows(instance_id, database_id):
    """Fetch the next set of 5 rows of a query result.

    The database and table must already exist and a table can be created using
    `create_table`.
    """
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    with connect(instance_id, database_id) as conn:
        with conn.cursor() as curs:
            sql = """
                INSERT Singers (SingerId, FirstName, LastName)
                VALUES (1, 'Marc', 'Martin'), 
                       (2, 'Catalina', 'Smith'),
                       (3, 'Alice', 'Trentor'),
                       (4, 'Lea', 'Martin'),
                       (5, 'David', 'Lomond')
                """
            curs.execute(sql)

            print("Inserted data.")


            sql = "SELECT SingerId, FirstName, LastName FROM Singers"
            curs.execute(sql)
            rows = curs.fetchmany(size=5)

            for row in rows:
                print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))


# [END spanner_dbapi_fetch_many_rows]


# [START spanner_dbapi_fetch_all_rows]
def fetch_all_rows(instance_id, database_id):
    """Fetch all (remaining) rows of a query result.

    The database and table must already exist and a table can be created using
    `create_table`.
    """
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    with connect(instance_id, database_id) as conn:
        with conn.cursor() as curs:
            sql = """
                INSERT Singers (SingerId, FirstName, LastName)
                VALUES (1, 'Marc', 'Martin'), 
                       (2, 'Catalina', 'Smith'),
                       (3, 'Alice', 'Trentor'),
                       (4, 'Lea', 'Martin'),
                       (5, 'David', 'Lomond')
                """
            curs.execute(sql)

            print("Inserted data.")


            sql = "SELECT SingerId, FirstName, LastName FROM Singers"
            curs.execute(sql)
            rows = curs.fetchall()

            for row in rows:
                print("SingerId: {}, FirstName: {}, LastName: {}".format(*row))


# [END spanner_dbapi_fetch_all_rows]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("instance_id", help="Your Cloud Spanner instance ID.")
    parser.add_argument(
        "--database-id",
        help="Your Cloud Spanner database ID.",
        default="example_db",
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("create_table", help=create_table.__doc__)
    subparsers.add_parser("create_connection", help=create_connection.__doc__)
    subparsers.add_parser("close_connection", help=close_connection.__doc__)
    subparsers.add_parser("create_cursor", help=create_cursor.__doc__)
    subparsers.add_parser("close_cursor", help=close_cursor.__doc__)
    subparsers.add_parser("insert_data", help=insert_data.__doc__)
    subparsers.add_parser(
        "query_with_parameters", help=query_with_parameters.__doc__
    )
    subparsers.add_parser("fetch_next_row", help=fetch_next_row.__doc__)
    subparsers.add_parser("fetch_many_rows", help=fetch_many_rows.__doc__)
    subparsers.add_parser("fetch_all_rows", help=fetch_all_rows.__doc__)

    args = parser.parse_args()

    if args.command == "create_table":
        create_table(args.instance_id, args.database_id)
    elif args.command == "create_connection":
        create_connection(args.instance_id, args.database_id)
    elif args.command == "close_connection":
        close_connection(args.instance_id, args.database_id)
    elif args.command == "create_cursor":
        create_cursor(args.instance_id, args.database_id)
    elif args.command == "close_cursor":
        close_cursor(args.instance_id, args.database_id)
    elif args.command == "insert_data":
        insert_data(args.instance_id, args.database_id)
    elif args.command == "executemany_operation":
        query_with_parameters(args.instance_id, args.database_id)
    elif args.command == "fetch_next_row":
        fetch_next_row(args.instance_id, args.database_id)
    elif args.command == "fetch_many_rows":
        fetch_many_rows(args.instance_id, args.database_id)
    elif args.command == "fetch_all_rows":
        fetch_all_rows(args.instance_id, args.database_id)
    else:
        print("Command {} did not match expected commands.".format(args.command))
