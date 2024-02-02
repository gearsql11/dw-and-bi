from typing import NewType

import psycopg2


PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_actors = "DROP TABLE IF EXISTS actors CASCADE"
table_drop_repo = "DROP TABLE IF EXISTS repo CASCADE"
table_drop_issues = "DROP TABLE IF EXISTS issues CASCADE"
table_drop_comments = "DROP TABLE IF EXISTS comments CASCADE"
table_drop_labels = "DROP TABLE IF EXISTS labels CASCADE"
table_drop_User = "DROP TABLE IF EXISTS user CASCADE"
table_create_payload = "DROP TABLE IF EXISTS payload"


table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors (
        id int,
        login text,
        display_login text,
        url text,
        PRIMARY KEY(id)
    )
"""
table_create_events = """
    CREATE TABLE IF NOT EXISTS events (
        id text,
        type text,
        actor_id int,
        PRIMARY KEY(id),
        CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id)
    )
"""
table_create_repo = """
    CREATE TABLE IF NOT EXISTS repo (
        id text,
        name text,
        url text,
        actor_id int,
        PRIMARY KEY(id),
        CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id)
    )
"""



# table_create_labels = """
#     CREATE TABLE IF NOT EXISTS labels (
#         id text,
#         node_id text,
#         url text,
#         name text,
#         color text,
#         is_default text,
#         description text,
#         PRIMARY KEY(id)
#     )
# """


# table_create_labels = """
#     CREATE TABLE IF NOT EXISTS labels (
#         id int,
#         node_id text,
#         url text,
#         name text,
#         color text,
#         is_default text,
#         description text,
#         PRIMARY KEY(id),
#         CONSTRAINT fk_actor FOREIGN KEY(id) REFERENCES actors(id)
#     )
# """

# table_create_issues = """
#     CREATE TABLE IF NOT EXISTS issues (
#         id text,
#         url text,
#         repository_id int,
#         user_id int,
#         title text,
#         state text,
#         created_at timestamp,
#         updated_at timestamp,
#         PRIMARY KEY(id),
#         FOREIGN KEY(repository_id) REFERENCES repositories(id),
#         FOREIGN KEY(user_id) REFERENCES actors(id)
#     )
# """
# table_create_comments = """
#     CREATE TABLE IF NOT EXISTS comments (
#         id text,
#         issue_id text,
#         user_id int,
#         body text,
#         created_at timestamp,
#         updated_at timestamp,
#         PRIMARY KEY(id),
#         FOREIGN KEY(issue_id) REFERENCES issues(id),
#         FOREIGN KEY(user_id) REFERENCES actors(id)
#     )
# """



# table_create_repositories = """
#     CREATE TABLE IF NOT EXISTS repositories (
#         id int,
#         name text,
#         url text,
#         actor_id int,
#         PRIMARY KEY(id),
#         CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id)
#     )
# """
# table_create_labels = """
#     CREATE TABLE IF NOT EXISTS labels (
#         id text,
#         node_id text,
#         url text,
#         name text,
#         color text,
#         is_default text,
#         description text,
#         PRIMARY KEY (id)
#     )
# """



create_table_queries = [
    table_create_actors,
    table_create_events,
    table_create_repo,
    # table_create_payload,
    # table_create_labels,
    # table_create_issues,
    # table_create_comments,

]
drop_table_queries = [
    table_drop_events,
    table_drop_actors,
    table_drop_repo,
    table_drop_labels,
]


def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
