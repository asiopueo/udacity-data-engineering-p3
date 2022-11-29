import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: Drops the tables by utilizing the queries stored in sql_queries.py

    Arguments:
        cur: the cursor object
        conn: database connection object

    Returns:
        None
    """
    print("Attempting to drop existing tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Creates the tables by utilizing the queries stored in sql_queries.py

    Arguments:
        cur: the cursor object
        conn: database connection object

    Returns:
        None
    """
    print("Creating tables (2x Staging and 5x OLAP-tables)")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()