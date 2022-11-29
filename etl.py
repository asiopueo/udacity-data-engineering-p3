import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: Loads data (logs and songs) from S3-storage into the staging tables
    utilizing the SQL queries in sql_queries.py

    Arguments:
        cur: the cursor object
        conn: database connection object

    Returns:
        None
    """
    for query in copy_table_queries:
        print("Executing the following query:{}".format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: The transformation phase. Here, the data is extracted from the staging tables and inserted into the five 
    target tables utilizing the SQL queries in sql_queries.py
    
    Arguments:
        cur: the cursor object
        conn: database connection object

    Returns:
        None
    """
    for query in insert_table_queries:
        print("Executing the following query:{}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()