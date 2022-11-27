import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data (logs and songs) from S3-storage into the staging tables
    """
    for query in copy_table_queries:
        print("Executing the following query:{}".format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    The transformation phase. Here the data is extracted from the staging tables and inserted into the five target tables
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