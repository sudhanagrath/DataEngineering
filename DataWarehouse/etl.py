import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

#function to load staging tables from the S3 buckets
def load_staging_tables(cur, conn):
    """
     connects to the Redshift cluster and executes the copy queries from the copy_table queries list
     """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

#function to load analytic tables from the staging tables
def insert_tables(cur, conn):
    """
    connects to the Redshift cluster and executes the bulk load queries from the insert_table_queries list
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

#function to automate the table loads
def main():
    """
    - reads the connect parameters from the config file
    - returns the connect and cursor object to use for calling functions
    - call the two functions, load_staging_tables and insert_tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
