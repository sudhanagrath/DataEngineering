import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#function to drop 7 tables if they exist
def drop_tables(cur, conn):
    """
    drops tables if they exist in the query strings of the drop_table_queries list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

#function to create 7 tables if do not exist
def create_tables(cur, conn):
    """
    creates tables if they do not exist in the query strings of the create_table_queries list
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

#automate the dropping and creating of tables
def main():
    """
    -reads the connect parameters from the config file
    -connect to the redshift cluster database
    -returns the cursor object
    -automate the dropping and creating of tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
