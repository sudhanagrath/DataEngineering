import sqlite3
from sql_queries import create_table_queries, drop_table_queries


def create_connection(db_file):
    """
    - Creates and connects to the pb_db
    - Returns the connection and cursor to pb_db
    """
    conn = None

    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        


def main():
    """
    - Establishes connection with the sqlite database and gets
    cursor to it.  
    
    - Drops the two tables.  
    
    - Creates the two tables. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_connection('pb_db.dbf')
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
