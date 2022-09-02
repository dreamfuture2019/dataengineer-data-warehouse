import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This procedure processes song files & log files from S3 and load into staging tables in RedShift
    Parameters:
    * cur: the cursor variable
    * conn: the connection database variable
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This procedure processes to load specific informations from staging tables and insert into tables in RedShift
    Parameters:
    * cur: the cursor variable
    * conn: the connection database variable
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Read configuration to load RedShift information
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Processes song files & log files to store into staging tables in database.  
    
    - Load and Transform data from staging tables into database. 
    
    - Finally, closes the connection. 
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