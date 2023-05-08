import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to staging tables in RedShift according to sql_queries.py.
    
    Raises:
        Exceptions in case of any issues. It will proceed with the other tables, so it's necessary to pay attention to the logs carefully.
    """
    for query in copy_table_queries:
        try:
            print(f"Starting to execute {query}")
            cur.execute(query)
            conn.commit()
            logging.info(f"Table created successfully: {query.split()[-1]}")
        except Exception as e:
            logging.error(f"Error creating table {query.split()[-1]}: {e}")
            raise e


def insert_tables(cur, conn):
    """
    Inserts data from all the tables created using create_tables.py, except for the staging tables that are filled directly with S3 data and     are used to insert data to the rest of the tables.
    Raises:
        Exceptions in case of any issues. It will proceed with the other tables, so it's necessary to pay attention to the logs carefully.
    """
    for query in insert_table_queries:
        try:
            print(f"Starting to execute {query}")
            cur.execute(query)
            conn.commit()
            logging.info(f"Data inserted successfully into table: {query.split()[-3]}")
        except Exception as e:
            logging.error(f"Error inserting data into table {query.split()[-3]}: {e}")
            raise e


def main():
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
