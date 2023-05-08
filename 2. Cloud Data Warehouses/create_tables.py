import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in case they exist.
    
    Raises:
        Exception in case an error happens.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: ", query)
            print(e)


def create_tables(cur, conn):   
    """
    Create all the tables according to sql_queries.py script.
    
    Raises:
        Exception in case an error happens.
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: ", query)
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection to database established")
    except psycopg2.Error as e:
        print("Error connecting to database")
        print(e)
        return

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Connection to database closed: If no errors were shown please consider the tables as created")


if __name__ == "__main__":
    main()
