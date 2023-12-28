import psycopg2
import subprocess

def create_db():
    conn = None
    try:
        # connect to the PostgreSQL server without specifying the database name
        conn = psycopg2.connect(dbname='postgres', user='postgres', password='root', host='localhost')
        conn.autocommit = True  

        # create a cursor object to execute commands
        cur = conn.cursor()

        # check if database 'amazon' exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'amazon'")

        if not cur.fetchone():
            # create database 'amazon' if it does not exist
            cur.execute("CREATE DATABASE amazon")

        # close cursor and connection
        cur.close()
        conn.close()

        print("'amazon' database created successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()