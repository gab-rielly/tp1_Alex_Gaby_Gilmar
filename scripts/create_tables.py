import psycopg2
from config import config

def create_tables():
    """ Create tables in the PostgreSQL database """
    conn = None
    try:
        # Read connection parameters from config file
        params = config()

        # Connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # Create a cursor
        cur = conn.cursor()

        # Create the 'personagem' table if it does not exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personagem (
                nome VARCHAR(50) NOT NULL PRIMARY KEY,
                filme VARCHAR(50)
            )
        """)
        print('Table "personagem" created successfully.')

        # Create the 'citações' table if it does not exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS citacoes (
                id SERIAL PRIMARY KEY,
                citacao TEXT NOT NULL,
                personagem_nome VARCHAR(50) REFERENCES personagem(nome)
            )
        """)
        print('Table "citações" created successfully.')

        # Commit the changes
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')