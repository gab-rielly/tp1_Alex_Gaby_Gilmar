import psycopg2
from psycopg2 import sql, extensions, errors

def create_tables():
    # con é responsável por se conectar ao banco de dados PostgreSQL
    con = psycopg2.connect(host = 'localhost', dbname = 'testes', user = 'postgres', 
                           password = 'VIDEOGAME03', port = 5432)

    # Criação do cursor
    cur = con.cursor()

    create_table_query1 = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        id_produto INT PRIMARY KEY NOT NULL,
        asin CHAR(10) NOT NULL,
        titulo VARCHAR(256) NOT NULL,
        grupo_produto VARCHAR(26) NOT NULL,
        rank_vendas INT NOT NULL,
        UNIQUE(asin)
    )
    """).format(sql.Identifier('tabela_produto'))

    create_table_query2 = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        asin_produto CHAR(10) NOT NULL,
        asin_similar CHAR(10),
        PRIMARY KEY(asin_produto, asin_similar),
        FOREIGN KEY(asin_produto) REFERENCES tabela_produto(asin)
    )
    """).format(sql.Identifier('tabela_similar'))

    create_table_query3 = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        asin_do_produto CHAR(10) PRIMARY KEY NOT NULL,
        categoria_id INT NOT NULL,
        UNIQUE(categoria_id),
        FOREIGN KEY(asin_do_produto) REFERENCES tabela_produto(asin)
    )
    """).format(sql.Identifier('tabela_categoria'))

    create_table_query4 = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        id_categoria INT PRIMARY KEY NOT NULL,
        nome_categoria VARCHAR(36) NOT NULL,
        FOREIGN KEY(id_categoria) REFERENCES tabela_categoria(categoria_id)
    )
    """).format(sql.Identifier('tabela_categoria_info'))

    create_table_query5 = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        id_review INT NOT NULL,
        identificacao_produto INT NOT NULL,
        asin_produto CHAR(10) NOT NULL,
        cliente VARCHAR(14) NOT NULL,
        data VARCHAR(20) NOT NULL,
        avaliacao INT NOT NULL,
        util INT,
        PRIMARY KEY(id_review, identificacao_produto, asin_produto),
        FOREIGN KEY(identificacao_produto) REFERENCES tabela_produto(id_produto),
        FOREIGN KEY(asin_produto) REFERENCES tabela_produto(asin)
    )
    """).format(sql.Identifier('tabela_review'))

    try:

        cur.execute(create_table_query1)
        cur.execute(create_table_query2)
        cur.execute(create_table_query3)
        cur.execute(create_table_query4)
        cur.execute(create_table_query5)

        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        
        if con is not None:
            con.close()
            