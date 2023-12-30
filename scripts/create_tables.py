import psycopg2
from psycopg2 import sql, extensions, errors
from config import config

def create_tables():
    # con é responsável por se conectar ao banco de dados PostgreSQL
    con = None
    try:
        parameters = config()

        con = psycopg2.connect(**parameters)

        # Criação do cursor
        cur = con.cursor()

        cria_tabela_produto = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            id_produto INT PRIMARY KEY NOT NULL,
            asin CHAR(10) NOT NULL,
            titulo VARCHAR(256) NOT NULL,
            grupo_produto VARCHAR(26) NOT NULL,
            rank_vendas INT NOT NULL,
            UNIQUE(asin)
        )
        """).format(sql.Identifier('tabela_produto'))

        cria_tabela_similar = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            asin_produto CHAR(10) NOT NULL,
            asin_similar CHAR(10),
            PRIMARY KEY(asin_produto, asin_similar),
            FOREIGN KEY(asin_produto) REFERENCES tabela_produto(asin)
        )
        """).format(sql.Identifier('tabela_similar'))

        cria_tabela_categoria = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            asin_do_produto CHAR(10) PRIMARY KEY NOT NULL,
            categoria_id INT NOT NULL,
            UNIQUE(categoria_id),
            FOREIGN KEY(asin_do_produto) REFERENCES tabela_produto(asin)
        )
        """).format(sql.Identifier('tabela_categoria'))

        cria_tabela_categoria_info = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            id_categoria INT PRIMARY KEY NOT NULL,
            nome_categoria VARCHAR(256) NOT NULL,
            FOREIGN KEY(id_categoria) REFERENCES tabela_categoria(categoria_id)
        )
        """).format(sql.Identifier('tabela_categoria_info'))

        cria_tabela_review = sql.SQL("""
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

        cur.execute(cria_tabela_produto)
        cur.execute(cria_tabela_similar)
        cur.execute(cria_tabela_categoria)
        cur.execute(cria_tabela_categoria_info)
        cur.execute(cria_tabela_review)

        con.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if con is not None:
            con.close()