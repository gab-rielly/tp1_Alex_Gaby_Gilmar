import psycopg2
from psycopg2 import sql
from config import config

class BancoDeDados:
    def __init__(self):
        self.conn = None

    def conectar(self):
        try:
            # Conectar ao servidor PostgreSQL sem especificar o nome do banco de dados
            self.conn = psycopg2.connect(dbname='postgres', user='postgres', password='root', host='localhost')
            self.conn.autocommit = True
        except (Exception, psycopg2.DatabaseError) as erro:
            print(erro)

    def desconectar(self):
        if getattr(self, 'conn', None) is not None:
            self.conn.close()
            print('Conexão com o banco de dados fechada.')

    def criar_bd(self):
        try:
            with self.conn.cursor() as cursor:
                # Verificar se o banco de dados 'amazon' existe
                cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'amazon'")

                if not cursor.fetchone():
                    # Criar o banco de dados 'amazon' se ele não existir
                    cursor.execute("CREATE DATABASE amazon")
                    print("'amazon' banco de dados criado com sucesso!")
        except (Exception, psycopg2.DatabaseError) as erro:
            print(erro)

    def criar_tabelas(self):
        try:
            parameters = config()
            with psycopg2.connect(**parameters) as conn:
                # Criação do cursor
                with conn.cursor() as cur:
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
                            asin_produto CHAR(10)  NOT NULL,
                            nome_categoria VARCHAR(256) NOT NULL,
                            FOREIGN KEY(asin_produto) REFERENCES tabela_produto(asin)
                        )
                    """).format(sql.Identifier('tabela_categoria'))

                    cria_tabela_subcategoria = sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {} (
                            asin_produto CHAR(10) NOT NULL ,
                            nome_subcategoria VARCHAR(256) NOT NULL,
                            id_subcategoria INT NOT NULL,
                            PRIMARY KEY(asin_produto, id_subcategoria),
                            FOREIGN KEY(asin_produto) REFERENCES tabela_produto(asin)
                        )
                    """).format(sql.Identifier('tabela_subcategoria'))

                    cria_tabela_review = sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {} (
                            id_produto SERIAL NOT NULL,
                            asin_produto CHAR(10) NOT NULL,
                            cliente VARCHAR(14) NOT NULL,
                            data DATE NOT NULL,
                            avaliacao INT NOT NULL,
                            util INT,
                            PRIMARY KEY(id_produto, asin_produto),
                            FOREIGN KEY(asin_produto) REFERENCES tabela_produto(asin)
                        )
                    """).format(sql.Identifier('tabela_review'))

                    cur.execute(cria_tabela_produto)
                    cur.execute(cria_tabela_similar)
                    cur.execute(cria_tabela_categoria)
                    cur.execute(cria_tabela_review)
                    cur.execute(cria_tabela_subcategoria)

                    conn.commit()

                    print('Tabelas criadas com sucesso!')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def drop_bd(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DROP DATABASE IF EXISTS amazon")
                print("'amazon' banco de dados excluído com sucesso!")
        except (Exception, psycopg2.DatabaseError) as erro:
            print(erro)
