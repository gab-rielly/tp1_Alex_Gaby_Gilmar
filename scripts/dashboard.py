import psycopg2
from psycopg2 import sql
from config import config

class Dashboard:

    def __init__(self):
        self.params = config()

    def line(self):
        print("-" * 50)

    def verifica_produto(self, asin):
        params = config()
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tabela_produto WHERE asin = %s", (asin,))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists
#dashboard a
    def lista_avaliacao(self, asin):
        try:
            params = config()

            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                
                self.line()
                # Consulta para os 5 comentários mais úteis com maior avaliação
                consulta_maior_avaliacao = sql.SQL("""
                    SELECT * FROM (
                        SELECT ROW_NUMBER() OVER (ORDER BY avaliacao DESC, util DESC) AS top,
                            cliente, 
                            TO_CHAR(data,'YYYY-MM-DD') AS data_formatada,
                            avaliacao,
                            util
                        FROM tabela_review
                        WHERE asin_produto = %s
                    ) AS top_reviews
                    WHERE top <= 5
                """)

                # Executar a consulta para maior avaliação
                cur.execute(consulta_maior_avaliacao, (asin,))
                rows_maior_avaliacao = cur.fetchall()

                print("-> Top 5 comentários mais úteis e com maior avaliação:")
                print("(id_produto, cliente, data, avaliacao, util)\n")
                for row in rows_maior_avaliacao:
                    print(row)
                self.line()

                # Consulta para os 5 comentários mais úteis com menor avaliação
                consulta_menor_avaliacao = sql.SQL("""
                    SELECT * FROM (
                        SELECT ROW_NUMBER() OVER (ORDER BY avaliacao ASC, util DESC) AS top,
                            cliente,
                            TO_CHAR(data,'YYYY-MM-DD') AS data_formatada,
                            avaliacao,
                            util
                        FROM tabela_review
                        WHERE asin_produto = %s
                    ) AS top_reviews
                    WHERE top <= 5
                """)

                # Executar a consulta para menor avaliação
                cur.execute(consulta_menor_avaliacao, (asin,))
                rows_menor_avaliacao = cur.fetchall()

                print("-> Top 5 comentários mais úteis e com menor avaliação:")
                print("(id_produto, cliente, data, avaliacao, util)\n")
                
                for row in rows_menor_avaliacao:
                    print(row)

                self.line()

        except Exception as e:
            print(f"Erro ao executar a consulta SQL em lista_avaliacao: {e}")
#dashboard b
    def lista_maior_similar(self, asin):
        try:
            params = config()
            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                consulta = sql.SQL("""
                    SELECT tabela_similar.asin_similar, tabela_produto.titulo, tabela_produto.rank_vendas
                    FROM tabela_similar
                    JOIN tabela_produto ON tabela_similar.asin_similar = tabela_produto.asin
                    WHERE tabela_similar.asin_produto = %s AND tabela_produto.rank_vendas < (
                        SELECT rank_vendas
                        FROM tabela_produto
                        WHERE asin = %s
                    )
                    ORDER BY tabela_produto.rank_vendas ASC
                    LIMIT 5
                """)

                cur.execute(consulta, (asin, asin))

                rows = cur.fetchall()
                cont = 1

                if len(rows) > 1:
                    print(f"-> Estes são os produtos similares com vendas maiores que o asin= {asin}:\n")
                else:
                    print(f"-> O produto com asin= {asin} não possui produtos similares com vendas maiores que ele.")

                for produto in rows:
                    print(f'Produto {cont}:\n')
                    print(f'Asin_similar: {produto[0]}')
                    print(f'Título: {produto[1]}')
                    print(f'Rank_vendas: {produto[2]}')
                    self.line()
                    cont += 1

        except Exception as error:
            print(f"Erro ao executar a consulta SQL em lista_maior_similar: {error}")
#dashboard c
    def mostra_evolucao_diaria(self, asin, n):
        try:
            params = config()

            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT date_trunc('day', data) AS data_review, AVG(avaliacao) AS media_avaliacao
                    FROM tabela_review
                    WHERE asin_produto = %s
                    GROUP BY data_review
                    ORDER BY data_review
                    LIMIT %s
                """, (asin, n))
                rows = cur.fetchall()
                self.line()

                print(f"-> Evolução diária das avaliações médias do produto com asin= {asin} ao longo de {n} dias:\n")
                for dia in rows:
                    data = dia[0]
                    data_formatada = data.strftime('%Y-%m-%d')
                    print(f'Data: {data_formatada} Média avaliação: {dia[1]:.2f}')
                self.line()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
#dashboard d
    def lista_produtos_lideres(self):
        try:
            params = config()

            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                
                cur.execute("SELECT DISTINCT grupo_produto FROM tabela_produto") #Isso retornara todos os valores unicos encontrados na coluna especificada da tabela.
                gp_produtos_distintos = cur.fetchall()

                print("-> Top 10 produtos mais vendidos em cada grupo de produtos:\n")
                for grupo in gp_produtos_distintos:
                    print("-"*10)
                    print(f'Grupo: {grupo[0]}')
                    print("-"*10)
                    cur.execute("""
                        SELECT tabela_produto.titulo, tabela_produto.rank_vendas
                        FROM tabela_produto
                        WHERE tabela_produto.grupo_produto = %s AND tabela_produto.rank_vendas >= 1
                        ORDER BY tabela_produto.rank_vendas
                        LIMIT 10;
                    """, (grupo,))
                    row = cur.fetchall()
                    for produto in row:
                        print(f'Título: {produto[0]}')
                        print(f'Ranque de vendas: {produto[1]}')
                        print("-"*10)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
#dashboard e
    def lista_produtos_media_util(self):
        try:
            params = config()

            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT tabela_produto.asin, tabela_produto.titulo, AVG(tabela_review.util)
                    FROM tabela_produto
                    JOIN tabela_review ON tabela_produto.asin = tabela_review.asin_produto
                    GROUP BY tabela_produto.asin, tabela_produto.titulo
                    ORDER BY AVG(tabela_review.util) DESC
                    LIMIT 10;
                """)
                self.line()
                print("-> Top 10 produtos com a maior média de avaliações úteis positivas:\n")
                rows = cur.fetchall()
                
                cont = 1
                for product in rows:
                    print("-"*10)
                    print(f'Top {cont}:')
                    print(f'ASIN: {product[0]}')
                    print(f'Título: {product[1]}')
                    print(f'Média util: {product[2]:.2f}')
                    print("-"*10)
                    cont += 1

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
#dashboard f    
    def lista_categoria_media_avaliacao(self):
        try:
            params = config()
            with psycopg2.connect(**params) as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT tabela_categoria.nome_categoria, AVG(tabela_review.util)
                    FROM tabela_categoria
                    JOIN tabela_produto ON tabela_categoria.asin_produto = tabela_produto.asin
                    JOIN tabela_review ON tabela_review.asin_produto = tabela_produto.asin
                    WHERE tabela_review.util > 0
                    GROUP BY tabela_categoria.nome_categoria
                    ORDER BY AVG(tabela_review.util) DESC
                    LIMIT 5;
                """)
                rows = cur.fetchall()
                self.line()
                print("-> Top 5 categorias de produtos com a maior média de avaliações úteis positivas por produto:")
                
                cont = 1
                for row in rows:
                    print(f'Top {cont}')
                    print(f"Categoria: {row[0]}\nMédia util: {row[1]:.2f}")
                    self.line()
                    cont += 1
                    

        except Exception as e:
            print(f"Um erro ocorreu em lista_categoria_media_avaliacao: {e}")
#dashboard g
