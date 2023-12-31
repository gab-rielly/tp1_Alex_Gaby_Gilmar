import re
import psycopg2
from config import config

def inserir_dados_arquivo(arquivo):
    # Obtendo a conexão com o banco de dados PostgreSQL
    try:
        parametros = config()
        conexao = psycopg2.connect(**parametros)
        cursor = conexao.cursor()

        with open(arquivo, 'r') as file:
            dados = {}
            for linha in file:
                linha = linha.strip()
                if linha and ':' in linha:
                    chave, valor = re.match(r'^\s*([^:]+):\s*(.*)$', linha).groups()
                    dados[chave.lower()] = valor.strip()
                elif linha == '':
                    # Inserção de dados na tabela produto
                    if 'id' in dados and 'asin' in dados and 'title' in dados and 'group' in dados and 'salesrank' in dados:
                        cursor.execute("""
                        INSERT INTO tabela_produto (id_produto, asin, titulo, grupo_produto, rank_vendas)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (asin) DO NOTHING
                        """, (dados['id'], dados['asin'], dados['title'], dados['group'], dados['salesrank']))

                        # Inserção de dados na tabela similar
                        if 'similar' in dados:
                            similar = dados['similar'].split()
                            for asin_similar in similar[1:]:
                                cursor.execute("""
                                INSERT INTO tabela_similar (asin_produto, asin_similar)
                                VALUES (%s, %s)
                                ON CONFLICT (asin_produto, asin_similar) DO NOTHING
                                """, (dados['asin'], asin_similar))

                        # Inserção de dados na tabela categoria
                        if 'categories' in dados:
                            categorias = dados['categories'].split('|')[1:]
                            for categoria in categorias:
                                categoria_id = categoria.split(']')[0].split('[')[1]
                                cursor.execute("""
                                INSERT INTO tabela_categoria (asin_do_produto, categoria_id)
                                VALUES (%s, %s)
                                ON CONFLICT (asin_do_produto, categoria_id) DO NOTHING
                                """, (dados['asin'], categoria_id))

                    # Limpa o dicionário de dados para o próximo produto
                    dados = {}

            # Inserção do último produto
            if 'id' in dados and 'asin' in dados and 'title' in dados and 'group' in dados and 'salesrank' in dados:
                cursor.execute("""
                INSERT INTO tabela_produto (id_produto, asin, titulo, grupo_produto, rank_vendas)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (asin) DO NOTHING
                """, (dados['id'], dados['asin'], dados['title'], dados['group'], dados['salesrank']))

                # Inserção de dados na tabela similar
                if 'similar' in dados:
                    similar = dados['similar'].split()
                    for asin_similar in similar[1:]:
                        cursor.execute("""
                        INSERT INTO tabela_similar (asin_produto, asin_similar)
                        VALUES (%s, %s)
                        ON CONFLICT (asin_produto, asin_similar) DO NOTHING
                        """, (dados['asin'], asin_similar))

                # Inserção de dados na tabela categoria
                if 'categories' in dados:
                    categorias = dados['categories'].split('|')[1:]
                    for categoria in categorias:
                        categoria_id = categoria.split(']')[0].split('[')[1]
                        cursor.execute("""
                        INSERT INTO tabela_categoria (asin_do_produto, categoria_id)
                        VALUES (%s, %s)
                        ON CONFLICT (asin_do_produto, categoria_id) DO NOTHING
                        """, (dados['asin'], categoria_id))

            # Commit das alterações
            conexao.commit()
            print("Inserção de dados concluída com sucesso!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conexao is not None:
            conexao.close()