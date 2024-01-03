import psycopg2
from config import config
import re
from psycopg2.extras import execute_values

def line_generator2(file, n_cat):
    for i in range(n_cat):
        line = file.readline()
        if not line:
            break
        yield line

def inserir_dados_arquivo(arquivo):
    try:
        parametros = config()
        conexao = psycopg2.connect(**parametros)
        cursor = conexao.cursor()

        with open(arquivo, 'r') as file:
            dados = {}
            in_category_section = False
            categories = []

            for linha in file:
                linha = linha.strip()

                if linha and ':' in linha:
                    chave, valor = re.match(r'^\s*([^:]+):\s*(.*)$', linha).groups()
                    dados[chave.lower()] = valor.strip()

                    if chave.lower() == 'categories':
                        in_category_section = True
                        categories.clear()  # Limpa a lista de categorias

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

                        if in_category_section:
                            for category in categories:
                                cursor.execute("""
                                INSERT INTO tabela_categoria (asin_do_produto, nome_categoria)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                                """, (dados['asin'], category[1]))

                            in_category_section = False  # Sai da seção de categorias
                            categories.clear()  # Limpa a lista de categorias

                    dados = {}  # Limpa o dicionário de dados para o próximo produto

                elif in_category_section:
                    result = re.findall(r"\|(.*?)\[(\d+)\]", linha)
                    if result:
                        categories.extend(result)

            # Commit das alterações
            conexao.commit()
            print("Inserção de dados concluída com sucesso!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conexao is not None:
            conexao.close()