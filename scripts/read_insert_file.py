import psycopg2
from config import config
import re
from psycopg2.extras import execute_values

def inserir_dados_arquivo(arquivo):
    try:
        parametros = config()
        conexao = psycopg2.connect(**parametros)
        cursor = conexao.cursor()

        with open(arquivo, 'r') as file:
            dados = {}
            in_category_section = False
            categories = []
            in_review_section = False
            reviews = []

            for linha in file:
                linha = linha.strip()

                if linha and ':' in linha:
                    chave, valor = re.match(r'^\s*([^:]+):\s*(.*)$', linha).groups()
                    dados[chave.lower()] = valor.strip()

                    if chave.lower() == 'categories':
                        in_category_section = True
                        categories.clear()  # Limpa a lista de categorias
                    elif chave.lower() == 'reviews':
                        in_review_section = True
                        reviews.clear()

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

                        # Inserção na 
                        if in_category_section:
                            for category in categories:
                                cursor.execute("""
                                INSERT INTO tabela_categoria (asin_produto, nome_categoria, id_categoria)
                                VALUES (%s, %s, %s)
                                ON CONFLICT DO NOTHING
                                """, (dados['asin'], category[0], category[1]))

                            in_category_section = False  # Sai da seção de categorias
                            categories.clear()  # Limpa a lista de categorias

                    # Inserção de dados na tabela review
                        if in_review_section: # Verifica se há informações de revisão
                            for review in reviews:
                                data, cliente, avaliacao, votes, util = review
                                id_review = int(data.replace('-', '')) # Gera um id_review a partir da data
                                cursor.execute("""
                                INSERT INTO tabela_review (id_review, id_produto, asin_produto, cliente, data, avaliacao, util)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id_review, id_produto, asin_produto) DO NOTHING
                                """, (id_review, dados.get('id'), dados.get('asin'), cliente, data, avaliacao, util))    

                            in_review_section = False  # Sai da seção de revisões
                            reviews.clear()  # Limpa a lista de revisões

                    dados = {}  # Limpa o dicionário de dados para o próximo produto


                elif in_category_section:
                    result = re.findall(r"\|(.*?)\[(\d+)\]", linha)
                    if result:
                        categories.extend(result)
                if in_review_section:
                    review_info = re.match(r'\s*(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(\w+)\s+rating:\s+(\d+)\s+\s+votes:\s+(\d+)\s+helpful:\s+(\d+)', linha)
                    if review_info:
                        reviews.append(review_info.groups()) # Adiciona a revisão à lista de revisões               


            # Se o dicionário ainda tiver dados após o término do loop, insira o último produto
            if dados:
                # Inserção dos dados do último produto
                if 'id' in dados and 'asin' in dados and 'title' in dados and 'group' in dados and 'salesrank' in dados:
                    cursor.execute("""
                    INSERT INTO tabela_produto (id_produto, asin, titulo, grupo_produto, rank_vendas)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (asin) DO NOTHING
                    """, (dados['id'], dados['asin'], dados['title'], dados['group'], dados['salesrank']))

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
                            INSERT INTO tabela_categoria (asin_produto, nome_categoria, id_categoria)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING
                            """, (dados['asin'], category[0], category[1]))

                        in_category_section = False  # Sai da seção de categorias
                        categories.clear()  # Limpa a lista de categorias
                    if in_review_section:
                        review_info = re.match(r'\s*(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(\w+)\s+rating:\s+(\d+)\s+votes:\s+(\d+)\s+helpful:\s+(\d+)', linha)
                        if review_info:
                            data, cliente, avaliacao, votes, util = review_info.groups()
                            id_review = int(data.replace('-', '')) # Gera um id_review a partir da data
                            cursor.execute("""
                            INSERT INTO tabela_review (id_review, id_produto, asin_produto, cliente, data, avaliacao, util)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id_review, id_produto, asin_produto) DO NOTHING
                            """, (id_review, dados.get('id'), dados.get('asin'), cliente, data, avaliacao, util))


            # Commit das alterações
            conexao.commit()
            print("Inserção de dados concluída com sucesso!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conexao is not None:
            conexao.close()