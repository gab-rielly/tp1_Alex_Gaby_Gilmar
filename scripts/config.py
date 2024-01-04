import psycopg2
from configparser import ConfigParser

def config(filename='bd.ini', section='postgresql'):
    
    parser = ConfigParser()
    # le a config de um arq
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Seção {0} não encontrada no arquivo {1}'.format(section, filename))

    return db


def conectar():
    """ Conectar ao servidor de banco de dados PostgreSQL """
    try:
        params = config()
        # conectar ao servidor de banco de dados PostgreSQL
        print('Conectando ao banco de dados PostgreSQL...')
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                print('Versão do banco de dados PostgreSQL:')
                cur.execute('SELECT version()')

                # exibe a versão do servidor do bd PostgreSQL
                versao_bd = cur.fetchone()
                print(versao_bd)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    else:
        print('Conexão com o banco de dados fechada automaticamente.')