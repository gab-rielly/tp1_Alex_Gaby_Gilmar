import sys
import time
from banco_de_dados import *
from dashboard import *


if __name__ == '__main__':
    start_time = time.time()
    dashboard = Dashboard()

    if "--dashboard" in sys.argv:
        asin = input("Digite o ASIN do produto para iniciar:")
        n= input("Insira um num para mostrar a evolução diária das médias de avaliação\nao longo do intervalo de tempo coberto no arquivo de entrada:")
        
        print("-" * 10 + "DASHBOARD" + "-" * 10)
        dashboard.verifica_produto(asin)
        dashboard.lista_avaliacao(asin)
        dashboard.lista_maior_similar(asin)
        dashboard.mostra_evolucao_diaria(asin, n)
        dashboard.lista_produtos_lideres()
        dashboard.lista_produtos_media_util()
        dashboard.lista_categoria_media_avaliacao()
        print("-" * 10 + "FINISHED" + "-" * 10)

