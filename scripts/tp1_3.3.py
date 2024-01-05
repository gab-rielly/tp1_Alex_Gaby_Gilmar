import sys
import time
from banco_de_dados import *
from dashboard import *


if _name_ == '_main_':
    start_time = time.time()
    dashboard = Dashboard()

    if "--dashboard" in sys.argv:
        asin = input("Digite o ASIN do produto para iniciar: ")
        print("\n")
        print("-" * 25 + "DASHBOARD" + "-" * 25)
        dashboard.verifica_produto(asin)
        dashboard.lista_avaliacao(asin)
        dashboard.lista_maior_similar(asin)
        dashboard.mostra_evolucao_diaria(asin)
        dashboard.lista_produtos_lideres()
        dashboard.lista_produtos_media_util()
        dashboard.lista_categoria_media_avaliacao()
        dashboard.lista_comentarios_clientes()

        print("-" * 25 + "FINISHED" + "-" * 25)
