import sys
import time
from config import conectar
from banco_de_dados import *
#from insert_data import insert_data

if __name__ == '__main__':
    start_time = time.time()
    banco = BancoDeDados()

    if "--setup" in sys.argv:
        conectar()
        banco.conectar()
        banco.criar_bd()
        banco.criar_tabelas()

    if "--insert" in sys.argv:
        #insert_data()
        pass

    if "--drop" in sys.argv:
        banco.conectar()
        banco.drop_bd()

    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)

    print(f"All operations completed successfully in {minutes} minutes and {seconds} seconds.")
